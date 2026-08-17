#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "segmentlib/bytes/binary_reader.h"
#include "segmentlib/kytea/char_table.h"
#include "segmentlib/support/attributes.h"
#include "segmentlib/support/span.h"

namespace segmentlib::kytea {

// Runtime representation of KyTea's Dictionary<Entry>: an Aho-Corasick
// automaton keyed by CharId, carrying a Payload per accepted string.
//
// The model file stores the automaton fully built — failure links resolved and
// outputs already propagated along suffix links — so matching never has to walk
// failure links to collect outputs; each state's `outputs` is the complete set.
//
// The goto graph is a canonical double-array: a state's id is the slot it
// occupies, so a single per-slot table `unit_` of {base, check} suffices. A
// transition from state `s` on `c` is `slot = unit_[s].base + c`, valid iff
// `unit_[slot].check == s`, and then the next state is `slot` itself — no
// separate target field is needed. Every transition is therefore O(1) — one
// addition and one contiguous cell read — with no per-state heap allocation, so
// the hot data stays cache-resident; the cell of the state just entered (read to
// validate its check) shares its cache line with the base the next step reads.
// This is a large win over a per-state, separately-heap-allocated,
// binary-searched goto list. Failure links and outputs are flattened (fail_ and
// an out_offset_/out_flat_ CSR) so matching never chases a pointer into a
// per-state vector.
//
// Payload is the per-entry data (e.g. a weight vector for the char/type n-gram
// dictionaries). Parsing a Payload is delegated to a caller-supplied reader so
// this class stays agnostic to entry layout.
template <class Payload>
class Automaton {
public:
    struct Match {
        std::size_t end_pos;      // index of the last character of the match
        const Payload* payload;
    };

    Automaton() = default;

    // Parses a Dictionary<Entry> from `r`. `read_payload(BinaryReader&)` reads
    // one entry. A dictionary written as "absent" (zero states) yields an empty
    // automaton, and — matching KyTea's on-disk form — no entry count follows,
    // so nothing further is consumed.
    //
    // Every state id, goto target and output index is checked against the counts
    // the file itself declares before it is used. build_canonical indexes
    // `new_id` and `payloads_` with these values directly, so an unchecked one
    // is an out-of-bounds write at an attacker-chosen offset, not a wrong
    // answer; the checks below are what makes a malformed model a rejected
    // model. One structural check cannot be made here — that every state is
    // reachable from the root — and lives in build_canonical, whose DFS
    // already establishes reachability as a side effect.
    template <class ReadPayload>
    static Automaton read(bytes::BinaryReader& r, ReadPayload read_payload) {
        Automaton a;
        a.num_dicts_ = r.read<std::uint8_t>();
        const auto n_states = r.read<std::uint32_t>();
        if (n_states == 0) {
            return a;  // absent dictionary: no entries follow
        }
        // Each state is at least failure + n_gotos + n_out + is_branch on disk.
        r.require_capacity(n_states, 4 + 4 + 4 + 1);
        std::vector<TempState> temp(n_states);
        // Bound on the output indices seen so far (largest + 1, 0 if none).
        // Outputs index payloads_, whose count is only known after every state
        // has been read, but carrying the bound along costs one compare per
        // output against a register, where a second pass would re-walk all
        // ~2M states' output vectors.
        std::uint64_t out_bound = 0;
        for (auto& s : temp) {
            s.failure = r.read<std::uint32_t>();
            if (s.failure >= n_states) {
                throw bytes::ParseError("automaton failure link out of range");
            }
            const auto n_gotos = r.read<std::uint32_t>();
            r.require_capacity(n_gotos, sizeof(CharId) + 4);  // CharId + target
            s.gotos.reserve(n_gotos);
            for (std::uint32_t i = 0; i < n_gotos; ++i) {
                const auto c = r.read<CharId>();
                const auto next = r.read<std::uint32_t>();
                if (next >= n_states) {
                    throw bytes::ParseError("automaton goto target out of range");
                }
                // build_canonical takes each state's first goto as its smallest
                // CharId (to derive the base) and the root's last as its
                // largest (to size root_next_), so strict ascent is a layout
                // requirement, not just a convention. It also rules out
                // duplicate keys, which would alias two states onto one slot.
                if (!s.gotos.empty() && c <= s.gotos.back().first) {
                    throw bytes::ParseError("automaton goto keys not strictly ascending");
                }
                s.gotos.emplace_back(c, next);
            }
            const auto n_out = r.read<std::uint32_t>();
            r.require_capacity(n_out, 4);  // one payload index each
            s.outputs.resize(n_out);
            for (auto& o : s.outputs) {
                o = r.read<std::uint32_t>();
                out_bound = std::max(out_bound, std::uint64_t{o} + 1);
            }
            s.is_branch = r.read_bool();  // marks a genuine word-end (see find_entry)
        }
        const auto n_entries = r.read<std::uint32_t>();
        if (out_bound > n_entries) {
            throw bytes::ParseError("automaton output index out of range");
        }
        // A payload's size is up to read_payload, but every form this parser
        // uses is length-prefixed, so one costs at least the 4-byte prefix.
        r.require_capacity(n_entries, 4);
        a.payloads_.reserve(n_entries);
        for (std::uint32_t i = 0; i < n_entries; ++i) {
            a.payloads_.push_back(read_payload(r));
        }
        a.build(temp);
        return a;
    }

    [[nodiscard]] bool empty() const noexcept { return num_states_ == 0; }
    [[nodiscard]] std::size_t num_states() const noexcept { return num_states_; }
    [[nodiscard]] std::size_t num_units() const noexcept { return unit_.size(); }
    [[nodiscard]] std::size_t num_payloads() const noexcept { return payloads_.size(); }
    [[nodiscard]] std::uint8_t num_dicts() const noexcept { return num_dicts_; }
    [[nodiscard]] const Payload& payload(std::size_t i) const { return payloads_[i]; }

    // Runs the automaton over `text`, invoking `on_match(end_pos, payload)` for
    // every accepted string, in the order KyTea's Dictionary::match emits them.
    template <class OnMatch>
    SEGMENTLIB_FLATTEN void match(Span<const CharId> text, OnMatch&& on_match) const {
        if (num_states_ == 0) {
            return;
        }
        std::uint32_t cur = 0;
        for (std::size_t i = 0; i < text.size(); ++i) {
            const CharId c = text[i];
            // The root is re-entered on most characters (any that don't extend a
            // match), so its transition is by far the hottest; take it from a
            // small, cache-resident direct index instead of the scattered cell.
            std::uint32_t next = (cur == 0) ? root_step(c) : step(cur, c);
            while (next == kNoState && cur != 0) {
                cur = fail_[cur];
                next = (cur == 0) ? root_step(c) : step(cur, c);
            }
            cur = (next == kNoState) ? 0 : next;
            for (std::uint32_t k = out_offset_[cur]; k < out_offset_[cur + 1]; ++k) {
                on_match(i, payload(out_flat_[k]));
            }
        }
    }

    // Exact whole-string lookup: returns the payload of the entry whose key is
    // exactly `str`, or nullptr if no such entry exists. Mirrors KyTea's
    // Dictionary::findEntry — it walks goto transitions
    // only (never failure links), then accepts only if the state reached is a
    // genuine word-end (is_branch) with its own output. The is_branch check is
    // essential: suffix-link output propagation can leave a non-word-end state
    // with a non-empty output (inherited from a proper suffix that *is* a
    // word), and that must not count as an exact match. The entry is
    // out_flat_'s first output for the state — the state's own entry, since
    // propagated outputs are appended after it (== KyTea's output[0]).
    [[nodiscard]] const Payload* find_entry(Span<const CharId> str) const {
        if (str.empty() || num_states_ == 0) {
            return nullptr;
        }
        std::uint32_t state = 0;
        std::size_t lev = 0;
        do {
            const CharId c = str[lev++];
            const std::uint32_t next = (state == 0) ? root_step(c) : step(state, c);
            state = (next == kNoState) ? 0 : next;
        } while (state != 0 && lev < str.size());
        if (out_offset_[state] == out_offset_[state + 1] || !is_branch_[state]) {
            return nullptr;
        }
        return &payloads_[out_flat_[out_offset_[state]]];
    }

    // Convenience wrapper that collects all matches into a vector.
    [[nodiscard]] std::vector<Match> match_all(Span<const CharId> text) const {
        std::vector<Match> result;
        match(text, [&](std::size_t pos, const Payload& p) {
            result.push_back(Match{pos, &p});
        });
        return result;
    }

private:
    // "no owner" marker for a cell's check / "no transition" result of step().
    static constexpr std::uint32_t kNoState = std::numeric_limits<std::uint32_t>::max();
    // "end of list" marker for the construction-time free list.
    static constexpr std::uint32_t kNull = std::numeric_limits<std::uint32_t>::max();
    // Marks a free slot abandoned during construction to bound the free list:
    // the fit test treats it as occupied (never reused) and, being distinct from
    // every real state id, it never matches in step().
    static constexpr std::uint32_t kClosed = std::numeric_limits<std::uint32_t>::max() - 1;

    // Upper bound on the construction free-list length. Placing a state's
    // children searches the free list for a non-colliding base; the search scans
    // the (sorted) list, so its length bounds per-state cost. Once the list
    // grows past this, the oldest (lowest-index) free slots are abandoned,
    // keeping the search window near the DFS write frontier.
    //
    // Because states are placed in DFS pre-order, the *useful* free slots are
    // always a short distance behind the frontier — the abandoned tail is space
    // this subtree has already moved past and would not reuse. So a small window
    // barely affects packing while cutting construction time dramatically: for
    // the ~2M-state word dictionary, 8192 builds ~6x faster than a window large
    // enough to reach full density, at ~25% extra slots (and the extra slots sit
    // in the deep, rarely-touched part of the table, which the root direct index
    // keeps off the hot path anyway). Small automata never fill the window, so
    // they still pack tightly.
    static constexpr std::size_t kFreeListCap = 8192;

    // Transitions decoded straight off disk, kept only during construction.
    struct TempState {
        std::uint32_t failure = 0;
        std::vector<std::pair<CharId, std::uint32_t>> gotos;  // sorted by CharId
        std::vector<std::uint32_t> outputs;                   // indices into payloads_
        bool is_branch = false;                               // genuine word-end state
    };

    // Follows the goto transition for `c` from `state`; kNoState if none. In the
    // canonical layout a state id *is* the slot it occupies, so the target slot
    // is itself the next state — no separate `next` field is needed.
    [[nodiscard]] std::uint32_t step(std::uint32_t state, CharId c) const noexcept {
        const std::int64_t slot = static_cast<std::int64_t>(unit_[state].base) + c;
        if (slot >= 0 && static_cast<std::size_t>(slot) < unit_.size() &&
            unit_[static_cast<std::size_t>(slot)].check == state) {
            return static_cast<std::uint32_t>(slot);
        }
        return kNoState;
    }

    // The root's transition (== step(0, c)) served from a direct index over the
    // root's child characters — a small contiguous table, unlike the root cell's
    // scattered slot in the full unit array.
    [[nodiscard]] std::uint32_t root_step(CharId c) const noexcept {
        return c < root_next_.size() ? root_next_[c] : kNoState;
    }

    void build(std::vector<TempState>& temp) {
        num_states_ = temp.size();
        if (num_states_ == 0) {
            return;
        }
        build_canonical(temp);
    }

    // Builds the canonical double-array: each state is renamed to the slot it
    // occupies (so a goto's target slot is directly its next-state id, and no
    // `next` field is needed), packing children densely with a capped,
    // darts-style free-list search. States are placed in DFS pre-order from the
    // root: this fixes a child's slot — its new id — before the child is itself
    // processed (parent before child), and, by keeping each subtree's nodes
    // contiguous, keeps the free-list frontier local so the base search stays
    // cheap (BFS order scatters siblings and multiplies collisions ~5x). Failure
    // links and outputs are then remapped into the new ids.
    void build_canonical(std::vector<TempState>& temp) {
        const std::size_t n = num_states_;
        std::vector<std::uint32_t> new_id(n, kNull);

        std::vector<std::uint32_t> free_next;
        std::vector<std::uint32_t> free_prev;
        std::uint32_t free_head = kNull;
        std::uint32_t free_tail = kNull;
        std::size_t free_len = 0;

        const auto grow = [&](std::size_t new_size) {
            const std::size_t old = unit_.size();
            if (new_size <= old) {
                return;
            }
            unit_.resize(new_size, Cell{0, kNoState});
            free_next.resize(new_size, kNull);
            free_prev.resize(new_size, kNull);
            for (std::size_t j = old; j < new_size; ++j) {
                free_prev[j] = free_tail;
                free_next[j] = kNull;
                if (free_tail != kNull) {
                    free_next[free_tail] = static_cast<std::uint32_t>(j);
                } else {
                    free_head = static_cast<std::uint32_t>(j);
                }
                free_tail = static_cast<std::uint32_t>(j);
            }
            free_len += new_size - old;
        };
        const auto unlink = [&](std::uint32_t j) {
            const std::uint32_t p = free_prev[j];
            const std::uint32_t nx = free_next[j];
            if (p != kNull) {
                free_next[p] = nx;
            } else {
                free_head = nx;
            }
            if (nx != kNull) {
                free_prev[nx] = p;
            } else {
                free_tail = p;
            }
            --free_len;
        };
        // Keeps the free list within kFreeListCap by abandoning its oldest
        // (lowest-index) slots, marked kClosed so they never re-enter the list
        // and never match a state in step().
        const auto enforce_cap = [&]() {
            while (free_len > kFreeListCap) {
                const std::uint32_t j = free_head;
                unlink(j);
                unit_[j].check = kClosed;
            }
        };

        // Reserve unit 0 for the root (its base is filled when it is processed).
        grow(1);
        unlink(0);
        unit_[0].check = kClosed;
        new_id[0] = 0;

        std::vector<std::uint32_t> stack;
        stack.reserve(n);
        stack.push_back(0);
        while (!stack.empty()) {
            const std::uint32_t old_s = stack.back();
            stack.pop_back();
            const std::uint32_t ns = new_id[old_s];
            const auto& gt = temp[old_s].gotos;
            if (gt.empty()) {
                continue;  // unit_[ns].base stays 0; step() can never match it
            }
            const CharId first = gt.front().first;

            // Find a base b such that every child slot b + c is free. The first
            // (smallest) child lands on a free slot f, giving b = f - first; scan
            // the free list for one whose derived base also clears the remaining
            // children. Because all children are >= first, no child can fall
            // below f, so none can land on an abandoned (lower-index) slot.
            std::int64_t base = -1;
            for (std::uint32_t f = free_head; f != kNull; f = free_next[f]) {
                if (f < first) {
                    continue;  // would make base negative for the first child
                }
                const std::int64_t cand = static_cast<std::int64_t>(f) - first;
                bool ok = true;
                for (std::size_t k = 1; k < gt.size(); ++k) {
                    const std::size_t slot =
                        static_cast<std::size_t>(cand + gt[k].first);
                    if (slot < unit_.size() && unit_[slot].check != kNoState) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    base = cand;
                    break;
                }
            }
            if (base < 0) {  // nothing fit: place the block entirely past the end
                base = unit_.size() > first
                           ? static_cast<std::int64_t>(unit_.size()) - first
                           : 0;
            }
            unit_[ns].base = static_cast<std::int32_t>(base);

            for (const auto& [c, child_old] : gt) {
                const std::size_t slot = static_cast<std::size_t>(base + c);
                if (slot >= unit_.size()) {
                    grow(slot + 1);
                }
                unlink(static_cast<std::uint32_t>(slot));
                unit_[slot].check = ns;  // base stays 0 until the child is processed
                new_id[child_old] = static_cast<std::uint32_t>(slot);
                stack.push_back(child_old);
            }
            enforce_cap();
        }

        // Every state must have been reached by the DFS above, or its new id is
        // still kNull and the remap below would index fail_/out_offset_ at
        // 2^32-1. A well-formed Aho-Corasick automaton is fully reachable from
        // the root by goto edges, so an unreachable state means a malformed
        // file, not a shape this build can represent.
        for (const auto id : new_id) {
            if (id == kNull) {
                throw bytes::ParseError("automaton has a state unreachable from the root");
            }
        }

        // Remap failure links and outputs into the canonical id space, indexed
        // by unit (so unused slots leave harmless zero-length output ranges).
        const std::size_t units = unit_.size();
        fail_.assign(units, 0);
        out_offset_.assign(units + 1, 0);
        is_branch_.assign(units, 0);
        for (std::size_t s = 0; s < n; ++s) {
            const std::uint32_t ns = new_id[s];
            fail_[ns] = new_id[temp[s].failure];
            out_offset_[ns + 1] = static_cast<std::uint32_t>(temp[s].outputs.size());
            is_branch_[ns] = temp[s].is_branch ? 1 : 0;
        }
        for (std::size_t i = 1; i <= units; ++i) {
            out_offset_[i] += out_offset_[i - 1];
        }
        out_flat_.resize(out_offset_[units]);
        for (std::size_t s = 0; s < n; ++s) {
            const std::uint32_t ns = new_id[s];
            std::copy(temp[s].outputs.begin(), temp[s].outputs.end(),
                      out_flat_.begin() + out_offset_[ns]);
        }

        // Direct index for the root's transitions (root is old/new id 0). Sized
        // to its largest child character; equals step(0, c) for every c.
        const auto& root_gotos = temp[0].gotos;  // sorted by CharId
        if (!root_gotos.empty()) {
            root_next_.assign(static_cast<std::size_t>(root_gotos.back().first) + 1,
                              kNoState);
            for (const auto& [c, child_old] : root_gotos) {
                root_next_[c] = new_id[child_old];
            }
        }
    }

    std::size_t num_states_ = 0;  // logical state count (units may exceed it)

    // Canonical double-array: a state's id is the slot it occupies, so one cell
    // per slot holds both its outgoing base and the id of the state owning it.
    // A transition reads a single cache line, and the cell of the state just
    // entered (its check) shares that line with the base the next step reads.
    struct Cell {
        std::int32_t base;    // outgoing: child slot = base + c
        std::uint32_t check;  // owning state id, or kNoState/kClosed
    };
    std::vector<Cell> unit_;  // indexed by canonical state id (= slot)

    // Direct index of the root's transitions by CharId (root is the hottest
    // state); root_next_[c] == step(0, c), kNoState past the last root child.
    std::vector<std::uint32_t> root_next_;

    // Flattened failure links and outputs (CSR), indexed by canonical state id.
    std::vector<std::uint32_t> fail_;
    std::vector<std::uint32_t> out_offset_;  // size units + 1
    std::vector<std::uint32_t> out_flat_;    // payload indices, grouped by state
    std::vector<char> is_branch_;            // per state: genuine word-end (find_entry)

    std::vector<Payload> payloads_;
    std::uint8_t num_dicts_ = 0;
};

}  // namespace segmentlib::kytea
