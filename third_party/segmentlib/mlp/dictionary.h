#pragma once

#if defined(_MSC_VER) && !defined(__clang__) && (defined(_M_X64) || defined(_M_ARM64))
#include <intrin.h>  // _BitScanForward64, for countr_zero_u64 below
#endif

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Path-qualified rather than a bare "fstlib.h": this header is public now that
// DictMatcher holds an fst::map directly, so the include lands in every
// consumer's translation unit. A consuming project with an fstlib.h of its own
// earlier on the include path would shadow the vendored copy with an untested
// version; requiring the cpp-fstlib/ prefix makes that collision take a
// directory name, not just a file name. The include path is
// third_party/ (see src/CMakeLists.txt), not third_party/cpp-fstlib/.
#include "cpp-fstlib/fstlib.h"

#include "segmentlib/bytes/binary_reader.h"
#include "segmentlib/bytes/binary_writer.h"
#include "segmentlib/mlp/vocab.h"
#include "segmentlib/support/span.h"
#include "segmentlib/unicode/normalize.h"
#include "segmentlib/unicode/utf8.h"

namespace segmentlib::mlp {

// Dictionary binary features (design.ja.md 4.4): per dictionary, position
// relation {L: match starts right after the boundary, I: match spans it,
// R: match ends right at it} × length bucket min(EGC-length, 4). The index
// within a dictionary is `position*4 + (bucket-1)`; the global index is
// `dict*12 + that`, matching the W_dict column layout (5.7 field 13). This is
// the canonical definition — the trainer's feature generation and the
// inference matcher below both use it.
inline constexpr std::uint32_t kDictFeaturesPerDict = 12;
enum class DictPosition : std::uint32_t { Left = 0, Inside = 1, Right = 2 };

// Per-boundary active dictionary features, CSR: boundary b owns
// indices[offsets[b] .. offsets[b+1]), sorted and unique (binary clamp, 5.4).
// Also carries the matcher's per-call scratch so features_into allocates
// nothing once the buffers have grown to a sentence's size.
struct DictFeatures {
    std::vector<std::uint32_t> offsets;  // size: boundary count + 1
    std::vector<std::uint32_t> indices;

    // scratch (managed by DictMatcher::features_into)
    //
    // The matcher runs over the sentence re-encoded as normalized UTF-8, so
    // `text` is that encoding, `egc_byte_starts` its per-cluster byte offsets
    // (byte offsets, unlike EncodedEgc::egc_starts, which indexes codepoints),
    // and `egc_at_byte` the reverse map used to reject matches that would end
    // in the middle of a cluster.
    std::string text;
    std::vector<std::uint32_t> egc_byte_starts;  // size: EGC count + 1
    std::vector<std::uint32_t> egc_at_byte;      // size: text.size() + 1
    // Per-boundary bitmap of active features, `mask_words` 64-bit words per
    // boundary. The feature space is num_dicts*12, so one word covers up to
    // five dictionaries; matches set bits and the CSR output is a bit scan,
    // which emits each boundary's features ascending and deduplicated.
    std::vector<std::uint64_t> masks;
    std::size_t mask_words = 0;
};

// A dictionary set in the form the matcher actually runs on: the FST byte
// code, plus the channel sets its outputs index into. An entry is stored once
// however many channels contain it, and its output is the id of its set, so
// `dicts[offsets[s] .. offsets[s+1])` are the channels of set s.
//
// The model file carries exactly this (Section 4.7 field 17), which is what
// keeps a large dictionary from being recompiled on every load.
struct CompiledDictionaries {
    std::string fst;
    std::vector<std::uint32_t> offsets;  // size: set count + 1, ascending
    std::vector<std::uint8_t> dicts;     // size: offsets.back()
    std::uint32_t num_dicts = 0;
};

namespace detail {

// Byte offset that starts no cluster; a match ending here spans only part of
// one and is not a dictionary hit.
inline constexpr std::uint32_t kNoEgc = 0xFFFFFFFFu;

// Index of the lowest set bit. std::countr_zero is C++20; `x` is never zero at
// the one call site (a bit-scan loop that clears as it goes).
inline int countr_zero_u64(std::uint64_t x) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_ctzll(x);
#elif defined(_MSC_VER) && (defined(_M_X64) || defined(_M_ARM64))
    // 64-bit only: MSVC does not declare _BitScanForward64 on 32-bit x86, which
    // falls through to the portable loop below.
    unsigned long i = 0;
    _BitScanForward64(&i, x);
    return static_cast<int>(i);
#else
    int n = 0;
    while ((x & 1u) == 0) {
        x >>= 1;
        ++n;
    }
    return n;
#endif
}

// Re-encodes an entry in the normalized form the scoring path sees, so the
// two sides compare byte for byte. Returns nullopt on malformed UTF-8.
inline std::optional<std::string> normalized_utf8(std::string_view word) {
    std::string out;
    std::size_t pos = 0;
    while (pos < word.size()) {
        const auto decoded = unicode::decode(word.substr(pos));
        if (!decoded) {
            return std::nullopt;
        }
        unicode::encode(unicode::normalize(decoded->codepoint), out);
        pos += decoded->length;
    }
    return out;
}

}  // namespace detail

// Compiles raw word lists, one per channel, into the above. Entries are
// normalized (方式(a)) before use, matching the treatment of the input side
// (5.7 field 17b); invalid-UTF-8 and empty entries are dropped, since they can
// never match.
[[nodiscard]] inline CompiledDictionaries compile_dictionaries(
    Span<const std::vector<std::string>> dictionaries) {
    CompiledDictionaries out;
    out.num_dicts = static_cast<std::uint32_t>(dictionaries.size());
    // A CSR with no sets still has one offset, so the "size is set count + 1"
    // invariant holds on the paths below that give up early. Serializing an
    // empty offsets vector wrote a set count of 0 - 1 instead.
    out.offsets.push_back(0);

    // (entry, channel) pairs, sorted into the byte order fst::compile wants.
    // A flat sort rather than an associative container: dictionaries run to
    // hundreds of thousands of entries and this is on the model-load path.
    std::vector<std::pair<std::string, std::uint8_t>> flat;
    for (std::size_t d = 0; d < dictionaries.size(); ++d) {
        for (const std::string& word : dictionaries[d]) {
            auto key = detail::normalized_utf8(word);
            if (!key || key->empty()) {
                continue;
            }
            flat.emplace_back(std::move(*key), static_cast<std::uint8_t>(d));
        }
    }
    if (flat.empty()) {
        return out;
    }
    std::sort(flat.begin(), flat.end());
    flat.erase(std::unique(flat.begin(), flat.end()), flat.end());

    // Intern the channel sets: an entry's output is a small set id rather than
    // the set itself, which keeps the output alphabet tiny however many
    // dictionaries (up to the format's 255) an entry belongs to.
    // The set is keyed as a byte string, not as a vector of channel ids:
    // ordering two vectors here reaches lexicographical_compare_three_way,
    // where GCC 14 cannot bound the memcmp length and rejects the build under
    // -Werror=stringop-overread. Strings compare through basic_string::compare
    // and are unaffected.
    std::map<std::string, std::uint32_t> set_ids;
    std::vector<std::uint32_t> offsets{0};
    std::vector<std::uint8_t> dicts;
    std::vector<std::pair<std::string, std::uint32_t>> input;
    std::string set_key;
    for (std::size_t i = 0; i < flat.size();) {
        // Consecutive rows sharing a key are that entry's channels, ascending.
        std::size_t j = i;
        set_key.clear();
        while (j < flat.size() && flat[j].first == flat[i].first) {
            set_key.push_back(static_cast<char>(flat[j].second));
            ++j;
        }
        const auto [it, inserted] =
            set_ids.try_emplace(set_key, static_cast<std::uint32_t>(offsets.size() - 1));
        if (inserted) {
            for (const char channel : set_key) {
                dicts.push_back(static_cast<std::uint8_t>(channel));
            }
            offsets.push_back(static_cast<std::uint32_t>(dicts.size()));
        }
        input.emplace_back(std::move(flat[i].first), it->second);
        i = j;
    }

    std::ostringstream os;
    const auto result = fst::compile<std::uint32_t>(input, os, /*sorted=*/true).first;
    assert(result == fst::Result::Success);
    if (result != fst::Result::Success) {
        return out;
    }
    out.fst = std::move(os).str();
    out.offsets = std::move(offsets);
    out.dicts = std::move(dicts);
    return out;
}

// Field 17 of the model file. One definition of the layout, used by the
// trainer that writes it, the loader that reads it and the tests that build
// fixtures: a writer duplicated per caller is a writer whose bugs the tests
// reproduce instead of catching.
//
// The reader validates what a file can get wrong (offsets ascending from zero,
// channel ids below num_dicts) and throws bytes::ParseError otherwise. What it
// cannot check without walking the automaton, namely the set ids the FST
// yields, DictMatcher bounds-checks as it matches.
inline void write_compiled_dictionaries(bytes::BinaryWriter& out,
                                        const CompiledDictionaries& compiled) {
    assert(!compiled.offsets.empty());
    out.write<std::uint32_t>(static_cast<std::uint32_t>(compiled.fst.size()));
    out.write_bytes(as_bytes(Span<const char>(compiled.fst)));
    out.write<std::uint32_t>(static_cast<std::uint32_t>(compiled.offsets.size() - 1));
    for (const std::uint32_t offset : compiled.offsets) {
        out.write<std::uint32_t>(offset);
    }
    for (const std::uint8_t dict : compiled.dicts) {
        out.write<std::uint8_t>(dict);
    }
}

[[nodiscard]] inline CompiledDictionaries read_compiled_dictionaries(
    bytes::BinaryReader& in, std::uint32_t num_dicts) {
    CompiledDictionaries out;
    out.num_dicts = num_dicts;
    out.fst = in.read_blob(in.read<std::uint32_t>());

    const std::uint32_t sets = in.read<std::uint32_t>();
    in.require_capacity(std::uint64_t{sets} + 1, sizeof(std::uint32_t));
    out.offsets.reserve(sets + 1);
    for (std::uint32_t s = 0; s <= sets; ++s) {
        out.offsets.push_back(in.read<std::uint32_t>());
    }
    // The offsets index `dicts`, and the channels index W_dict's columns, so
    // both are checked before anything can use them.
    if (out.offsets.front() != 0 ||
        !std::is_sorted(out.offsets.begin(), out.offsets.end())) {
        throw bytes::ParseError("dictionary channel-set offsets are not ascending");
    }

    const std::uint32_t ids = out.offsets.back();
    in.require_capacity(ids, 1);
    out.dicts.reserve(ids);
    for (std::uint32_t i = 0; i < ids; ++i) {
        out.dicts.push_back(in.read<std::uint8_t>());
    }
    if (std::any_of(out.dicts.begin(), out.dicts.end(),
                    [&](std::uint8_t d) { return d >= num_dicts; })) {
        throw bytes::ParseError("dictionary channel id out of range");
    }
    return out;
}

// The dictionary feature extractor: entries are normalized and compiled into
// one FST (cpp-fstlib) keyed by their normalized UTF-8 bytes, which is queried
// with a common-prefix search from every cluster start.
//
// Matching is byte-level but the features are defined over EGCs, so a match is
// kept only when its end lands on a cluster boundary. That check is what keeps
// a dictionary entry from matching a proper prefix of a longer cluster (an
// entry "か" must not match inside the cluster "か" + U+3099). Keying on the
// normalized text rather than on embedding rows also keeps distinct codepoints
// that alias under UNK from matching each other.
class DictMatcher {
public:
    // No dictionaries: features_into emits empty ranges everywhere.
    DictMatcher() noexcept = default;

    // Builds from raw word lists, one per dictionary channel, compiling them.
    // This is the trainer's path; the loader takes the compiled form below.
    explicit DictMatcher(Span<const std::vector<std::string>> dictionaries)
        : DictMatcher(compile_dictionaries(dictionaries)) {}

    // Adopts an already-compiled dictionary set. The byte code arrives from a
    // file here, so valid() reports whether it parsed.
    explicit DictMatcher(CompiledDictionaries compiled)
        : num_dicts_(compiled.num_dicts) {
        if (!compiled.fst.empty()) {
            impl_ = std::make_unique<Impl>(std::move(compiled.fst),
                                           std::move(compiled.offsets),
                                           std::move(compiled.dicts));
        }
    }

    ~DictMatcher() = default;
    DictMatcher(DictMatcher&&) noexcept = default;
    DictMatcher& operator=(DictMatcher&&) noexcept = default;

    DictMatcher(const DictMatcher& other)
        : impl_(other.impl_ ? std::make_unique<Impl>(*other.impl_) : nullptr),
          num_dicts_(other.num_dicts_) {}

    DictMatcher& operator=(const DictMatcher& other) {
        // Copy-and-move, so how a matcher is duplicated stays in one place above.
        DictMatcher copy(other);
        *this = std::move(copy);
        return *this;
    }

    // False only when the byte code handed to the CompiledDictionaries
    // constructor is not a usable FST, which a corrupt model file can produce
    // and a locally compiled dictionary cannot.
    [[nodiscard]] bool valid() const noexcept {
        return !impl_ || static_cast<bool>(impl_->matcher);
    }

    [[nodiscard]] std::uint32_t num_dicts() const noexcept { return num_dicts_; }
    [[nodiscard]] std::uint32_t feature_count() const noexcept {
        return num_dicts_ * kDictFeaturesPerDict;
    }

    // Computes the active features of every boundary of `enc` into `out`.
    void features_into(const EncodedEgc& enc, DictFeatures& out) const {
        const std::size_t m = enc.egc_count();
        const std::size_t boundaries = m > 0 ? m - 1 : 0;

        out.offsets.clear();
        out.indices.clear();
        out.offsets.push_back(0);
        if (boundaries == 0 || num_dicts_ == 0 || !impl_) {
            out.offsets.assign(boundaries + 1, 0);
            return;
        }

        out.mask_words = (feature_count() + 63) / 64;
        out.masks.assign(boundaries * out.mask_words, 0);

        // Re-encode the sentence as normalized UTF-8, recording where each cluster
        // starts and, in the reverse direction, which byte offsets are boundaries.
        out.text.clear();
        out.egc_byte_starts.clear();
        for (std::size_t i = 0; i < m; ++i) {
            out.egc_byte_starts.push_back(static_cast<std::uint32_t>(out.text.size()));
            for (const char32_t cp : enc.egc_cps(i)) {
                unicode::encode(cp, out.text);
            }
        }
        out.egc_byte_starts.push_back(static_cast<std::uint32_t>(out.text.size()));
        out.egc_at_byte.assign(out.text.size() + 1, detail::kNoEgc);
        for (std::size_t i = 0; i <= m; ++i) {
            out.egc_at_byte[out.egc_byte_starts[i]] = static_cast<std::uint32_t>(i);
        }

        const auto mark = [&](std::size_t b, std::uint32_t f) {
            out.masks[b * out.mask_words + f / 64] |= std::uint64_t{1} << (f % 64);
        };

        // A match over EGCs [s, e] contributes L at boundary s-1, I at
        // boundaries s..e-1, R at boundary e (5.4).
        for (std::size_t s = 0; s < m; ++s) {
            const std::uint32_t start_byte = out.egc_byte_starts[s];
            impl_->matcher.common_prefix_search(
                std::string_view(out.text).substr(start_byte),
                [&](std::size_t len, const std::uint32_t& set_id) {
                    const std::uint32_t past = out.egc_at_byte[start_byte + len];
                    if (past == detail::kNoEgc) {
                        return;  // ends inside a cluster, so not a match here
                    }
                    // The set ids come out of the FST, which a model file
                    // supplies verbatim; the parser cannot check them without
                    // walking it, so the indexing below is guarded here instead.
                    // Widened first: set_id + 1 in uint32 wraps to 0 at the top of
                    // the range, which is exactly the value a corrupt model would
                    // need to slip past.
                    if (std::size_t{set_id} + 1 >= impl_->offsets.size()) {
                        return;
                    }
                    const std::size_t end = past - 1;
                    const auto bucket =
                        static_cast<std::uint32_t>(std::min<std::size_t>(past - s, 4) - 1);
                    for (std::uint32_t k = impl_->offsets[set_id];
                         k < impl_->offsets[set_id + 1]; ++k) {
                        const std::uint32_t base = impl_->dicts[k] * kDictFeaturesPerDict;
                        const auto feat = [&](DictPosition p) {
                            return base + static_cast<std::uint32_t>(p) * 4 + bucket;
                        };
                        if (s >= 1) {
                            mark(s - 1, feat(DictPosition::Left));
                        }
                        const std::uint32_t inside = feat(DictPosition::Inside);
                        for (std::size_t b = s; b < end; ++b) {
                            mark(b, inside);
                        }
                        if (end < boundaries) {
                            mark(end, feat(DictPosition::Right));
                        }
                    }
                });
        }

        for (std::size_t b = 0; b < boundaries; ++b) {
            const std::uint64_t* row = out.masks.data() + b * out.mask_words;
            for (std::size_t w = 0; w < out.mask_words; ++w) {
                for (std::uint64_t bits = row[w]; bits != 0; bits &= bits - 1) {
                    out.indices.push_back(static_cast<std::uint32_t>(
                        w * 64 + static_cast<std::size_t>(detail::countr_zero_u64(bits))));
                }
            }
            out.offsets.push_back(static_cast<std::uint32_t>(out.indices.size()));
        }
    }

private:
    // The compiled automaton. fst::map views the byte code rather than owning it,
    // so the two live together and a copy recompiles the view over its own bytes.
    // Held through unique_ptr so that address stays fixed: moving an Impl by
    // value would leave `matcher` pointing at the moved-from `bytes`.
    struct Impl {
        std::string bytes;
        fst::map<std::uint32_t> matcher;
        // Distinct channel sets in CSR form: an entry's FST output is the id of
        // its set, and set id s owns dicts[offsets[s] .. offsets[s+1]).
        std::vector<std::uint32_t> offsets;
        std::vector<std::uint8_t> dicts;

        Impl(std::string b, std::vector<std::uint32_t> o, std::vector<std::uint8_t> d)
            : bytes(std::move(b)),
              matcher(bytes.data(), bytes.size()),
              offsets(std::move(o)),
              dicts(std::move(d)) {}

        Impl(const Impl& other) : Impl(other.bytes, other.offsets, other.dicts) {}
        Impl& operator=(const Impl&) = delete;
    };

    std::unique_ptr<Impl> impl_;  // null when there is nothing to match
    std::uint32_t num_dicts_ = 0;
};

}  // namespace segmentlib::mlp
