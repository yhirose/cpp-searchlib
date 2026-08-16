//
//  termdict.h
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "lib/fstlib.h"
#include "lib/unicodelib_encodings.h"
#include "searchlib.h"
#include "utils.h"

namespace searchlib {
namespace detail {

// The Compressed format stores the term dictionary as an FST over the UTF-8
// encoded term strings instead of writing each term out as a length-prefixed
// uint32-per-character string.
//
// fst::compile with need_output assigns each key a sequential uint32 in key
// order, and the terms are written in ascending term-string order already (a
// format invariant, see InMemoryInvertedIndexBase::save). UTF-8 byte order
// matches codepoint order, so the FST's ordinal for a term is exactly its
// position in that sequence: whatever per-term records follow the FST can be
// indexed directly by the ordinal, with no separate mapping.
//
// Everything about the section lives here because three places have to agree
// on it: the writer and reader in invertedindex.cpp, and the independent
// reader in compressedindex.cpp.
//
// Layout:
//   uint64  fst_byte_size
//   char[fst_byte_size]  fst byte code (empty when there are no terms)

// Builds and writes the FST section. `keys` must be UTF-8, in ascending byte
// order, and unique; each key's ordinal is its index in `keys`.
inline void write_term_dictionary_fst(std::ostream &os,
                                      const std::vector<std::string> &keys) {
  if (keys.empty()) {
    write_scalar<uint64_t>(os, 0);
    return;
  }

  std::ostringstream fst_os;
  auto [result, error_index] =
      fst::compile(keys, fst_os, /*need_output=*/true, /*sorted=*/true);
  if (result != fst::Result::Success) {
    // EmptyKey is reachable from ordinary user code, since a Normalizer may
    // map a token to the empty string; the other two would mean save() broke
    // its own sorted-and-unique invariant.
    const char *reason = result == fst::Result::EmptyKey ? "an empty term"
                         : result == fst::Result::DuplicateKey
                             ? "a duplicate term"
                             : "unsorted terms";
    throw std::runtime_error(
        "searchlib: cannot build the term dictionary FST (" +
        std::string(reason) + " at index " + std::to_string(error_index) +
        ")");
  }

  write_bytes(os, fst_os.str());
}

// Drives a glob match ('*' = zero or more codepoints, every other codepoint
// literal) through an FST via fst::map::custom_search. Modeled on fstlib's
// own LevenshteinAutomaton: bit i of the state tracks whether the codepoints
// consumed so far can match pattern[0:i), the same subset-construction DP
// that InMemoryInvertedIndexBase's non-incremental matches_wildcard runs in
// one shot, just updated one codepoint at a time as custom_search descends
// the FST. Bytes are buffered until a full UTF-8 codepoint decodes (the FST
// visits one byte per arc) so a star never gets tested against half a
// multi-byte character.
//
// The whole DP row is a bitmask, so one codepoint costs a handful of word
// operations instead of a loop over the pattern, and -- what matters far more
// -- a pattern short enough to fit in one word carries its state inline,
// which is what keeps the per-arc copy below from allocating.
class WildcardAutomaton {
public:
  // The pattern, compiled once per query into the masks step() needs. Every
  // copy the traversal makes points at the same one, hence immutable.
  struct Program {
    size_t size = 0;       // pattern length in codepoints, after collapsing
    size_t word_count = 1; // words needed for size + 1 bits
    std::vector<uint64_t> star;
    // Which positions carry which literal. ASCII gets a directly indexed
    // table because that is the common case by far; anything above it falls
    // back to a binary search over the distinct codepoints the pattern
    // actually mentions, which is at most one per position.
    std::vector<uint64_t> ascii; // 128 rows of word_count words
    std::vector<char32_t> wide;  // sorted, >= U+0080 only
    std::vector<uint64_t> wide_masks;
    std::vector<uint64_t> no_match; // word_count zeros

    explicit Program(const std::u32string &raw) {
      // "**" means what "*" means, and collapsing the runs is what lets
      // close_over_stars() below be a single shift-or: with no two star bits
      // adjacent, a bit the closure sets can never feed another star.
      std::u32string pattern;
      for (auto cp : raw) {
        if (cp == U'*' && !pattern.empty() && pattern.back() == U'*') {
          continue;
        }
        pattern += cp;
      }

      size = pattern.size();
      word_count = (size + 1 + 63) / 64;
      star.assign(word_count, 0);
      no_match.assign(word_count, 0);
      ascii.assign(128 * word_count, 0);

      for (auto cp : pattern) {
        if (cp >= 0x80) { wide.push_back(cp); }
      }
      std::sort(wide.begin(), wide.end());
      wide.erase(std::unique(wide.begin(), wide.end()), wide.end());
      wide_masks.assign(wide.size() * word_count, 0);

      // Position i stands for pattern[i - 1], so the masks start at bit 1.
      // Bit 0 is position 0, the empty prefix, which no pattern codepoint
      // owns and so no mask touches.
      for (size_t i = 1; i <= size; i++) {
        auto cp = pattern[i - 1];
        auto bit = uint64_t(1) << (i % 64);
        if (cp == U'*') {
          star[i / 64] |= bit;
        } else if (cp < 0x80) {
          ascii[size_t(cp) * word_count + i / 64] |= bit;
        } else {
          auto it = std::lower_bound(wide.begin(), wide.end(), cp);
          wide_masks[size_t(it - wide.begin()) * word_count + i / 64] |= bit;
        }
      }
    }

    const uint64_t *literal_mask(char32_t cp) const {
      if (cp < 0x80) { return ascii.data() + size_t(cp) * word_count; }
      auto it = std::lower_bound(wide.begin(), wide.end(), cp);
      if (it == wide.end() || *it != cp) { return no_match.data(); }
      return wide_masks.data() + size_t(it - wide.begin()) * word_count;
    }
  };

  explicit WildcardAutomaton(const Program &program) : program_(&program) {
    state_.init(program_->word_count);
    auto *row = state_.data();
    row[0] = 1; // the empty prefix matches pattern[0:0)
    close_over_stars(row);
  }

  // The Program has to outlive every copy of the automaton, so binding one
  // to a temporary is a mistake worth refusing outright.
  explicit WildcardAutomaton(Program &&) = delete;

  // custom_search's FST traversal copies the automaton once per arc it
  // visits (fstlib's depth_first_visit, one copy per sibling byte-edge), so
  // program_ is a bare pointer to a Program the caller keeps alive for the
  // whole traversal, so that copy stays a word copy: a shared_ptr would put
  // an atomic increment and an acquire-release decrement on every arc, and
  // the shared ownership would never be used -- every copy the traversal
  // makes is destroyed inside the scope that built the Program.
  WildcardAutomaton(const WildcardAutomaton &) = default;

  void step(char c) {
    // The buffer stops at 4 bytes because decode_codepoint only ever inspects
    // the lead byte's shape: once 4 bytes have failed to decode, no further
    // byte can make it succeed, and the automaton is permanently
    // non-matching either way. Refusing the extra bytes keeps this state
    // bounded instead of accumulating the rest of a malformed key.
    if (u8len_ < sizeof(u8bytes_)) { u8bytes_[u8len_++] = c; }
    char32_t cp;
    if (unicode::utf8::decode_codepoint(u8bytes_, u8len_, cp) == 0) {
      return; // mid-codepoint; wait for more bytes
    }
    u8len_ = 0;

    // One step of the DP, on every position at once:
    //   literal position i: new[i] = old[i - 1] && pattern[i - 1] == cp
    //                             -> (old << 1) & literal_mask(cp)
    //   star position i:    new[i] = old[i] || new[i - 1]
    //                             -> (old & star) then close_over_stars
    // Bit 0 falls out as zero on its own: nothing shifts into it, and no star
    // bit sits there.
    const auto *literal = program_->literal_mask(cp);
    const auto *star = program_->star.data();
    auto *row = state_.data();

    if (program_->word_count == 1) {
      auto old = row[0];
      auto next = ((old << 1) & literal[0]) | (old & star[0]);
      next |= (next << 1) & star[0];
      row[0] = next;
      live_ = next;
      return;
    }

    uint64_t carry = 0;
    for (size_t w = 0; w < program_->word_count; w++) {
      auto old = row[w];
      auto shifted = (old << 1) | carry;
      carry = old >> 63;
      row[w] = (shifted & literal[w]) | (old & star[w]);
    }
    close_over_stars(row);
  }

  bool is_match() const {
    if (u8len_ > 0) { return false; }
    return (state_.data()[program_->size / 64] >> (program_->size % 64)) & 1;
  }

  // A whole-row test that costs nothing to keep: custom_search calls this
  // once per arc, just like step(), and step() has the row in registers
  // anyway.
  bool can_match() const { return live_ != 0; }

private:
  // The DP row. depth_first_visit copies the automaton once per arc, so a
  // std::vector here would mean a malloc/free pair per arc; a pattern of up
  // to 63 codepoints -- every pattern anyone types -- keeps its row inline
  // instead. Longer patterns spill to the heap so the class stays general.
  //
  // data() is recomputed from word_count_ rather than cached in a member
  // pointer, so there is nothing to fix up after a copy and the copy
  // constructor can stay defaulted.
  //
  // There are deliberately no move operations. A defaulted move would take
  // heap_ out of a spilled row while leaving word_count_ still claiming it,
  // so data() would return nullptr; WildcardAutomaton's user-declared copy
  // constructor suppresses its own implicit moves, which is what keeps that
  // unreachable today.
  class Row {
  public:
    // Sizes the row once, at construction; cells are not carried across the
    // inline/heap boundary, so this is not a general resize.
    void init(size_t word_count) {
      word_count_ = word_count;
      if (word_count > 1) { heap_.assign(word_count, 0); }
    }

    uint64_t *data() { return word_count_ <= 1 ? &inline_ : heap_.data(); }
    const uint64_t *data() const {
      return word_count_ <= 1 ? &inline_ : heap_.data();
    }

  private:
    size_t word_count_ = 1;
    uint64_t inline_ = 0;
    std::vector<uint64_t> heap_; // empty unless word_count_ > 1
  };

  // A star matches the empty string, so a live position immediately below a
  // star makes that star live too. Collapsed runs mean one shift-or reaches
  // the fixed point; the carry hands bit 63 of a word to bit 0 of the next.
  //
  // The OR of the row is accumulated here rather than rescanned in
  // can_match(), which the traversal calls once per arc just like step().
  // The constructor's initial live_ comes from this same pass.
  void close_over_stars(uint64_t *row) {
    const auto *star = program_->star.data();
    uint64_t carry = 0;
    uint64_t live = 0;
    for (size_t w = 0; w < program_->word_count; w++) {
      auto next = row[w] | (((row[w] << 1) | carry) & star[w]);
      carry = next >> 63;
      row[w] = next;
      live |= next;
    }
    live_ = live;
  }

  const Program *program_;
  Row state_;
  uint64_t live_ = 0;   // the OR of state_, so can_match() need not rescan
  char u8bytes_[4]{};   // bytes of a not-yet-fully-decoded codepoint
  uint8_t u8len_ = 0;
};

// The FST section as read back from a stream. It owns both the byte code and
// the fst::map built over it; fst::map keeps a bare pointer into the bytes
// rather than copying them, so the two must never be separated. Hence this
// type is neither copyable nor movable, which also propagates to whatever
// holds it as a member.
class TermDictionaryFst {
public:
  TermDictionaryFst() = default;
  TermDictionaryFst(const TermDictionaryFst &) = delete;
  TermDictionaryFst &operator=(const TermDictionaryFst &) = delete;
  TermDictionaryFst(TermDictionaryFst &&) = delete;
  TermDictionaryFst &operator=(TermDictionaryFst &&) = delete;

  void load(std::istream &is) {
    bytes_ = read_bytes(is);
    if (bytes_.empty()) {
      return; // no terms, so no FST was written
    }
    map_ = std::make_unique<fst::map<uint32_t>>(bytes_.data(), bytes_.size());
    if (!*map_) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }
  }

  // Exact lookup. Returns false for an absent term, or when the dictionary
  // is empty.
  bool find(const std::u32string &str, uint32_t &ordinal) const {
    return map_ && map_->exact_match_search(u8(str), ordinal);
  }

  void enumerate_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback) const {
    if (!map_) {
      return;
    }
    // One buffer reused across hits: a u32string longer than the small-string
    // buffer (4 codepoints on libc++) would otherwise cost a malloc/free pair
    // per term. Callbacks receive it by reference and must not retain it,
    // which is what IInvertedIndex::enumerate_terms_with_prefix promises.
    std::u32string term;
    map_->predictive_search(u8(prefix),
                            [&](const std::string &key, const uint32_t &) {
                              term.clear();
                              unicode::utf8::decode(key, term);
                              callback(term);
                            });
  }

  void enumerate_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback) const {
    // The Program outlives the traversal, which is what lets the automaton
    // (and every copy custom_search makes of it) hold a bare pointer to it.
    WildcardAutomaton::Program program(pattern);
    enumerate_with_automaton(WildcardAutomaton(program), callback);
  }

  void enumerate_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback) const {
    // fst::map::edit_distance_search would do the same walk, but it collects
    // every hit into a vector first, and it rejects an empty needle outright;
    // going through custom_search keeps this in the same streaming,
    // one-reused-buffer shape as the enumerators above, and lets an empty
    // `target` mean what the interface says it means (every term of length <=
    // max_edits), which is what the in-memory backend answers too. The
    // automaton is fstlib's own, so nothing is reimplemented here.
    enumerate_with_automaton(
        fst::LevenshteinAutomaton(u8(target), max_edits, /*insert_cost=*/1,
                                  /*delete_cost=*/1, /*replace_cost=*/1),
        callback);
  }

  // Recovers the ordinal -> term mapping. Used by readers that need the term
  // strings themselves (the in-memory index rebuilds its hash map from them);
  // readers that only look terms up keep this object instead and never
  // materialize the vocabulary.
  std::vector<std::u32string> terms(size_t term_count) const {
    std::vector<std::u32string> result(term_count);
    if (term_count == 0) {
      return result;
    }
    if (!map_) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }

    // enumerate's order is unspecified, hence indexing by the ordinal rather
    // than appending. Every ordinal has to be claimed exactly once: a
    // duplicate would overwrite one term and leave another slot empty, which
    // would silently attach a record to the wrong term.
    std::vector<bool> claimed(term_count, false);
    size_t seen = 0;
    map_->enumerate([&](const std::string &key, const uint32_t &ordinal) {
      if (ordinal >= term_count || claimed[ordinal]) {
        throw std::runtime_error("searchlib: corrupt term dictionary FST");
      }
      claimed[ordinal] = true;
      result[ordinal] = u32(key);
      seen++;
    });
    if (seen != term_count) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }

    return result;
  }

private:
  // The shape both automaton-driven enumerators share: one buffer refilled per
  // hit rather than a fresh u32string each time, since a term longer than the
  // small-string buffer (4 codepoints on libc++) would otherwise cost a
  // malloc/free pair. Callbacks receive it by reference and must not retain
  // it, which is what the IInvertedIndex::enumerate_terms_with_* contract
  // promises. (enumerate_with_prefix stays separate: it descends to a subtree
  // with predictive_search instead of walking with an automaton.)
  template <typename Automaton>
  void enumerate_with_automaton(
      const Automaton &automaton,
      const std::function<void(const std::u32string &str)> &callback) const {
    if (!map_) {
      return;
    }
    std::u32string term;
    map_->custom_search(automaton,
                        [&](const std::string &key, const uint32_t &) {
                          term.clear();
                          unicode::utf8::decode(key, term);
                          callback(term);
                        });
  }

  std::string bytes_;
  std::unique_ptr<fst::map<uint32_t>> map_; // points into bytes_
};

} // namespace detail
} // namespace searchlib
