//
//  termdict.h
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
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
// own LevenshteinAutomaton: state_[i] tracks whether the codepoints consumed
// so far can match pattern[0:i), the same subset-construction DP that
// InMemoryInvertedIndexBase's non-incremental matches_wildcard runs in one
// shot, just updated one codepoint at a time as custom_search descends the
// FST. Bytes are buffered until a full UTF-8 codepoint decodes (the FST
// visits one byte per arc) so a star never gets tested against half a
// multi-byte character.
class WildcardAutomaton {
public:
  explicit WildcardAutomaton(const std::u32string &pattern)
      : pattern_(std::make_shared<const std::u32string>(pattern)) {
    state_.resize(pattern_->size() + 1);
    state_[0] = true;
    for (size_t i = 1; i <= pattern_->size(); i++) {
      state_[i] = state_[i - 1] && (*pattern_)[i - 1] == U'*';
    }
  }

  // custom_search's FST traversal copies the automaton once per arc it
  // visits (fstlib's depth_first_visit, one copy per sibling byte-edge), so
  // pattern_ is a shared_ptr to keep that copy an atomic refcount bump
  // instead of a std::u32string deep-copy on every arc.
  WildcardAutomaton(const WildcardAutomaton &) = default;

  void step(char c) {
    u8bytes_ += c;
    char32_t cp;
    auto consumed =
        unicode::utf8::decode_codepoint(u8bytes_.data(), u8bytes_.size(), cp);
    if (consumed == 0) {
      return; // mid-codepoint; wait for more bytes
    }
    u8bytes_.clear();

    // Updated in place, left to right: by the time iteration i runs,
    // state_[0..i-1] already hold their new (post-step) values and
    // state_[i..] still hold their old (pre-step) ones, matching what the
    // DP needs at each position (state_[i-1] for the star case reads the
    // just-updated neighbor; prev_old for the literal case reads what that
    // neighbor held before this step). Avoids allocating a second
    // vector<bool> every codepoint.
    const auto &pattern = *pattern_;
    bool prev_old = state_[0];
    state_[0] = false;
    for (size_t i = 1; i <= pattern.size(); i++) {
      bool cur_old = state_[i];
      state_[i] = pattern[i - 1] == U'*' ? (state_[i - 1] || cur_old)
                                         : (prev_old && pattern[i - 1] == cp);
      prev_old = cur_old;
    }
  }

  bool is_match() const { return u8bytes_.empty() && state_.back(); }

  bool can_match() const {
    return std::find(state_.begin(), state_.end(), true) != state_.end();
  }

private:
  std::shared_ptr<const std::u32string> pattern_;
  std::vector<bool> state_;
  std::string u8bytes_; // bytes of a not-yet-fully-decoded codepoint
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
    enumerate_with_automaton(WildcardAutomaton(pattern), callback);
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
