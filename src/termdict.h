//
//  termdict.h
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

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
  std::string bytes_;
  std::unique_ptr<fst::map<uint32_t>> map_; // points into bytes_
};

} // namespace detail
} // namespace searchlib
