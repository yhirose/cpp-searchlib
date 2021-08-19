//
//  indexing.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <cassert>

#include "searchlib.h"
#include "utils.h"

namespace searchlib {

IPositionalList::~IPositionalList() = default;

IInvertedIndex::~IInvertedIndex() = default;

//-----------------------------------------------------------------------------

std::u32string InvertedIndex::normalize(const std::u32string &str) const {
  return normalizer ? normalizer(str) : str;
}

size_t InvertedIndex::PositionalList::size() const {
  return positional_list_.size();
}

size_t InvertedIndex::PositionalList::document_id(size_t index) const {
  return positional_list_iter(index)->first;
}

size_t InvertedIndex::PositionalList::search_hit_count(size_t index) const {
  return positional_list_iter(index)->second.size();
}

size_t InvertedIndex::PositionalList::term_position(
    size_t index, size_t search_hit_index) const {
  return positional_list_iter(index)->second[search_hit_index];
}

size_t InvertedIndex::PositionalList::term_count(
    size_t index, size_t search_hit_index) const {
  return 1;
}

bool InvertedIndex::PositionalList::has_term_pos(size_t index,
                                                 size_t term_pos) const {
  const auto &positions = positional_list_iter(index)->second;
  return std::binary_search(positions.begin(), positions.end(), term_pos);
}

void InvertedIndex::PositionalList::add_term_position(size_t document_id,
                                                      size_t term_pos) {
  return positional_list_[document_id].push_back(term_pos);
}

InvertedIndex::PositionalList::PositionsMap::const_iterator
InvertedIndex::PositionalList::positional_list_iter(size_t index) const {
  // TODO: performance improvement with caching the last access value
  auto it = positional_list_.begin();
  std::advance(it, index);
  return it;
}

//-----------------------------------------------------------------------------

size_t InvertedIndex::document_count() const { return document_count_; }

bool InvertedIndex::has_term(const std::u32string &str) const {
  return contains(term_dictionary_, str);
}

size_t InvertedIndex::term_id(const std::u32string &str) const {
  return term_dictionary_.at(str);
}

size_t InvertedIndex::tf(size_t term_id) const { return terms_[term_id].tf; }

size_t InvertedIndex::df(size_t term_id) const {
  return posting_list_[term_id].size();
}

void InvertedIndex::indexing(size_t document_id, ITokenizer &tokenizer) {
  tokenizer.tokenize([&](const auto &str, auto term_pos) {
    if (!contains(term_dictionary_, str)) {
      terms_.push_back({str, 0});

      auto term_id = term_dictionary_.size();
      term_dictionary_[str] = term_id;
      posting_list_.resize(term_id + 1);
    }

    auto term_id = term_dictionary_[str];
    assert(term_id < term_dictionary_.size());
    assert(term_id < posting_list_.size());

    terms_[term_id].tf++;

    auto &positional_list = posting_list_[term_id];
    positional_list.add_term_position(document_id, term_pos);
  });

  document_count_++;
}

const IPositionalList &InvertedIndex::positional_list(size_t term_id) const {
  return posting_list_[term_id];
}

}  // namespace searchlib

