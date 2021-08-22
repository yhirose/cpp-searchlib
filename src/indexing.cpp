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

IPostings::~IPostings() = default;

IInvertedIndex::~IInvertedIndex() = default;

OnMemoryIndex::ITokenizer::~ITokenizer() = default;

//-----------------------------------------------------------------------------

size_t OnMemoryIndex::Postings::size() const { return positions_map_.size(); }

size_t OnMemoryIndex::Postings::document_id(size_t index) const {
  return find_positions_map(index)->first;
}

size_t OnMemoryIndex::Postings::search_hit_count(size_t index) const {
  return find_positions_map(index)->second.size();
}

size_t OnMemoryIndex::Postings::term_position(size_t index,
                                              size_t search_hit_index) const {
  return find_positions_map(index)->second[search_hit_index];
}

size_t OnMemoryIndex::Postings::term_count(size_t index,
                                           size_t search_hit_index) const {
  return 1;
}

bool OnMemoryIndex::Postings::has_term_pos(size_t index,
                                           size_t term_pos) const {
  const auto &positions = find_positions_map(index)->second;
  return std::binary_search(positions.begin(), positions.end(), term_pos);
}

void OnMemoryIndex::Postings::add_term_position(size_t document_id,
                                                size_t term_pos) {
  return positions_map_[document_id].push_back(term_pos);
}

OnMemoryIndex::Postings::PositionsMap::const_iterator
OnMemoryIndex::Postings::find_positions_map(size_t index) const {
  assert(index < positions_map_.size());
  // TODO: performance improvement with caching the last access value
  auto it = positions_map_.begin();
  std::advance(it, index);
  return it;
}

//-----------------------------------------------------------------------------

size_t OnMemoryIndex::document_count() const { return documents_.size(); }

bool OnMemoryIndex::term_exists(const std::u32string &str) const {
  return contains(term_dictionary_, str);
}

size_t OnMemoryIndex::term_occurrences(const std::u32string &str) const {
  return term_dictionary_.at(str).term_occurrences;
}

size_t OnMemoryIndex::df(const std::u32string &str) const {
  return postings(str).size();
}

double OnMemoryIndex::tf(const std::u32string &str, size_t document_id) const {
  const auto &p = postings(str);
  for (size_t i = 0; i < p.size(); i++) {
    if (p.document_id(i) == document_id) {
      return static_cast<double>(p.search_hit_count(i)) /
             static_cast<double>(documents_.at(document_id).term_count);
    }
  }
  return 0.0;
}

double OnMemoryIndex::idf(const std::u32string &str) const {
  auto adjustment = 0.001;
  return std::log2(static_cast<double>(documents_.size() + adjustment) /
                   (static_cast<double>(df(str) + adjustment)));
}

double OnMemoryIndex::tf_idf(const std::u32string &str,
                             size_t document_id) const {
  return tf(str, document_id) * idf(str);
}

const IPostings &OnMemoryIndex::postings(const std::u32string &str) const {
  return term_dictionary_.at(str).postings;
}

void OnMemoryIndex::indexing(size_t document_id, ITokenizer &tokenizer) {
  size_t term_count = 0;
  tokenizer.tokenize([&](const auto &str, auto term_pos) {
    if (!contains(term_dictionary_, str)) {
      term_dictionary_[str] = {str, 0};
    }

    auto &term = term_dictionary_.at(str);
    term.term_occurrences++;
    term.postings.add_term_position(document_id, term_pos);

    term_count++;
  });

  documents_[document_id] = {term_count};
}

std::u32string OnMemoryIndex::normalize(const std::u32string &str) const {
  return normalizer ? normalizer(str) : str;
}

}  // namespace searchlib

