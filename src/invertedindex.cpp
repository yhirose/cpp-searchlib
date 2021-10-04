//
//  invertedindex.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"
#include "utils.h"

namespace searchlib {

IPostings::~IPostings() = default;

IInvertedIndex::~IInvertedIndex() = default;

//-----------------------------------------------------------------------------

size_t InvertedIndex::Postings::size() const { return positions_map_.size(); }

size_t InvertedIndex::Postings::document_id(size_t index) const {
  return find_positions_map(index)->first;
}

size_t InvertedIndex::Postings::search_hit_count(size_t index) const {
  return find_positions_map(index)->second.size();
}

size_t InvertedIndex::Postings::term_position(size_t index,
                                              size_t search_hit_index) const {
  return find_positions_map(index)->second[search_hit_index];
}

size_t InvertedIndex::Postings::term_count(size_t index,
                                           size_t search_hit_index) const {
  return 1;
}

bool InvertedIndex::Postings::is_term_position(size_t index,
                                               size_t term_pos) const {
  const auto &positions = find_positions_map(index)->second;
  return std::binary_search(positions.begin(), positions.end(), term_pos);
}

void InvertedIndex::Postings::add_term_position(size_t document_id,
                                                size_t term_pos) {
  return positions_map_[document_id].push_back(term_pos);
}

InvertedIndex::Postings::PositionsMap::const_iterator
InvertedIndex::Postings::find_positions_map(size_t index) const {
  assert(index < positions_map_.size());
  // TODO: performance improvement with caching the last access value
  auto it = positions_map_.begin();
  std::advance(it, index);
  return it;
}

//-----------------------------------------------------------------------------

size_t InvertedIndex::document_count() const { return documents_.size(); }

bool InvertedIndex::term_exists(const std::u32string &str) const {
  return contains(term_dictionary_, str);
}

size_t InvertedIndex::term_occurrences(const std::u32string &str) const {
  return term_dictionary_.at(str).term_occurrences;
}

size_t InvertedIndex::df(const std::u32string &str) const {
  return postings(str).size();
}

double InvertedIndex::tf(const std::u32string &str, size_t document_id) const {
  const auto &p = postings(str);
  for (size_t i = 0; i < p.size(); i++) {
    if (p.document_id(i) == document_id) {
      return static_cast<double>(p.search_hit_count(i)) /
             static_cast<double>(documents_.at(document_id).term_count);
    }
  }
  return 0.0;
}

double InvertedIndex::idf(const std::u32string &str) const {
  auto adjustment = 0.001;
  return std::log2(static_cast<double>(documents_.size() + adjustment) /
                   (static_cast<double>(df(str) + adjustment)));
}

double InvertedIndex::tf_idf(const std::u32string &str,
                             size_t document_id) const {
  return tf(str, document_id) * idf(str);
}

const IPostings &InvertedIndex::postings(const std::u32string &str) const {
  return term_dictionary_.at(str).postings;
}

std::u32string InvertedIndex::normalize(const std::u32string &str) const {
  return normalizer_ ? normalizer_(str) : str;
}

}  // namespace searchlib

