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

OnMemoryIndxer::ITokenizer::~ITokenizer() = default;

//-----------------------------------------------------------------------------

std::u32string OnMemoryIndxer::normalize(const std::u32string &str) const {
  return normalizer ? normalizer(str) : str;
}

size_t OnMemoryIndxer::Postings::size() const { return positions_map_.size(); }

size_t OnMemoryIndxer::Postings::document_id(size_t index) const {
  return find_positions_map(index)->first;
}

size_t OnMemoryIndxer::Postings::search_hit_count(size_t index) const {
  return find_positions_map(index)->second.size();
}

size_t OnMemoryIndxer::Postings::term_position(size_t index,
                                               size_t search_hit_index) const {
  return find_positions_map(index)->second[search_hit_index];
}

size_t OnMemoryIndxer::Postings::term_count(size_t index,
                                            size_t search_hit_index) const {
  return 1;
}

bool OnMemoryIndxer::Postings::has_term_pos(size_t index,
                                            size_t term_pos) const {
  const auto &positions = find_positions_map(index)->second;
  return std::binary_search(positions.begin(), positions.end(), term_pos);
}

void OnMemoryIndxer::Postings::add_term_position(size_t document_id,
                                                 size_t term_pos) {
  return positions_map_[document_id].push_back(term_pos);
}

OnMemoryIndxer::Postings::PositionsMap::const_iterator
OnMemoryIndxer::Postings::find_positions_map(size_t index) const {
  // TODO: performance improvement with caching the last access value
  auto it = positions_map_.begin();
  std::advance(it, index);
  return it;
}

//-----------------------------------------------------------------------------

size_t OnMemoryIndxer::document_count() const { return document_count_; }

bool OnMemoryIndxer::has_term(const std::u32string &str) const {
  return contains(term_dictionary_, str);
}

size_t OnMemoryIndxer::term_id(const std::u32string &str) const {
  return term_dictionary_.at(str);
}

size_t OnMemoryIndxer::tf(size_t term_id) const { return terms_[term_id].tf; }

size_t OnMemoryIndxer::df(size_t term_id) const {
  return postings_list_[term_id].size();
}

void OnMemoryIndxer::indexing(size_t document_id, ITokenizer &tokenizer) {
  tokenizer.tokenize([&](const auto &str, auto term_pos) {
    if (!contains(term_dictionary_, str)) {
      terms_.push_back({str, 0});

      auto term_id = term_dictionary_.size();
      term_dictionary_[str] = term_id;
      postings_list_.resize(term_id + 1);
    }

    auto term_id = term_dictionary_[str];
    assert(term_id < term_dictionary_.size());
    assert(term_id < postings_list_.size());

    terms_[term_id].tf++;

    postings_list_[term_id].add_term_position(document_id, term_pos);
  });

  document_count_++;
}

const IPostings &OnMemoryIndxer::postings(size_t term_id) const {
  return postings_list_[term_id];
}

}  // namespace searchlib

