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

IMutableInvertedIndex::~IMutableInvertedIndex() = default;

//-----------------------------------------------------------------------------

size_t InMemoryInvertedIndexBase::Postings::size() const {
  return positions_.size();
}

size_t InMemoryInvertedIndexBase::Postings::document_id(size_t index) const {
  return positions_[index].first;
}

size_t
InMemoryInvertedIndexBase::Postings::search_hit_count(size_t index) const {
  return positions_[index].second.size();
}

size_t InMemoryInvertedIndexBase::Postings::term_position(
    size_t index, size_t search_hit_index) const {
  return positions_[index].second[search_hit_index];
}

size_t InMemoryInvertedIndexBase::Postings::term_length(
    size_t index, size_t search_hit_index) const {
  return 1;
}

bool InMemoryInvertedIndexBase::Postings::is_term_position(
    size_t index, size_t term_pos) const {
  const auto &positions = positions_[index].second;
  return std::binary_search(positions.begin(), positions.end(), term_pos);
}

void InMemoryInvertedIndexBase::Postings::add_term_position(size_t document_id,
                                                            size_t term_pos) {
  auto it = std::lower_bound(
      positions_.begin(), positions_.end(), document_id,
      [](const auto &entry, size_t doc_id) { return entry.first < doc_id; });
  if (it != positions_.end() && it->first == document_id) {
    it->second.push_back(term_pos);
  } else {
    positions_.insert(it, Entry{document_id, {term_pos}});
  }
}

//-----------------------------------------------------------------------------

// Assumes document_id(index) is monotonically increasing in index, which
// holds for every IPostings this library produces: Postings keeps entries
// sorted by document_id, and SearchResult (search.cpp) only ever appends
// documents in ascending order via its cursor-merge algorithms.
size_t find_postings_index_for_document_id_(const IPostings &p,
                                            size_t document_id) {
  size_t lo = 0, hi = p.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (p.document_id(mid) < document_id) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo < p.size() && p.document_id(lo) == document_id) {
    return lo;
  }
  return p.size();
}

size_t InMemoryInvertedIndexBase::document_count() const {
  return documents_.size();
}

size_t
InMemoryInvertedIndexBase::document_term_count(size_t document_id) const {
  return documents_.at(document_id).term_count;
}

double InMemoryInvertedIndexBase::average_document_term_count() const {
  auto buffs = std::vector<std::pair<size_t, size_t>>{{0.0, 0}};
  for (const auto &[_, document] : documents_) {
    if (document.term_count <
        std::numeric_limits<size_t>::max() - buffs.back().first) {
      buffs.back().first += document.term_count;
      buffs.back().second += 1;
    } else {
      buffs.emplace_back(std::pair(document.term_count, 1));
    }
  }

  double avg = 0.0;
  for (const auto [term_count, document_count] : buffs) {
    avg +=
        static_cast<double>(term_count) / static_cast<double>(document_count);
  }
  return avg;
}

bool InMemoryInvertedIndexBase::term_exists(const std::u32string &str) const {
  return term_dictionary_.find(str) != term_dictionary_.end();
}

size_t InMemoryInvertedIndexBase::term_count(const std::u32string &str) const {
  auto it = term_dictionary_.find(str);
  return it != term_dictionary_.end() ? it->second.term_count : 0;
}

size_t InMemoryInvertedIndexBase::term_count(const std::u32string &str,
                                             size_t document_id) const {
  auto p = postings(str);
  auto i = find_postings_index_for_document_id_(*p, document_id);
  if (i < p->size()) {
    return p->search_hit_count(i);
  }
  return 0;
}

size_t InMemoryInvertedIndexBase::df(const std::u32string &str) const {
  return postings(str)->size();
}

double InMemoryInvertedIndexBase::tf(const std::u32string &str,
                                     size_t document_id) const {
  auto p = postings(str);
  auto i = find_postings_index_for_document_id_(*p, document_id);
  if (i < p->size()) {
    return static_cast<double>(p->search_hit_count(i)) /
           static_cast<double>(document_term_count(document_id));
  }
  return 0.0;
}

std::shared_ptr<const IPostings>
InMemoryInvertedIndexBase::postings(const std::u32string &str) const {
  static const auto empty_postings = std::make_shared<const Postings>();
  auto it = term_dictionary_.find(str);
  if (it != term_dictionary_.end()) {
    return std::shared_ptr<const IPostings>(
        std::shared_ptr<void>(), &it->second.postings);
  }
  return empty_postings;
}

} // namespace searchlib

