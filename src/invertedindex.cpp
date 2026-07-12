//
//  invertedindex.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"
#include "succinct.h"
#include "utils.h"

namespace searchlib {

// Terms with fewer postings entries than this keep the plain fixed-width
// encoding even in the Compressed format: term frequencies are Zipf
// distributed, so most terms have tiny postings where the Elias-Fano
// structures' fixed overhead would exceed the savings. The chosen
// representation is recorded per term in the file, so this threshold can be
// tuned without breaking compatibility.
static constexpr size_t kCompressedPostingsThreshold = 64;

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

void InMemoryInvertedIndexBase::Postings::save(std::ostream &os) const {
  detail::write_scalar<uint64_t>(os, positions_.size());
  for (const auto &[document_id, positions] : positions_) {
    detail::write_scalar<uint64_t>(os, document_id);
    detail::write_scalar<uint64_t>(os, positions.size());
    for (auto position : positions) {
      detail::write_scalar<uint64_t>(os, position);
    }
  }
}

void InMemoryInvertedIndexBase::Postings::load(std::istream &is) {
  auto entry_count = detail::read_scalar<uint64_t>(is);
  positions_.clear();
  positions_.reserve(static_cast<size_t>(entry_count));
  for (uint64_t i = 0; i < entry_count; i++) {
    auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    auto position_count = detail::read_scalar<uint64_t>(is);
    std::vector<size_t> positions;
    positions.reserve(static_cast<size_t>(position_count));
    for (uint64_t j = 0; j < position_count; j++) {
      positions.push_back(static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
    }
    positions_.emplace_back(document_id, std::move(positions));
  }
}

void InMemoryInvertedIndexBase::Postings::save_compressed(
    std::ostream &os) const {
  // Everything becomes a monotone sequence and is Elias-Fano coded:
  // document_ids and the end offsets of each entry's slice in the
  // concatenated position array are strictly increasing as-is. The
  // positions themselves restart at every document, so each entry's
  // positions get a per-entry base added (base = previous base + previous
  // entry's last position + 1), which makes the concatenation strictly
  // increasing too; the bases form a fourth monotone sequence so that a
  // slice can be decoded (or randomly accessed later) by subtracting its
  // base.
  std::vector<uint64_t> document_ids;
  std::vector<uint64_t> end_offsets;
  std::vector<uint64_t> bases;
  std::vector<uint64_t> monotonized_positions;
  document_ids.reserve(positions_.size());
  end_offsets.reserve(positions_.size());
  bases.reserve(positions_.size());
  uint64_t total_positions = 0;
  uint64_t base = 0;
  for (const auto &[document_id, positions] : positions_) {
    document_ids.push_back(document_id);
    total_positions += positions.size();
    end_offsets.push_back(total_positions);
    bases.push_back(base);
    for (auto position : positions) {
      monotonized_positions.push_back(base + position);
    }
    base = monotonized_positions.back() + 1;
  }

  detail::EliasFano(document_ids, document_ids.back() + 1).save(os);
  detail::EliasFano(end_offsets, total_positions + 1).save(os);
  detail::EliasFano(bases, bases.back() + 1).save(os);
  detail::EliasFano(monotonized_positions, monotonized_positions.back() + 1)
      .save(os);
}

void InMemoryInvertedIndexBase::Postings::load_compressed(std::istream &is) {
  detail::EliasFano document_ids;
  document_ids.load(is);
  detail::EliasFano end_offsets;
  end_offsets.load(is);
  detail::EliasFano bases;
  bases.load(is);
  detail::EliasFano monotonized_positions;
  monotonized_positions.load(is);
  if (end_offsets.size() != document_ids.size() ||
      bases.size() != document_ids.size() ||
      (document_ids.size() > 0 &&
       monotonized_positions.size() !=
           end_offsets.access(end_offsets.size() - 1))) {
    throw std::runtime_error("searchlib: corrupt compressed postings");
  }

  positions_.clear();
  positions_.reserve(document_ids.size());
  uint64_t begin = 0;
  for (size_t i = 0; i < document_ids.size(); i++) {
    auto end = end_offsets.access(i);
    auto base = bases.access(i);
    if (end < begin) {
      throw std::runtime_error("searchlib: corrupt compressed postings");
    }
    std::vector<size_t> positions;
    positions.reserve(static_cast<size_t>(end - begin));
    for (auto j = begin; j < end; j++) {
      auto value = monotonized_positions.access(static_cast<size_t>(j));
      if (value < base) {
        throw std::runtime_error("searchlib: corrupt compressed postings");
      }
      positions.push_back(static_cast<size_t>(value - base));
    }
    positions_.emplace_back(static_cast<size_t>(document_ids.access(i)),
                            std::move(positions));
    begin = end;
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

bool InMemoryInvertedIndexBase::has_removed_documents() const {
  return !removed_document_ids_.empty();
}

bool InMemoryInvertedIndexBase::is_document_removed(size_t document_id) const {
  return removed_document_ids_.find(document_id) != removed_document_ids_.end();
}

void InMemoryInvertedIndexBase::remove_document(size_t document_id) {
  removed_document_ids_.insert(document_id);
}

void InMemoryInvertedIndexBase::save(std::ostream &os,
                                     IndexFormat format) const {
  // Documents section, ordered by document_id for deterministic output.
  detail::write_scalar<uint64_t>(os, documents_.size());
  std::vector<size_t> document_ids;
  document_ids.reserve(documents_.size());
  for (const auto &[document_id, _] : documents_) {
    document_ids.push_back(document_id);
  }
  std::sort(document_ids.begin(), document_ids.end());
  for (auto document_id : document_ids) {
    detail::write_scalar<uint64_t>(os, document_id);
    detail::write_scalar<uint64_t>(os, documents_.at(document_id).term_count);
  }

  // Term dictionary section, ordered by term string for deterministic output.
  detail::write_scalar<uint64_t>(os, term_dictionary_.size());
  std::vector<const Term *> terms;
  terms.reserve(term_dictionary_.size());
  for (const auto &[_, term] : term_dictionary_) {
    terms.push_back(&term);
  }
  std::sort(terms.begin(), terms.end(),
            [](const Term *a, const Term *b) { return a->str < b->str; });
  for (const auto *term : terms) {
    detail::write_u32string(os, term->str);
    detail::write_scalar<uint64_t>(os, term->term_count);
    if (format == IndexFormat::Compressed) {
      // Per-term representation flag; see kCompressedPostingsThreshold.
      uint32_t compressed =
          term->postings.size() >= kCompressedPostingsThreshold;
      detail::write_scalar<uint32_t>(os, compressed);
      if (compressed) {
        term->postings.save_compressed(os);
      } else {
        term->postings.save(os);
      }
    } else {
      term->postings.save(os);
    }
  }

  // Removed-documents (tombstone) section, sorted for deterministic output.
  detail::write_scalar<uint64_t>(os, removed_document_ids_.size());
  std::vector<size_t> removed_ids(removed_document_ids_.begin(),
                                  removed_document_ids_.end());
  std::sort(removed_ids.begin(), removed_ids.end());
  for (auto document_id : removed_ids) {
    detail::write_scalar<uint64_t>(os, document_id);
  }
}

void InMemoryInvertedIndexBase::load(std::istream &is, IndexFormat format) {
  documents_.clear();
  auto document_count = detail::read_scalar<uint64_t>(is);
  documents_.reserve(static_cast<size_t>(document_count));
  for (uint64_t i = 0; i < document_count; i++) {
    auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    auto term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    documents_[document_id] = Document{term_count};
  }

  term_dictionary_.clear();
  auto term_count = detail::read_scalar<uint64_t>(is);
  term_dictionary_.reserve(static_cast<size_t>(term_count));
  for (uint64_t i = 0; i < term_count; i++) {
    auto str = detail::read_u32string(is);
    auto count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    auto &term = term_dictionary_[str];
    term.str = str;
    term.term_count = count;
    if (format == IndexFormat::Compressed) {
      auto compressed = detail::read_scalar<uint32_t>(is);
      if (compressed) {
        term.postings.load_compressed(is);
      } else {
        term.postings.load(is);
      }
    } else {
      term.postings.load(is);
    }
  }

  removed_document_ids_.clear();
  auto removed_count = detail::read_scalar<uint64_t>(is);
  removed_document_ids_.reserve(static_cast<size_t>(removed_count));
  for (uint64_t i = 0; i < removed_count; i++) {
    removed_document_ids_.insert(
        static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
  }
}

} // namespace searchlib

