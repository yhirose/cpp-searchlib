//
//  invertedindex.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <algorithm>
#include <numeric>

#include "searchlib.h"
#include "succinct.h"
#include "termdict.h"
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

IScopeIndex::~IScopeIndex() = default;

//-----------------------------------------------------------------------------

// Definition of the opaque type forward-declared in searchlib.h. One
// Elias-Fano sequence per (scope_name, document_id), each mapping term_pos ->
// scope ordinal via EliasFano::access (see docs comment on ScopeIndexData's
// forward declaration for why this stays out of the public header).
namespace detail {
class ScopeIndexData {
public:
  std::unordered_map<std::string, std::unordered_map<size_t, EliasFano>>
      by_name;
};
} // namespace detail

//-----------------------------------------------------------------------------

size_t InMemoryInvertedIndexBase::Postings::size() const {
  return document_ids_.size();
}

size_t InMemoryInvertedIndexBase::Postings::document_id(size_t index) const {
  return document_ids_[index];
}

size_t
InMemoryInvertedIndexBase::Postings::search_hit_count(size_t index) const {
  return offsets_[index + 1] - offsets_[index];
}

size_t InMemoryInvertedIndexBase::Postings::term_position(
    size_t index, size_t search_hit_index) const {
  return positions_[offsets_[index] + search_hit_index];
}

size_t InMemoryInvertedIndexBase::Postings::term_length(
    size_t index, size_t search_hit_index) const {
  return 1;
}

bool InMemoryInvertedIndexBase::Postings::is_term_position(
    size_t index, size_t term_pos) const {
  return std::binary_search(positions_.begin() + offsets_[index],
                            positions_.begin() + offsets_[index + 1], term_pos);
}

void InMemoryInvertedIndexBase::Postings::add_term_position(size_t document_id,
                                                            size_t term_pos) {
  // Indexing walks documents in ascending id order and emits each document's
  // positions in ascending order, so these two appends are the hot path and
  // both are O(1) amortized. The general insert below only runs for a caller
  // that indexes out of order, or re-indexes a document already present.
  if (!document_ids_.empty() && document_ids_.back() == document_id) {
    positions_.push_back(term_pos);
    offsets_.back() = positions_.size();
    return;
  }
  if (document_ids_.empty() || document_ids_.back() < document_id) {
    document_ids_.push_back(document_id);
    positions_.push_back(term_pos);
    offsets_.push_back(positions_.size());
    return;
  }

  auto it =
      std::lower_bound(document_ids_.begin(), document_ids_.end(), document_id);
  auto index = static_cast<size_t>(it - document_ids_.begin());

  if (it != document_ids_.end() && *it == document_id) {
    // Append to this document's slice, as pushing onto its own vector used
    // to, and shift the slices after it along.
    positions_.insert(positions_.begin() + offsets_[index + 1], term_pos);
    for (auto i = index + 1; i < offsets_.size(); i++) {
      offsets_[i]++;
    }
  } else {
    document_ids_.insert(it, document_id);
    positions_.insert(positions_.begin() + offsets_[index], term_pos);
    offsets_.insert(offsets_.begin() + index + 1, offsets_[index] + 1);
    for (auto i = index + 2; i < offsets_.size(); i++) {
      offsets_[i]++;
    }
  }
}

void InMemoryInvertedIndexBase::Postings::save(std::ostream &os) const {
  detail::write_scalar<uint64_t>(os, document_ids_.size());
  for (size_t i = 0; i < document_ids_.size(); i++) {
    detail::write_scalar<uint64_t>(os, document_ids_[i]);
    detail::write_scalar<uint64_t>(os, offsets_[i + 1] - offsets_[i]);
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      detail::write_scalar<uint64_t>(os, positions_[j]);
    }
  }
}

void InMemoryInvertedIndexBase::Postings::load(std::istream &is) {
  auto entry_count = detail::read_scalar<uint64_t>(is);
  document_ids_.clear();
  offsets_.assign(1, 0);
  positions_.clear();
  document_ids_.reserve(static_cast<size_t>(entry_count));
  offsets_.reserve(static_cast<size_t>(entry_count) + 1);
  for (uint64_t i = 0; i < entry_count; i++) {
    document_ids_.push_back(
        static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
    auto position_count = detail::read_scalar<uint64_t>(is);
    for (uint64_t j = 0; j < position_count; j++) {
      positions_.push_back(
          static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
    }
    offsets_.push_back(positions_.size());
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
  document_ids.reserve(document_ids_.size());
  end_offsets.reserve(document_ids_.size());
  bases.reserve(document_ids_.size());
  // One entry per element of the position arena, whose length the flat
  // layout has on hand -- and this is the largest of the four vectors.
  monotonized_positions.reserve(positions_.size());
  uint64_t total_positions = 0;
  uint64_t base = 0;
  for (size_t i = 0; i < document_ids_.size(); i++) {
    document_ids.push_back(document_ids_[i]);
    total_positions += offsets_[i + 1] - offsets_[i];
    end_offsets.push_back(total_positions);
    bases.push_back(base);
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      monotonized_positions.push_back(base + positions_[j]);
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

  document_ids_.clear();
  offsets_.assign(1, 0);
  positions_.clear();
  document_ids_.reserve(document_ids.size());
  offsets_.reserve(document_ids.size() + 1);
  // The corrupt-check above pinned monotonized_positions.size() to the total
  // position count, which is exactly what the decode loop pushes.
  positions_.reserve(monotonized_positions.size());
  uint64_t begin = 0;
  for (size_t i = 0; i < document_ids.size(); i++) {
    auto end = end_offsets.access(i);
    auto base = bases.access(i);
    if (end < begin) {
      throw std::runtime_error("searchlib: corrupt compressed postings");
    }
    for (auto j = begin; j < end; j++) {
      auto value = monotonized_positions.access(static_cast<size_t>(j));
      if (value < base) {
        throw std::runtime_error("searchlib: corrupt compressed postings");
      }
      positions_.push_back(static_cast<size_t>(value - base));
    }
    document_ids_.push_back(static_cast<size_t>(document_ids.access(i)));
    offsets_.push_back(positions_.size());
    begin = end;
  }
}

//-----------------------------------------------------------------------------

namespace detail {

// The text-range section compressed with the same monotonization scheme as
// the postings: token positions are strictly increasing within a document,
// so a per-document base turns their concatenation into one monotone
// Elias-Fano sequence, and token lengths (>= 1) become monotone as a
// cumulative sum. Five global EF sequences cover the whole section, so the
// per-document overhead is negligible even for many small documents.
void save_text_ranges_compressed(std::ostream &os,
                                 const TextRangeList<TextRange> &list) {
  std::vector<uint64_t> document_ids;
  document_ids.reserve(list.size());
  for (const auto &[document_id, _] : list) {
    document_ids.push_back(document_id);
  }
  std::sort(document_ids.begin(), document_ids.end());

  std::vector<uint64_t> end_offsets;
  std::vector<uint64_t> bases;
  std::vector<uint64_t> positions;
  std::vector<uint64_t> cumulative_lengths;
  end_offsets.reserve(document_ids.size());
  bases.reserve(document_ids.size());
  uint64_t total_values = 0;
  uint64_t base = 0;
  uint64_t length_sum = 0;
  for (auto document_id : document_ids) {
    const auto &values = list.at(static_cast<size_t>(document_id));
    total_values += values.size();
    end_offsets.push_back(total_values);
    bases.push_back(base);
    for (const auto &value : values) {
      positions.push_back(base + value.position);
      length_sum += value.length;
      cumulative_lengths.push_back(length_sum);
    }
    if (!values.empty()) {
      base = positions.back() + 1;
    }
  }

  auto save_sequence = [&os](const std::vector<uint64_t> &values) {
    EliasFano(values, values.empty() ? 0 : values.back() + 1).save(os);
  };
  save_sequence(document_ids);
  save_sequence(end_offsets);
  save_sequence(bases);
  save_sequence(positions);
  save_sequence(cumulative_lengths);
}

void load_text_ranges_compressed(std::istream &is,
                                 TextRangeList<TextRange> &list) {
  EliasFano document_ids;
  document_ids.load(is);
  EliasFano end_offsets;
  end_offsets.load(is);
  EliasFano bases;
  bases.load(is);
  EliasFano positions;
  positions.load(is);
  EliasFano cumulative_lengths;
  cumulative_lengths.load(is);

  auto document_count = document_ids.size();
  auto total_values =
      document_count == 0 ? 0 : end_offsets.access(document_count - 1);
  if (end_offsets.size() != document_count ||
      bases.size() != document_count || positions.size() != total_values ||
      cumulative_lengths.size() != total_values) {
    throw std::runtime_error("searchlib: corrupt compressed text ranges");
  }

  uint64_t begin = 0;
  uint64_t previous_length_sum = 0;
  for (size_t i = 0; i < document_count; i++) {
    auto end = end_offsets.access(i);
    auto base = bases.access(i);
    if (end < begin) {
      throw std::runtime_error("searchlib: corrupt compressed text ranges");
    }
    std::vector<TextRange> values;
    values.reserve(static_cast<size_t>(end - begin));
    for (auto j = begin; j < end; j++) {
      auto position = positions.access(static_cast<size_t>(j));
      auto length_sum = cumulative_lengths.access(static_cast<size_t>(j));
      if (position < base || length_sum < previous_length_sum) {
        throw std::runtime_error("searchlib: corrupt compressed text ranges");
      }
      values.push_back(
          TextRange{static_cast<size_t>(position - base),
                    static_cast<size_t>(length_sum - previous_length_sum)});
      previous_length_sum = length_sum;
    }
    list[static_cast<size_t>(document_ids.access(i))] = std::move(values);
    begin = end;
  }
}

} // namespace detail

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

void InMemoryInvertedIndexBase::set_document_term_count(size_t document_id,
                                                        size_t term_count) {
  auto [it, inserted] = documents_.emplace(document_id, Document{term_count});
  if (!inserted) {
    // Re-indexing an existing document_id replaces its term count.
    total_document_term_count_ -= it->second.term_count;
    it->second.term_count = term_count;
  }
  total_document_term_count_ += term_count;
}

double InMemoryInvertedIndexBase::average_document_term_count() const {
  if (documents_.empty()) {
    return 0.0;
  }
  return static_cast<double>(total_document_term_count_) /
         static_cast<double>(documents_.size());
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

void InMemoryInvertedIndexBase::enumerate_terms_with_prefix(
    const std::u32string &prefix,
    const std::function<void(const std::u32string &str)> &callback) const {
  // term_dictionary_ is a hash map, so there is no subtree to descend into:
  // every term has to be tested. Linear in the vocabulary size, which is the
  // price of keeping the writable index cheap to mutate.
  for (const auto &[str, term] : term_dictionary_) {
    if (str.size() >= prefix.size() &&
        str.compare(0, prefix.size(), prefix) == 0) {
      callback(str);
    }
  }
}

namespace {

// True if `term` matches the glob `pattern` (`*` = zero or more codepoints,
// every other codepoint literal). Standard two-pointer greedy matcher with
// backtracking to the most recent `*`: linear in practice, quadratic only on
// adversarial inputs (many stars each forced to backtrack).
bool matches_wildcard(const std::u32string &pattern,
                      const std::u32string &term) {
  size_t p = 0, t = 0;
  size_t star = std::u32string::npos, star_match = 0;
  while (t < term.size()) {
    if (p < pattern.size() &&
        (pattern[p] == U'*' || pattern[p] == term[t])) {
      if (pattern[p] == U'*') {
        star = p++;
        star_match = t;
      } else {
        p++;
        t++;
      }
    } else if (star != std::u32string::npos) {
      p = star + 1;
      t = ++star_match;
    } else {
      return false;
    }
  }
  while (p < pattern.size() && pattern[p] == U'*') {
    p++;
  }
  return p == pattern.size();
}

// Levenshtein distance, capped: once every cell of a row exceeds max_edits no
// later row can come back under it, so the walk stops there instead of
// finishing a distance the caller is going to reject anyway. Two rolling rows
// rather than a full matrix, since only the previous one is ever read.
//
// The rows are the caller's, not locals, because this runs once per term of a
// full dictionary scan and std::vector has no small-buffer optimization, so
// owning them here would mean two allocations per term tested. Passing `b` the
// string that stays fixed across the scan also makes the resize a no-op after
// the first term. Both rows are pure scratch; nothing is read back out.
bool within_edit_distance(const std::u32string &a, const std::u32string &b,
                          size_t max_edits, std::vector<size_t> &prev,
                          std::vector<size_t> &curr) {
  prev.resize(b.size() + 1);
  curr.resize(b.size() + 1);
  std::iota(prev.begin(), prev.end(), size_t{0});

  for (size_t i = 1; i <= a.size(); i++) {
    curr[0] = i;
    auto row_min = curr[0];
    for (size_t j = 1; j <= b.size(); j++) {
      auto cost = a[i - 1] == b[j - 1] ? size_t{0} : size_t{1};
      curr[j] = std::min({curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost});
      row_min = std::min(row_min, curr[j]);
    }
    if (row_min > max_edits) {
      return false;
    }
    std::swap(prev, curr);
  }

  return prev[b.size()] <= max_edits;
}

} // namespace

void InMemoryInvertedIndexBase::enumerate_terms_with_wildcard(
    const std::u32string &pattern,
    const std::function<void(const std::u32string &str)> &callback) const {
  // Same full-scan tradeoff as enumerate_terms_with_prefix: term_dictionary_
  // is a hash map, so every term has to be tested against the pattern.
  for (const auto &[str, term] : term_dictionary_) {
    if (matches_wildcard(pattern, str)) {
      callback(str);
    }
  }
}

void InMemoryInvertedIndexBase::enumerate_terms_with_edit_distance(
    const std::u32string &target, size_t max_edits,
    const std::function<void(const std::u32string &str)> &callback) const {
  // Same full-scan tradeoff as the other two enumerators. The length check
  // first because it settles most terms without touching the DP: a term can
  // only be within max_edits if its length is, since every edit changes the
  // length by at most one.
  //
  // The DP rows live out here rather than inside within_edit_distance so that
  // one pair of allocations covers the whole scan. They are locals, not
  // members: a mutable member cache would be a data race under
  // ThreadSafeInvertedIndex's shared_lock, which this const accessor runs
  // beneath.
  std::vector<size_t> prev;
  std::vector<size_t> curr;
  for (const auto &[str, term] : term_dictionary_) {
    auto shorter = std::min(target.size(), str.size());
    auto longer = std::max(target.size(), str.size());
    if (longer - shorter > max_edits) {
      continue;
    }
    if (within_edit_distance(str, target, max_edits, prev, curr)) {
      callback(str);
    }
  }
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

void InMemoryInvertedIndexBase::set_scope_ids(
    const std::string &scope_name, size_t document_id,
    const std::vector<size_t> &scope_ids) {
  if (scope_ids.size() != document_term_count(document_id)) {
    throw std::invalid_argument(
        "searchlib: scope_ids.size() must equal document_term_count()");
  }
  if (!std::is_sorted(scope_ids.begin(), scope_ids.end())) {
    throw std::invalid_argument(
        "searchlib: scope_ids must be monotonically non-decreasing");
  }

  if (!scope_data_) {
    scope_data_ = std::make_shared<detail::ScopeIndexData>();
  }

  auto universe =
      scope_ids.empty() ? uint64_t(1) : uint64_t(scope_ids.back()) + 1;
  std::vector<uint64_t> values(scope_ids.begin(), scope_ids.end());
  scope_data_->by_name[scope_name].insert_or_assign(
      document_id, detail::EliasFano(values, universe));
}

bool InMemoryInvertedIndexBase::has_scope(const std::string &scope_name,
                                          size_t document_id) const {
  if (!scope_data_) {
    return false;
  }
  auto it = scope_data_->by_name.find(scope_name);
  if (it == scope_data_->by_name.end()) {
    return false;
  }
  return it->second.find(document_id) != it->second.end();
}

size_t InMemoryInvertedIndexBase::scope_id(const std::string &scope_name,
                                           size_t document_id,
                                           size_t term_pos) const {
  if (!scope_data_) {
    return static_cast<size_t>(-1);
  }
  auto it = scope_data_->by_name.find(scope_name);
  if (it == scope_data_->by_name.end()) {
    return static_cast<size_t>(-1);
  }
  auto doc_it = it->second.find(document_id);
  if (doc_it == it->second.end()) {
    return static_cast<size_t>(-1);
  }
  return static_cast<size_t>(doc_it->second.access(term_pos));
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

  if (format == IndexFormat::Compressed) {
    // The term strings go into one FST instead of being written per term;
    // the per-term records below are then indexed by the FST's ordinal,
    // which is the term's position in this sorted sequence (see termdict.h).
    std::vector<std::string> keys;
    keys.reserve(terms.size());
    for (const auto *term : terms) {
      keys.push_back(u8(term->str));
    }
    detail::write_term_dictionary_fst(os, keys);
  }

  for (const auto *term : terms) {
    if (format != IndexFormat::Compressed) {
      detail::write_u32string(os, term->str);
    }
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

  // Scope-index section: per scope_name (sorted), per document_id (sorted),
  // an Elias-Fano encoded term_pos->scope-ordinal sequence. Stored the same
  // way regardless of `format`, since EliasFano is already compact.
  if (scope_data_) {
    detail::write_scalar<uint64_t>(os, scope_data_->by_name.size());
    std::vector<std::string> scope_names;
    scope_names.reserve(scope_data_->by_name.size());
    for (const auto &[name, _] : scope_data_->by_name) {
      scope_names.push_back(name);
    }
    std::sort(scope_names.begin(), scope_names.end());
    for (const auto &name : scope_names) {
      detail::write_scalar<uint64_t>(os, name.size());
      os.write(name.data(), static_cast<std::streamsize>(name.size()));

      const auto &by_document = scope_data_->by_name.at(name);
      std::vector<size_t> doc_ids;
      doc_ids.reserve(by_document.size());
      for (const auto &[document_id, _] : by_document) {
        doc_ids.push_back(document_id);
      }
      std::sort(doc_ids.begin(), doc_ids.end());

      detail::write_scalar<uint64_t>(os, doc_ids.size());
      for (auto document_id : doc_ids) {
        detail::write_scalar<uint64_t>(os, document_id);
        by_document.at(document_id).save(os);
      }
    }
  } else {
    detail::write_scalar<uint64_t>(os, uint64_t(0));
  }
}

void InMemoryInvertedIndexBase::load(std::istream &is, IndexFormat format) {
  documents_.clear();
  total_document_term_count_ = 0;
  auto document_count = detail::read_scalar<uint64_t>(is);
  documents_.reserve(static_cast<size_t>(document_count));
  for (uint64_t i = 0; i < document_count; i++) {
    auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    auto term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    set_document_term_count(document_id, term_count);
  }

  term_dictionary_.clear();
  auto term_count = detail::read_scalar<uint64_t>(is);
  term_dictionary_.reserve(static_cast<size_t>(term_count));

  // In the Compressed format the term strings live in one FST ahead of the
  // per-term records; recover them up front so the loop below can stay
  // shared with the Plain format.
  std::vector<std::u32string> fst_terms;
  if (format == IndexFormat::Compressed) {
    detail::TermDictionaryFst fst;
    fst.load(is);
    fst_terms = fst.terms(static_cast<size_t>(term_count));
  }

  for (uint64_t i = 0; i < term_count; i++) {
    auto str = format == IndexFormat::Compressed
                   ? fst_terms[static_cast<size_t>(i)]
                   : detail::read_u32string(is);
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

  scope_data_.reset();
  auto scope_name_count = detail::read_scalar<uint64_t>(is);
  if (scope_name_count > 0) {
    scope_data_ = std::make_shared<detail::ScopeIndexData>();
    for (uint64_t i = 0; i < scope_name_count; i++) {
      auto name_size = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      std::string name(name_size, '\0');
      is.read(name.data(), static_cast<std::streamsize>(name_size));

      auto &by_document = scope_data_->by_name[name];
      auto doc_count = detail::read_scalar<uint64_t>(is);
      for (uint64_t j = 0; j < doc_count; j++) {
        auto document_id =
            static_cast<size_t>(detail::read_scalar<uint64_t>(is));
        detail::EliasFano ef;
        ef.load(is);
        by_document.insert_or_assign(document_id, std::move(ef));
      }
    }
  }
}

} // namespace searchlib

