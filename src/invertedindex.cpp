//
//  invertedindex.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <algorithm>
#include <cassert>
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

IKeyedIndex::~IKeyedIndex() = default;

//-----------------------------------------------------------------------------

// Definition of the opaque type forward-declared in searchlib.h. One
// Elias-Fano sequence per (scope_name, ordinal), each mapping term_pos ->
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
  return document_ordinals_.size();
}

size_t
InMemoryInvertedIndexBase::Postings::document_ordinal(size_t index) const {
  return document_ordinals_[index];
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

void InMemoryInvertedIndexBase::Postings::add_term_position(size_t ordinal,
                                                            size_t term_pos) {
  // Ordinals arrive in indexing order and each document's positions in
  // ascending order, so this is append-only: extend the current document's
  // slice, or open a new one. An ordinal below the last would mean a
  // document was revisited, which allocate_ordinal rules out -- there used
  // to be a sorted-insert path here for callers indexing out of order, and
  // it is gone with them.
  if (!document_ordinals_.empty() && document_ordinals_.back() == ordinal) {
    positions_.push_back(term_pos);
    offsets_.back() = positions_.size();
    return;
  }
  assert(document_ordinals_.empty() || document_ordinals_.back() < ordinal);
  document_ordinals_.push_back(ordinal);
  positions_.push_back(term_pos);
  offsets_.push_back(positions_.size());
}

void InMemoryInvertedIndexBase::Postings::save(std::ostream &os) const {
  detail::write_scalar<uint64_t>(os, document_ordinals_.size());
  for (size_t i = 0; i < document_ordinals_.size(); i++) {
    detail::write_scalar<uint64_t>(os, document_ordinals_[i]);
    detail::write_scalar<uint64_t>(os, offsets_[i + 1] - offsets_[i]);
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      detail::write_scalar<uint64_t>(os, positions_[j]);
    }
  }
}

void InMemoryInvertedIndexBase::Postings::load(std::istream &is) {
  auto entry_count = detail::read_scalar<uint64_t>(is);
  document_ordinals_.clear();
  offsets_.assign(1, 0);
  positions_.clear();
  document_ordinals_.reserve(static_cast<size_t>(entry_count));
  offsets_.reserve(static_cast<size_t>(entry_count) + 1);
  for (uint64_t i = 0; i < entry_count; i++) {
    document_ordinals_.push_back(
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
  // ordinals and the end offsets of each entry's slice in the concatenated
  // position array are strictly increasing as-is. The positions themselves
  // restart at every document, so each entry's positions get a per-entry
  // base added (base = previous base + previous entry's last position + 1),
  // which makes the concatenation strictly increasing too; the bases form a
  // fourth monotone sequence so that a slice can be decoded (or randomly
  // accessed later) by subtracting its base.
  std::vector<uint64_t> ordinals;
  std::vector<uint64_t> end_offsets;
  std::vector<uint64_t> bases;
  std::vector<uint64_t> monotonized_positions;
  ordinals.reserve(document_ordinals_.size());
  end_offsets.reserve(document_ordinals_.size());
  bases.reserve(document_ordinals_.size());
  // One entry per element of the position arena, whose length the flat
  // layout has on hand -- and this is the largest of the four vectors.
  monotonized_positions.reserve(positions_.size());
  uint64_t total_positions = 0;
  uint64_t base = 0;
  for (size_t i = 0; i < document_ordinals_.size(); i++) {
    ordinals.push_back(document_ordinals_[i]);
    total_positions += offsets_[i + 1] - offsets_[i];
    end_offsets.push_back(total_positions);
    bases.push_back(base);
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      monotonized_positions.push_back(base + positions_[j]);
    }
    base = monotonized_positions.back() + 1;
  }

  // A universe of last + 1 on a non-empty sequence, and 1 on an empty one:
  // no term in a dictionary is ever posting-less today, but a Postings is
  // also a public class, and an empty one must round-trip rather than read
  // past its end.
  auto universe = [](const std::vector<uint64_t> &v) {
    return v.empty() ? uint64_t(1) : v.back() + 1;
  };
  detail::EliasFano(ordinals, universe(ordinals)).save(os);
  detail::EliasFano(end_offsets, total_positions + 1).save(os);
  detail::EliasFano(bases, universe(bases)).save(os);
  detail::EliasFano(monotonized_positions, universe(monotonized_positions))
      .save(os);
}

void InMemoryInvertedIndexBase::Postings::load_compressed(std::istream &is) {
  detail::EliasFano ordinals;
  ordinals.load(is);
  detail::EliasFano end_offsets;
  end_offsets.load(is);
  detail::EliasFano bases;
  bases.load(is);
  detail::EliasFano monotonized_positions;
  monotonized_positions.load(is);
  if (end_offsets.size() != ordinals.size() ||
      bases.size() != ordinals.size() ||
      (ordinals.size() > 0 &&
       monotonized_positions.size() !=
           end_offsets.access(end_offsets.size() - 1))) {
    throw std::runtime_error("searchlib: corrupt compressed postings");
  }

  document_ordinals_.clear();
  offsets_.assign(1, 0);
  positions_.clear();
  document_ordinals_.reserve(ordinals.size());
  offsets_.reserve(ordinals.size() + 1);
  // The corrupt-check above pinned monotonized_positions.size() to the total
  // position count, which is exactly what the decode loop pushes.
  positions_.reserve(monotonized_positions.size());
  uint64_t begin = 0;
  for (size_t i = 0; i < ordinals.size(); i++) {
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
    document_ordinals_.push_back(static_cast<size_t>(ordinals.access(i)));
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
  std::vector<uint64_t> ordinals;
  ordinals.reserve(list.size());
  for (const auto &[ordinal, _] : list) {
    ordinals.push_back(ordinal);
  }
  std::sort(ordinals.begin(), ordinals.end());

  std::vector<uint64_t> end_offsets;
  std::vector<uint64_t> bases;
  std::vector<uint64_t> positions;
  std::vector<uint64_t> cumulative_lengths;
  end_offsets.reserve(ordinals.size());
  bases.reserve(ordinals.size());
  uint64_t total_values = 0;
  uint64_t base = 0;
  uint64_t length_sum = 0;
  for (auto ordinal : ordinals) {
    const auto &values = list.at(static_cast<size_t>(ordinal));
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
  save_sequence(ordinals);
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

// Assumes document_ordinal(index) is monotonically increasing in index,
// which holds for every IPostings this library produces: Postings is
// append-only in ordinal order, and SearchResult (search.cpp) only ever
// appends documents in ascending order via its cursor-merge algorithms.
size_t find_postings_index_for_ordinal_(const IPostings &p, size_t ordinal) {
  size_t lo = 0, hi = p.size();
  while (lo < hi) {
    size_t mid = lo + (hi - lo) / 2;
    if (p.document_ordinal(mid) < ordinal) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (lo < p.size() && p.document_ordinal(lo) == ordinal) {
    return lo;
  }
  return p.size();
}

size_t InMemoryInvertedIndexBase::document_count() const {
  return documents_.size() - removed_ordinals_.size();
}

size_t InMemoryInvertedIndexBase::document_term_count(size_t ordinal) const {
  return documents_.at(ordinal).term_count;
}

size_t InMemoryInvertedIndexBase::allocate_ordinal(size_t document_key) {
  auto it = key_to_ordinal_.find(document_key);
  if (it != key_to_ordinal_.end()) {
    tombstone_(it->second);
  }
  auto ordinal = documents_.size();
  documents_.push_back(Document{0, document_key});
  key_to_ordinal_[document_key] = ordinal;
  return ordinal;
}

void InMemoryInvertedIndexBase::set_document_term_count(size_t ordinal,
                                                        size_t term_count) {
  auto &document = documents_.at(ordinal);
  total_document_term_count_ -= document.term_count;
  document.term_count = term_count;
  total_document_term_count_ += term_count;
}

double InMemoryInvertedIndexBase::average_document_term_count() const {
  auto live = document_count();
  if (live == 0) {
    return 0.0;
  }
  return static_cast<double>(total_document_term_count_) /
         static_cast<double>(live);
}

size_t InMemoryInvertedIndexBase::document_key(size_t ordinal) const {
  return documents_.at(ordinal).key;
}

std::optional<size_t>
InMemoryInvertedIndexBase::document_ordinal(size_t document_key) const {
  auto it = key_to_ordinal_.find(document_key);
  if (it == key_to_ordinal_.end()) {
    return std::nullopt;
  }
  return it->second;
}

void InMemoryInvertedIndexBase::tombstone_(size_t ordinal) {
  if (removed_ordinals_.insert(ordinal).second) {
    total_document_term_count_ -= documents_[ordinal].term_count;
  }
}

bool InMemoryInvertedIndexBase::term_exists(const std::u32string &str) const {
  return term_dictionary_.find(str) != term_dictionary_.end();
}

size_t InMemoryInvertedIndexBase::term_count(const std::u32string &str) const {
  auto it = term_dictionary_.find(str);
  return it != term_dictionary_.end() ? it->second.term_count : 0;
}

size_t InMemoryInvertedIndexBase::term_count(const std::u32string &str,
                                             size_t ordinal) const {
  auto p = postings(str);
  auto i = find_postings_index_for_ordinal_(*p, ordinal);
  if (i < p->size()) {
    return p->search_hit_count(i);
  }
  return 0;
}

size_t InMemoryInvertedIndexBase::df(const std::u32string &str) const {
  return postings(str)->size();
}

double InMemoryInvertedIndexBase::tf(const std::u32string &str,
                                     size_t ordinal) const {
  auto p = postings(str);
  auto i = find_postings_index_for_ordinal_(*p, ordinal);
  if (i < p->size()) {
    return static_cast<double>(p->search_hit_count(i)) /
           static_cast<double>(document_term_count(ordinal));
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
  return !removed_ordinals_.empty();
}

bool InMemoryInvertedIndexBase::is_document_removed(size_t ordinal) const {
  return removed_ordinals_.find(ordinal) != removed_ordinals_.end();
}

void InMemoryInvertedIndexBase::remove_document(size_t document_key) {
  auto it = key_to_ordinal_.find(document_key);
  if (it != key_to_ordinal_.end()) {
    tombstone_(it->second);
  }
}

void InMemoryInvertedIndexBase::set_scope_ids(
    const std::string &scope_name, size_t document_key,
    const std::vector<size_t> &scope_ids) {
  auto ordinal = document_ordinal(document_key);
  if (!ordinal) {
    throw std::invalid_argument(
        "searchlib: set_scope_ids: the key names no document");
  }
  if (scope_ids.size() != document_term_count(*ordinal)) {
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
      *ordinal, detail::EliasFano(values, universe));
}

bool InMemoryInvertedIndexBase::has_scope(const std::string &scope_name,
                                          size_t ordinal) const {
  if (!scope_data_) {
    return false;
  }
  auto it = scope_data_->by_name.find(scope_name);
  if (it == scope_data_->by_name.end()) {
    return false;
  }
  return it->second.find(ordinal) != it->second.end();
}

size_t InMemoryInvertedIndexBase::scope_id(const std::string &scope_name,
                                           size_t ordinal,
                                           size_t term_pos) const {
  if (!scope_data_) {
    return static_cast<size_t>(-1);
  }
  auto it = scope_data_->by_name.find(scope_name);
  if (it == scope_data_->by_name.end()) {
    return static_cast<size_t>(-1);
  }
  auto doc_it = it->second.find(ordinal);
  if (doc_it == it->second.end()) {
    return static_cast<size_t>(-1);
  }
  return static_cast<size_t>(doc_it->second.access(term_pos));
}

void InMemoryInvertedIndexBase::save(std::ostream &os,
                                     IndexFormat format) const {
  // Documents section, in ordinal order. The ordinal is the position, so
  // each record carries only the term count and the caller's key; a
  // tombstoned document is written like any other, since its ordinal must
  // keep its slot for the postings that still name it.
  detail::write_scalar<uint64_t>(os, documents_.size());
  for (const auto &document : documents_) {
    detail::write_scalar<uint64_t>(os, document.term_count);
    detail::write_scalar<uint64_t>(os, document.key);
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
  detail::write_scalar<uint64_t>(os, removed_ordinals_.size());
  std::vector<size_t> removed(removed_ordinals_.begin(),
                              removed_ordinals_.end());
  std::sort(removed.begin(), removed.end());
  for (auto ordinal : removed) {
    detail::write_scalar<uint64_t>(os, ordinal);
  }

  // Scope-index section: per scope_name (sorted), per ordinal (sorted), an
  // Elias-Fano encoded term_pos->scope-ordinal sequence. Stored the same
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
      std::vector<size_t> ordinals;
      ordinals.reserve(by_document.size());
      for (const auto &[ordinal, _] : by_document) {
        ordinals.push_back(ordinal);
      }
      std::sort(ordinals.begin(), ordinals.end());

      detail::write_scalar<uint64_t>(os, ordinals.size());
      for (auto ordinal : ordinals) {
        detail::write_scalar<uint64_t>(os, ordinal);
        by_document.at(ordinal).save(os);
      }
    }
  } else {
    detail::write_scalar<uint64_t>(os, uint64_t(0));
  }
}

void InMemoryInvertedIndexBase::load(std::istream &is, IndexFormat format) {
  documents_.clear();
  key_to_ordinal_.clear();
  auto document_count = detail::read_scalar<uint64_t>(is);
  documents_.reserve(static_cast<size_t>(document_count));
  for (uint64_t i = 0; i < document_count; i++) {
    auto term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    auto key = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    documents_.push_back(Document{term_count, key});
    // Records are in ordinal order, so a key that was re-indexed ends up
    // mapped to its newest ordinal -- the one that is live, unless it too
    // was removed, which is exactly what the in-memory map would say.
    key_to_ordinal_[key] = static_cast<size_t>(i);
  }
  // The running total is settled once the tombstones below are known.
  total_document_term_count_ = 0;

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

  removed_ordinals_.clear();
  auto removed_count = detail::read_scalar<uint64_t>(is);
  removed_ordinals_.reserve(static_cast<size_t>(removed_count));
  for (uint64_t i = 0; i < removed_count; i++) {
    removed_ordinals_.insert(
        static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
  }
  for (size_t ordinal = 0; ordinal < documents_.size(); ordinal++) {
    if (removed_ordinals_.find(ordinal) == removed_ordinals_.end()) {
      total_document_term_count_ += documents_[ordinal].term_count;
    }
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
        auto ordinal = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
        detail::EliasFano ef;
        ef.load(is);
        by_document.insert_or_assign(ordinal, std::move(ef));
      }
    }
  }
}

} // namespace searchlib

