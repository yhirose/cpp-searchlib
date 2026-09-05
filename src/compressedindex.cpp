//
//  compressedindex.cpp
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <limits>

#include "searchlib.h"
#include "succinct.h"
#include "termdict.h"
#include "utils.h"

namespace searchlib {

// Defined in invertedindex.cpp; shared here because the compressed backend
// answers term_count(str, ordinal) / tf the same way the in-memory index
// does.
size_t find_postings_index_for_ordinal_(const IPostings &p, size_t ordinal);

namespace {

// IPostings served directly from the four Elias-Fano sequences written by
// Postings::save_compressed, without expanding them (see
// docs/postings_compression_design.ja.md section 5.5). Every accessor is an
// O(1) EF access; is_term_position uses next_geq on the monotonized
// position sequence instead of a binary search over a materialized vector.
class EFPostings : public IPostings {
public:
  void load(std::istream &is) {
    ordinals_.load(is);
    end_offsets_.load(is);
    bases_.load(is);
    positions_.load(is);
    if (end_offsets_.size() != ordinals_.size() ||
        bases_.size() != ordinals_.size() ||
        (ordinals_.size() > 0 &&
         positions_.size() != end_offsets_.access(end_offsets_.size() - 1))) {
      throw std::runtime_error("searchlib: corrupt compressed postings");
    }
  }

  size_t size() const override { return ordinals_.size(); }

  size_t document_ordinal(size_t index) const override {
    return static_cast<size_t>(ordinals_.access(index));
  }

  size_t search_hit_count(size_t index) const override {
    return static_cast<size_t>(end_offsets_.access(index) - begin_(index));
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return static_cast<size_t>(
        positions_.access(static_cast<size_t>(begin_(index)) +
                          search_hit_index) -
        bases_.access(index));
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return 1;
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    // Phrase matching probes "position before the first term" as a
    // wrapped-around huge term_pos (see is_adjacent in search.cpp), which a
    // vector-backed binary search harmlessly misses. Here it would wrap
    // base + term_pos back into an earlier entry's slice, so reject it
    // explicitly.
    auto base = bases_.access(index);
    auto target = base + term_pos;
    if (target < base) {
      return false;
    }
    // The monotonized sequence is globally increasing and every value of an
    // earlier entry is below this entry's base, so next_geq lands inside
    // (or just past) this entry's slice.
    auto i = positions_.next_geq(target);
    return i < end_offsets_.access(index) && positions_.access(i) == target;
  }

private:
  uint64_t begin_(size_t index) const {
    return index == 0 ? 0 : end_offsets_.access(index - 1);
  }

  detail::EliasFano ordinals_;
  detail::EliasFano end_offsets_;
  detail::EliasFano bases_;
  detail::EliasFano positions_;
};

// The text-range section held as the five global Elias-Fano sequences
// written by save_text_ranges_compressed, answering point lookups without
// materializing the per-document vectors.
class EFTextRanges {
public:
  void load(std::istream &is) {
    ordinals_.load(is);
    end_offsets_.load(is);
    bases_.load(is);
    positions_.load(is);
    cumulative_lengths_.load(is);

    auto document_count = ordinals_.size();
    auto total_values =
        document_count == 0 ? 0 : end_offsets_.access(document_count - 1);
    if (end_offsets_.size() != document_count ||
        bases_.size() != document_count ||
        positions_.size() != total_values ||
        cumulative_lengths_.size() != total_values) {
      throw std::runtime_error("searchlib: corrupt compressed text ranges");
    }
  }

  TextRange text_range(size_t ordinal, size_t term_pos) const {
    auto i = ordinals_.next_geq(ordinal);
    if (i == ordinals_.size() || ordinals_.access(i) != ordinal) {
      throw std::out_of_range("searchlib: unknown ordinal");
    }
    auto begin = i == 0 ? 0 : end_offsets_.access(i - 1);
    auto j = begin + term_pos;
    if (j >= end_offsets_.access(i)) {
      throw std::out_of_range("searchlib: term position out of range");
    }
    auto position = positions_.access(static_cast<size_t>(j)) -
                    bases_.access(i);
    // Lengths are stored as one global cumulative sum (it never resets at
    // document boundaries), so the predecessor is simply entry j - 1.
    auto length_sum = cumulative_lengths_.access(static_cast<size_t>(j));
    auto previous_length_sum =
        j == 0 ? 0 : cumulative_lengths_.access(static_cast<size_t>(j - 1));
    return TextRange{static_cast<size_t>(position),
                     static_cast<size_t>(length_sum - previous_length_sum)};
  }

private:
  detail::EliasFano ordinals_;
  detail::EliasFano end_offsets_;
  detail::EliasFano bases_;
  detail::EliasFano positions_;
  detail::EliasFano cumulative_lengths_;
};

// The read-only index behind load_compressed_index. Documents and term
// statistics are small and kept expanded (bm25 needs them anyway); the
// memory heavyweights -- postings and text ranges -- stay Elias-Fano
// coded. Terms below the compression threshold were written plain and are
// held as ordinary Postings. Immutable after load, hence trivially safe to
// share across reader threads.
template <typename Key>
class CompressedInvertedIndex
    : public IInvertedIndexWithTextRange<TextRange, Key> {
public:
  void load(std::istream &is) {
    char magic[sizeof(detail::kIndexMagic)];
    is.read(magic, sizeof(magic));
    if (!is || std::memcmp(magic, detail::kIndexMagic, sizeof(magic)) != 0) {
      throw std::runtime_error("searchlib: not a valid index file (bad magic)");
    }
    if (detail::read_scalar<uint32_t>(is) != detail::kFormatTypeCompressed) {
      throw std::runtime_error(
          "searchlib: the compressed backend requires a Compressed-format "
          "index file");
    }
    if (detail::read_scalar<uint32_t>(is) != detail::kSchemaVersionCompressed) {
      throw std::runtime_error("searchlib: unsupported index schema_version");
    }

    // Section order mirrors InMemoryInvertedIndexBase::load, then the key
    // section, then the text-range section of
    // InMemoryInvertedIndex<TextRange, Key>::load. Document records are in
    // ordinal order, so the vector indexes by ordinal directly.
    auto document_count = detail::read_scalar<uint64_t>(is);
    term_counts_.reserve(static_cast<size_t>(document_count));
    for (uint64_t i = 0; i < document_count; i++) {
      term_counts_.push_back(
          static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
    }

    // Term dictionary: the FST stays as the dictionary rather than being
    // expanded into a hash map, so lookups run over the byte code in place
    // and prefix enumeration descends a subtree instead of scanning. The
    // per-term records that follow are in FST-ordinal order (see termdict.h),
    // so the ordinal indexes terms_ directly.
    auto term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    term_dictionary_.load(is);

    terms_.resize(term_count);
    for (size_t i = 0; i < term_count; i++) {
      auto &term = terms_[i];
      term.term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      if (detail::read_scalar<uint32_t>(is)) {
        auto postings = std::make_shared<EFPostings>();
        postings->load(is);
        term.postings = std::move(postings);
      } else {
        auto postings =
            std::make_shared<InMemoryInvertedIndexBase::Postings>();
        postings->load(is);
        term.postings = std::move(postings);
      }
    }

    auto removed_count = detail::read_scalar<uint64_t>(is);
    removed_ordinals_.reserve(static_cast<size_t>(removed_count));
    for (uint64_t i = 0; i < removed_count; i++) {
      removed_ordinals_.insert(
          static_cast<size_t>(detail::read_scalar<uint64_t>(is)));
    }

    // Scope-index section (see InMemoryInvertedIndexBase::save): read and
    // discard, since IScopeIndex is not exposed by this read-only backend
    // yet (v1 limitation, see docs/missing_features.ja.md 3.4.1). Skipping
    // it explicitly keeps the following sections aligned.
    auto scope_name_count = detail::read_scalar<uint64_t>(is);
    for (uint64_t i = 0; i < scope_name_count; i++) {
      auto name_size = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      std::string name(name_size, '\0');
      is.read(name.data(), static_cast<std::streamsize>(name_size));

      auto doc_count = detail::read_scalar<uint64_t>(is);
      for (uint64_t j = 0; j < doc_count; j++) {
        detail::read_scalar<uint64_t>(is); // ordinal
        detail::EliasFano discarded;
        discarded.load(is);
      }
    }

    keys_.load(is);
    if (keys_.size() != term_counts_.size()) {
      throw std::runtime_error(
          "searchlib: the key section disagrees with the documents section");
    }

    if (detail::read_scalar<uint32_t>(is) != 1) {
      throw std::runtime_error(
          "searchlib: index text-range section requires TextRange");
    }
    text_ranges_.load(is);

    average_document_term_count_ = compute_average_document_term_count_();
  }

  size_t document_count() const override {
    return term_counts_.size() - removed_ordinals_.size();
  }

  size_t document_term_count(size_t ordinal) const override {
    return term_counts_.at(ordinal);
  }

  const Key &document_key(size_t ordinal) const override {
    return keys_.key(ordinal);
  }

  std::optional<size_t>
  document_ordinal(const Key &document_key) const override {
    return keys_.ordinal(document_key);
  }

  double average_document_term_count() const override {
    return average_document_term_count_;
  }

  bool term_exists(const std::u32string &str) const override {
    return find_term_(str) != nullptr;
  }

  size_t term_count(const std::u32string &str) const override {
    const auto *term = find_term_(str);
    return term ? term->term_count : 0;
  }

  size_t term_count(const std::u32string &str,
                    size_t ordinal) const override {
    auto p = postings(str);
    auto i = find_postings_index_for_ordinal_(*p, ordinal);
    if (i < p->size()) {
      return p->search_hit_count(i);
    }
    return 0;
  }

  size_t df(const std::u32string &str) const override {
    return postings(str)->size();
  }

  double tf(const std::u32string &str, size_t ordinal) const override {
    auto p = postings(str);
    auto i = find_postings_index_for_ordinal_(*p, ordinal);
    if (i < p->size()) {
      return static_cast<double>(p->search_hit_count(i)) /
             static_cast<double>(document_term_count(ordinal));
    }
    return 0.0;
  }

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override {
    static const auto empty_postings =
        std::make_shared<const InMemoryInvertedIndexBase::Postings>();
    const auto *term = find_term_(str);
    return term ? term->postings : empty_postings;
  }

  void enumerate_terms_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    // Unlike the in-memory index, this descends straight to the subtree the
    // prefix names instead of testing every term, so the cost scales with the
    // number of matches rather than with the vocabulary size. It therefore
    // wins by a wide margin for the selective prefixes queries actually use,
    // and loses only when the prefix matches a large fraction of the
    // dictionary (roughly a tenth of it, measured on KJV).
    term_dictionary_.enumerate_with_prefix(prefix, callback);
  }

  void enumerate_terms_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    // Unlike enumerate_terms_with_prefix, this can't constrain descent to a
    // single subtree (a `*` may match anything), so the automaton walks the
    // whole FST -- still cheaper than the in-memory backend's full term scan
    // since can_match() prunes every subtree the pattern cannot possibly
    // match, and no per-term string has to be materialized to test it.
    term_dictionary_.enumerate_with_wildcard(pattern, callback);
  }

  void enumerate_terms_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    // Like the wildcard walk, this is an automaton driven over the whole FST
    // rather than a descent into one subtree, but the pruning is much tighter
    // here: can_match() rejects a subtree as soon as every alignment already
    // costs more than max_edits, which happens within a few characters for
    // most of the dictionary.
    term_dictionary_.enumerate_with_edit_distance(target, max_edits, callback);
  }

  bool has_removed_documents() const override {
    return !removed_ordinals_.empty();
  }

  bool is_document_removed(size_t ordinal) const override {
    return removed_ordinals_.find(ordinal) !=
           removed_ordinals_.end();
  }

  TextRange text_range(const IPostings &positions, size_t index,
                       size_t search_hit_index) const override {
    auto ordinal = positions.document_ordinal(index);
    auto term_pos = positions.term_position(index, search_hit_index);
    auto term_length = positions.term_length(index, search_hit_index);
    if (term_length == 1) {
      return text_ranges_.text_range(ordinal, term_pos);
    }
    auto beg = text_ranges_.text_range(ordinal, term_pos);
    auto end =
        text_ranges_.text_range(ordinal, term_pos + term_length - 1);
    return TextRange{beg.position, end.position + end.length - beg.position};
  }

private:
  struct CompressedTerm {
    size_t term_count = 0;
    std::shared_ptr<const IPostings> postings;
  };

  // Exact lookup through the FST: one walk over the byte code yields the
  // term's ordinal, which indexes terms_ directly.
  const CompressedTerm *find_term_(const std::u32string &str) const {
    uint32_t ordinal = 0;
    if (!term_dictionary_.find(str, ordinal) || ordinal >= terms_.size()) {
      return nullptr;
    }
    return &terms_[ordinal];
  }

  // Evaluated once at load time, since the index never changes afterwards.
  // Matches InMemoryInvertedIndexBase::average_document_term_count: live
  // documents only, and 0.0 when there are none.
  double compute_average_document_term_count_() const {
    auto live = document_count();
    if (live == 0) {
      return 0.0;
    }
    size_t total = 0;
    for (size_t ordinal = 0; ordinal < term_counts_.size(); ordinal++) {
      if (!is_document_removed(ordinal)) {
        total += term_counts_[ordinal];
      }
    }
    return static_cast<double>(total) / static_cast<double>(live);
  }

  std::vector<size_t> term_counts_; // indexed by ordinal
  KeyTable<Key> keys_;

  // Holding this makes the whole index non-copyable and non-movable, which
  // is required: the FST points into its own byte buffer (see termdict.h).
  detail::TermDictionaryFst term_dictionary_;
  std::vector<CompressedTerm> terms_; // indexed by the FST's ordinal

  std::unordered_set<size_t> removed_ordinals_;
  EFTextRanges text_ranges_;
  double average_document_term_count_ = 0.0;
};

} // namespace

template <typename Key>
std::shared_ptr<IInvertedIndexWithTextRange<TextRange, Key>>
load_compressed_index(std::istream &is) {
  auto index = std::make_shared<CompressedInvertedIndex<Key>>();
  index->load(is);
  return index;
}

template <typename Key>
std::shared_ptr<IInvertedIndexWithTextRange<TextRange, Key>>
load_compressed_index(const std::string &path) {
  std::ifstream is(path, std::ios::binary);
  if (!is) {
    throw std::runtime_error("searchlib: cannot open index file for reading: " +
                             path);
  }
  return load_compressed_index<Key>(is);
}

// The key types KeyTable serializes without help; another Key means adding
// its instantiation here, since the backend stays out of the public header.
template std::shared_ptr<IInvertedIndexWithTextRange<TextRange, size_t>>
load_compressed_index<size_t>(std::istream &is);
template std::shared_ptr<IInvertedIndexWithTextRange<TextRange, size_t>>
load_compressed_index<size_t>(const std::string &path);
template std::shared_ptr<IInvertedIndexWithTextRange<TextRange, std::string>>
load_compressed_index<std::string>(std::istream &is);
template std::shared_ptr<IInvertedIndexWithTextRange<TextRange, std::string>>
load_compressed_index<std::string>(const std::string &path);

} // namespace searchlib
