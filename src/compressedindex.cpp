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
// answers term_count(str, document_id) / tf the same way the in-memory
// index does.
size_t find_postings_index_for_document_id_(const IPostings &p,
                                            size_t document_id);

namespace {

// IPostings served directly from the four Elias-Fano sequences written by
// Postings::save_compressed, without expanding them (see
// docs/postings_compression_design.ja.md section 5.5). Every accessor is an
// O(1) EF access; is_term_position uses next_geq on the monotonized
// position sequence instead of a binary search over a materialized vector.
class EFPostings : public IPostings {
public:
  void load(std::istream &is) {
    document_ids_.load(is);
    end_offsets_.load(is);
    bases_.load(is);
    positions_.load(is);
    if (end_offsets_.size() != document_ids_.size() ||
        bases_.size() != document_ids_.size() ||
        (document_ids_.size() > 0 &&
         positions_.size() != end_offsets_.access(end_offsets_.size() - 1))) {
      throw std::runtime_error("searchlib: corrupt compressed postings");
    }
  }

  size_t size() const override { return document_ids_.size(); }

  size_t document_id(size_t index) const override {
    return static_cast<size_t>(document_ids_.access(index));
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

  detail::EliasFano document_ids_;
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
    document_ids_.load(is);
    end_offsets_.load(is);
    bases_.load(is);
    positions_.load(is);
    cumulative_lengths_.load(is);

    auto document_count = document_ids_.size();
    auto total_values =
        document_count == 0 ? 0 : end_offsets_.access(document_count - 1);
    if (end_offsets_.size() != document_count ||
        bases_.size() != document_count ||
        positions_.size() != total_values ||
        cumulative_lengths_.size() != total_values) {
      throw std::runtime_error("searchlib: corrupt compressed text ranges");
    }
  }

  TextRange text_range(size_t document_id, size_t term_pos) const {
    auto i = document_ids_.next_geq(document_id);
    if (i == document_ids_.size() || document_ids_.access(i) != document_id) {
      throw std::out_of_range("searchlib: unknown document_id");
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
  detail::EliasFano document_ids_;
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
class CompressedInvertedIndex : public IInvertedIndexWithTextRange<TextRange> {
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

    // Section order mirrors InMemoryInvertedIndexBase::load followed by the
    // text-range section of InMemoryInvertedIndex<TextRange>::load.
    auto document_count = detail::read_scalar<uint64_t>(is);
    documents_.reserve(static_cast<size_t>(document_count));
    for (uint64_t i = 0; i < document_count; i++) {
      auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      auto term_count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      documents_[document_id] = term_count;
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
    removed_document_ids_.reserve(static_cast<size_t>(removed_count));
    for (uint64_t i = 0; i < removed_count; i++) {
      removed_document_ids_.insert(
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
        detail::read_scalar<uint64_t>(is); // document_id
        detail::EliasFano discarded;
        discarded.load(is);
      }
    }

    if (detail::read_scalar<uint32_t>(is) != 1) {
      throw std::runtime_error(
          "searchlib: index text-range section requires TextRange");
    }
    text_ranges_.load(is);

    average_document_term_count_ = compute_average_document_term_count_();
  }

  size_t document_count() const override { return documents_.size(); }

  size_t document_term_count(size_t document_id) const override {
    return documents_.at(document_id);
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
                    size_t document_id) const override {
    auto p = postings(str);
    auto i = find_postings_index_for_document_id_(*p, document_id);
    if (i < p->size()) {
      return p->search_hit_count(i);
    }
    return 0;
  }

  size_t df(const std::u32string &str) const override {
    return postings(str)->size();
  }

  double tf(const std::u32string &str, size_t document_id) const override {
    auto p = postings(str);
    auto i = find_postings_index_for_document_id_(*p, document_id);
    if (i < p->size()) {
      return static_cast<double>(p->search_hit_count(i)) /
             static_cast<double>(document_term_count(document_id));
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

  bool has_removed_documents() const override {
    return !removed_document_ids_.empty();
  }

  bool is_document_removed(size_t document_id) const override {
    return removed_document_ids_.find(document_id) !=
           removed_document_ids_.end();
  }

  TextRange text_range(const IPostings &positions, size_t index,
                       size_t search_hit_index) const override {
    auto document_id = positions.document_id(index);
    auto term_pos = positions.term_position(index, search_hit_index);
    auto term_length = positions.term_length(index, search_hit_index);
    if (term_length == 1) {
      return text_ranges_.text_range(document_id, term_pos);
    }
    auto beg = text_ranges_.text_range(document_id, term_pos);
    auto end =
        text_ranges_.text_range(document_id, term_pos + term_length - 1);
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
  // Matches InMemoryInvertedIndexBase::average_document_term_count, including
  // the 0.0 it returns for an empty index.
  double compute_average_document_term_count_() const {
    if (documents_.empty()) {
      return 0.0;
    }
    size_t total = 0;
    for (const auto &[_, term_count] : documents_) {
      total += term_count;
    }
    return static_cast<double>(total) /
           static_cast<double>(documents_.size());
  }

  std::unordered_map<size_t /*document_id*/, size_t /*term_count*/> documents_;

  // Holding this makes the whole index non-copyable and non-movable, which
  // is required: the FST points into its own byte buffer (see termdict.h).
  detail::TermDictionaryFst term_dictionary_;
  std::vector<CompressedTerm> terms_; // indexed by the FST's ordinal

  std::unordered_set<size_t> removed_document_ids_;
  EFTextRanges text_ranges_;
  double average_document_term_count_ = 0.0;
};

} // namespace

std::shared_ptr<IInvertedIndexWithTextRange<TextRange>>
load_compressed_index(std::istream &is) {
  auto index = std::make_shared<CompressedInvertedIndex>();
  index->load(is);
  return index;
}

std::shared_ptr<IInvertedIndexWithTextRange<TextRange>>
load_compressed_index(const std::string &path) {
  std::ifstream is(path, std::ios::binary);
  if (!is) {
    throw std::runtime_error("searchlib: cannot open index file for reading: " +
                             path);
  }
  return load_compressed_index(is);
}

} // namespace searchlib
