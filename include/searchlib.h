//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <istream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace searchlib {

//-----------------------------------------------------------------------------
// Serialization primitives (shared by the index save/load implementations)
//-----------------------------------------------------------------------------

namespace detail {

// The on-disk formats are host-endian dumps of fixed-width fields, as
// designed in docs/embedded_minimal_roadmap.ja.md 9.1. Every integer is
// normalized to a fixed 64-bit (or 32-bit for the header tags) width so that
// a 32-bit and a 64-bit build agree on layout; endianness is left
// host-native, which is acceptable because an index file is expected to be
// loaded on the same platform that wrote it.
//
// format_type identifies peer alternatives (0 = plain fixed-width dump,
// 2 = Elias-Fano compressed postings, 1 reserved for a future mmap layout;
// see docs/postings_compression_design.ja.md), while schema_version tracks
// layout evolution within one format_type, so each format versions
// independently.
inline constexpr char kIndexMagic[4] = {'S', 'I', 'D', 'X'};
inline constexpr uint32_t kFormatTypePlain = 0;
inline constexpr uint32_t kFormatTypeCompressed = 2;
inline constexpr uint32_t kSchemaVersionPlain = 2;
inline constexpr uint32_t kSchemaVersionCompressed = 1;

template <typename T> inline void write_scalar(std::ostream &os, T value) {
  static_assert(std::is_trivially_copyable_v<T>);
  os.write(reinterpret_cast<const char *>(&value), sizeof(T));
}

template <typename T> inline T read_scalar(std::istream &is) {
  static_assert(std::is_trivially_copyable_v<T>);
  T value{};
  is.read(reinterpret_cast<char *>(&value), sizeof(T));
  if (!is) {
    throw std::runtime_error("searchlib: unexpected end of index stream");
  }
  return value;
}

inline void write_u32string(std::ostream &os, const std::u32string &s) {
  write_scalar<uint64_t>(os, s.size());
  for (char32_t c : s) {
    write_scalar<uint32_t>(os, static_cast<uint32_t>(c));
  }
}

inline std::u32string read_u32string(std::istream &is) {
  auto n = read_scalar<uint64_t>(is);
  std::u32string s;
  s.reserve(static_cast<size_t>(n));
  for (uint64_t i = 0; i < n; i++) {
    s.push_back(static_cast<char32_t>(read_scalar<uint32_t>(is)));
  }
  return s;
}

} // namespace detail

//-----------------------------------------------------------------------------
// Interface
//-----------------------------------------------------------------------------

class IPostings {
public:
  virtual ~IPostings() = 0;

  virtual size_t size() const = 0;

  // document_id is scoped to the IInvertedIndex instance that produced this
  // IPostings; it is not a globally unique identifier across indexes. Code
  // that fans out across multiple indexes (e.g. a future federated search
  // layer) must tag results with their originating index rather than
  // assuming document_id alone is enough to disambiguate them.
  virtual size_t document_id(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_length(size_t index, size_t search_hit_index) const = 0;
  virtual bool is_term_position(size_t index, size_t term_pos) const = 0;
};

class IInvertedIndex {
public:
  virtual ~IInvertedIndex() = 0;

  virtual size_t document_count() const = 0;

  // document_id here (and everywhere else in this interface) is local to
  // this IInvertedIndex instance; see the note on IPostings::document_id.
  virtual size_t document_term_count(size_t document_id) const = 0;
  virtual double average_document_term_count() const = 0;

  virtual bool term_exists(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str,
                            size_t document_id) const = 0;

  virtual size_t df(const std::u32string &str) const = 0;
  virtual double tf(const std::u32string &str, size_t document_id) const = 0;

  virtual std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const = 0;

  // Logical (tombstone) deletion support. Read-only indexes report no
  // removals; searches filter out removed document_ids via these hooks.
  // Overridden by indexes that support IMutableInvertedIndex::remove_document.
  virtual bool has_removed_documents() const { return false; }
  virtual bool is_document_removed(size_t document_id) const { return false; }
};

using Normalizer = std::function<std::u32string(const std::u32string &str)>;

template <typename T>
using TextRangeList =
    std::unordered_map<size_t /*document_id*/, std::vector<T>>;

template <typename T>
using Tokenizer =
    std::function<void(Normalizer normalizer,
                       std::function<void(const std::u32string &str,
                                          size_t term_pos, T text_range)>
                           callback)>;

//-----------------------------------------------------------------------------
// Analyzer pipeline (see docs/analyzer_pipeline_design.ja.md)
//-----------------------------------------------------------------------------

// One composable stage of an analyzer pipeline. It receives a single token
// string and calls emit 0 times (drop, e.g. stop-word removal), 1 time
// (transform, e.g. lowercasing/stemming), or several times (expansion, e.g.
// synonyms). Equivalent to Lucene's TokenFilter / Tantivy's TokenFilter
// trait, but kept in the same push-style (continuation-passing) shape as
// Tokenizer<T>.
//
// Note: the type can express 1->N, but the index-side Analyzer<T> supports
// only 1->0 / 1->1 (v1); 1->N is reserved for query-side expansion. It is
// intentionally independent of the text-range type T so that the same string
// logic can be reused by parse_query (design section 6).
using TermFilter =
    std::function<void(const std::u32string &str,
                       std::function<void(std::u32string)> emit)>;

// Serially composes several TermFilters left-to-right into one TermFilter;
// the vector order is the application order. An empty vector yields the
// identity filter (emits its input once).
TermFilter compose(std::vector<TermFilter> filters);

// Lifts an existing Normalizer (a single 1->1 string transform) into a
// TermFilter that always emits exactly once. A null Normalizer lifts to the
// identity filter.
TermFilter to_term_filter(Normalizer normalizer);

template <typename T> class ITextRange {
public:
  virtual ~ITextRange(){};

  virtual T text_range(const IPostings &positions, size_t index,
                       size_t search_hit_index) const = 0;
};

template <typename T>
class IInvertedIndexWithTextRange : public IInvertedIndex,
                                    public ITextRange<T> {
public:
  virtual ~IInvertedIndexWithTextRange(){};
};

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or, Near, Not };

struct Expression {
  Operation operation;
  std::u32string term_str;
  size_t near_operation_distance;
  std::vector<Expression> nodes;
};

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query);

std::shared_ptr<IPostings> perform_search(const IInvertedIndex &invidx,
                                          const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

//-----------------------------------------------------------------------------
// Federated Search
//-----------------------------------------------------------------------------

// A mutable counterpart to IInvertedIndex. Kept as a separate interface
// (rather than discovered via dynamic_pointer_cast<IMutableInvertedIndex>)
// so that callers who already know an index is writable at registration
// time can pass that fact along explicitly, with no RTTI involved.
class IMutableInvertedIndex {
public:
  virtual ~IMutableInvertedIndex() = 0;

  // document_id is local to the corresponding IInvertedIndex; see the note
  // on IPostings::document_id.
  virtual void remove_document(size_t document_id) = 0;
};

// One entry in a FederatedIndex. mutable_index is null for read-only
// members (e.g. an installed book) and non-null for members that support
// removal (e.g. a notes index), decided by the caller at registration time.
struct FederationMember {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IMutableInvertedIndex> mutable_index;
};

// One hit produced by perform_federated_search. document ids are only
// meaningful together with `index`, since each member has its own local id
// space (see the note on IPostings::document_id).
struct FederatedHit {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
};

// A dynamic collection of independently-built IInvertedIndex instances that
// can be searched as one. Members are added/removed by shared_ptr identity;
// snapshot() returns a copy of the member list so that a search in
// progress is unaffected by concurrent add()/remove() calls (the
// underlying indexes are kept alive by the shared_ptr in the snapshot).
//
// This class only guards its own member list; it does not make any
// individual IInvertedIndex/IMutableInvertedIndex thread-safe on its own.
//
// There is deliberately no federation-level remove_document(document_id):
// document ids are member-local (see the note on IPostings::document_id), so
// an id alone does not identify which member to delete from. Callers remove
// through the owning member's IMutableInvertedIndex
// (FederationMember::mutable_index->remove_document(id)); a removed document
// then disappears from perform_federated_search automatically, since each
// member's perform_search filters its own tombstones.
class FederatedIndex {
public:
  void add(std::shared_ptr<IInvertedIndex> index,
           std::shared_ptr<IMutableInvertedIndex> mutable_index = nullptr);
  void remove(const std::shared_ptr<IInvertedIndex> &index);

  std::vector<FederationMember> snapshot() const;

private:
  mutable std::mutex mutex_;
  std::vector<FederationMember> members_;
};

// Evaluates `expr` independently against every member of `federation`
// (each member resolves its own And/Or/Near/Not cursors internally, see
// perform_search) and concatenates the results, tagged with the
// originating member. No cross-member score normalization or sorting is
// performed; callers that need a combined ranking must do so themselves.
std::vector<FederatedHit> perform_federated_search(const FederatedIndex &federation,
                                                   const Expression &expr);

//-----------------------------------------------------------------------------
// Indexers
//-----------------------------------------------------------------------------

template <typename T> class IIndexer {
public:
  virtual ~IIndexer(){};

  // document_id is chosen by the caller and is only meaningful within the
  // IInvertedIndex this indexer writes to; see the note on
  // IPostings::document_id.
  virtual void index_document(size_t document_id, Tokenizer<T> tokenizer) = 0;
};

//-----------------------------------------------------------------------------
// Text Ranges
//-----------------------------------------------------------------------------

struct TextRange {
  size_t position;
  size_t length;
};

//-----------------------------------------------------------------------------
// Tokenizers
//-----------------------------------------------------------------------------

class UTF8PlainTextTokenizer {
public:
  explicit UTF8PlainTextTokenizer(std::string_view text);

  void operator()(Normalizer normalizer,
                  std::function<void(const std::u32string &str, size_t term_pos,
                                     TextRange text_range)>
                      callback);

private:
  std::string_view text_;
};

// Wraps a raw-splitting Tokenizer<T> (e.g. UTF8PlainTextTokenizer) with a
// TermFilter chain and re-exposes the result under the existing Tokenizer<T>
// contract, so IIndexer/InMemoryIndexer/InMemoryInvertedIndex need no changes
// -- callers just swap UTF8PlainTextTokenizer for
// Analyzer<T>{UTF8PlainTextTokenizer(text), chain}.
//
// Position policy (v1): only tokens that survive the chain get 0,1,2,...
// term positions; dropped tokens do NOT consume a position (gaps are
// closed). This keeps the term_pos == text_range array-index invariant that
// InMemoryIndexer and the text-range machinery (including the format_type=2
// on-disk layout) depend on -- see design section 1.1. The known trade-off
// is that a phrase spanning removed stop-words can false-match (e.g. "apple
// of the tree" indexes as "apple tree"); gap-preservation is a future step.
//
// If a filter emits more than once for one token (1->N), operator() throws:
// silently accepting it would break the same invariant and make text_range
// read out of bounds. Index-side synonym expansion is intentionally
// unsupported; do it query-side (design section 5).
//
// The text_range from base_tokenizer is carried through to the emitted
// output unchanged (offsets are into the original text, so filtering the
// string does not move them). The normalizer passed to operator() is applied
// as "stage 0" (before the chain) by being forwarded to base_tokenizer,
// matching InMemoryIndexer, which always hands its normalizer to the
// tokenizer -- ignoring it would silently drop a normalizer-equipped
// indexer's normalization.
template <typename T> class Analyzer {
public:
  Analyzer(Tokenizer<T> base_tokenizer, TermFilter filter)
      : base_tokenizer_(std::move(base_tokenizer)),
        filter_(std::move(filter)) {}

  void operator()(Normalizer normalizer,
                  std::function<void(const std::u32string &, size_t, T)>
                      callback) {
    size_t term_pos = 0;
    base_tokenizer_(normalizer, [&](const std::u32string &str, size_t,
                                    T text_range) {
      size_t emit_count = 0;
      auto emit = [&](std::u32string out) {
        if (++emit_count > 1) {
          throw std::runtime_error(
              "searchlib: Analyzer does not support 1->N (synonym) expansion "
              "on the index side; expand synonyms query-side instead");
        }
        callback(out, term_pos, text_range);
      };
      if (filter_) {
        filter_(str, emit);
      } else {
        emit(str);
      }
      // Only advance the position when at least one token survived, so drops
      // close the gap (term_pos stays == text_range array index).
      if (emit_count > 0) {
        term_pos++;
      }
    });
  }

private:
  Tokenizer<T> base_tokenizer_;
  TermFilter filter_;
};

//-----------------------------------------------------------------------------

TextRange text_range(const TextRangeList<TextRange> &text_range_list,
                     const IPostings &positions, size_t index,
                     size_t search_hit_index);

// Selects the on-disk representation for InMemoryInvertedIndex<T>::save.
// The formats are peer alternatives, not versions: Plain favors encode/
// decode simplicity, Compressed (Elias-Fano postings) favors file size.
// Either way load() restores the same in-memory structure and auto-detects
// the format from the file header, so no format argument is needed there.
enum class IndexFormat : uint32_t {
  Plain = 0,
  Compressed = 2, // 1 is reserved for a future mmap-oriented format
};

namespace detail {

// Structural Elias-Fano compression of the text-range section, available
// when T is the built-in TextRange (whose token positions are monotone per
// document). Implemented in invertedindex.cpp so that the succinct
// machinery stays out of this public header; other T fall back to the
// generic fixed-width section even in the Compressed format.
void save_text_ranges_compressed(std::ostream &os,
                                 const TextRangeList<TextRange> &list);
void load_text_ranges_compressed(std::istream &is,
                                 TextRangeList<TextRange> &list);

} // namespace detail

// Read-only backend for a Compressed-format (format_type=2) index file that
// keeps the Elias-Fano postings and text-range structures compressed in
// memory, instead of expanding them into the InMemoryInvertedIndex
// representation the way InMemoryInvertedIndex<TextRange>::load does. Use it
// when memory footprint matters more than write access (e.g. an installed
// corpus searched alongside a mutable notes index via FederatedIndex, with
// FederationMember::mutable_index left null).
//
// Only files written by InMemoryInvertedIndex<TextRange>::save with
// IndexFormat::Compressed are accepted; any other format_type, schema
// version, or text-range value type fails with an exception. The returned
// index is immutable, so it is safe to share across reader threads without
// external locking. The implementation lives in compressedindex.cpp so that
// the succinct machinery stays out of this public header.
std::shared_ptr<IInvertedIndexWithTextRange<TextRange>>
load_compressed_index(std::istream &is);
std::shared_ptr<IInvertedIndexWithTextRange<TextRange>>
load_compressed_index(const std::string &path);

class InMemoryInvertedIndexBase : public IInvertedIndex {
public:
  size_t document_count() const override;

  size_t document_term_count(size_t document_id) const override;
  double average_document_term_count() const override;

  bool term_exists(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str,
                    size_t document_id) const override;

  size_t df(const std::u32string &str) const override;
  double tf(const std::u32string &str, size_t document_id) const override;

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override;

  bool has_removed_documents() const override;
  bool is_document_removed(size_t document_id) const override;

  // Logical deletion: mark a document_id as removed. The postings and term
  // statistics are left intact (no physical compaction); searches exclude
  // removed document_ids at their output. Re-indexing the same document_id
  // via InMemoryIndexer clears the tombstone.
  void remove_document(size_t document_id);

  // Serialize/deserialize the T-independent part of the index (documents_
  // and term_dictionary_, including postings). The templated
  // InMemoryInvertedIndex<T> wraps this with the file header and the
  // T-dependent text-range section. The format decides the per-term
  // postings encoding; the section layout is shared.
  void save(std::ostream &os, IndexFormat format = IndexFormat::Plain) const;
  void load(std::istream &is, IndexFormat format = IndexFormat::Plain);

  class Postings : public IPostings {
  public:
    size_t size() const override;

    size_t document_id(size_t index) const override;
    size_t search_hit_count(size_t index) const override;

    size_t term_position(size_t index, size_t search_hit_index) const override;
    size_t term_length(size_t index, size_t search_hit_index) const override;
    bool is_term_position(size_t index, size_t term_pos) const override;

    void add_term_position(size_t document_id, size_t term_pos);

    void save(std::ostream &os) const;
    void load(std::istream &is);

    // Elias-Fano encoding of the same data: document_ids and the end
    // offsets into a concatenated position array are stored as two
    // monotone Elias-Fano sequences, the positions as fixed-width words
    // (docs/postings_compression_design.ja.md section 3). Only used for
    // postings long enough that the succinct-structure overhead pays off.
    void save_compressed(std::ostream &os) const;
    void load_compressed(std::istream &is);

  private:
    // Kept sorted by document_id ascending so that document_id(index) is
    // O(1) and lookups by document_id can binary-search.
    using Entry =
        std::pair<size_t /*document_id*/, std::vector<size_t /*position*/>>;
    std::vector<Entry> positions_;
  };

  struct Document {
    size_t term_count;
  };

  struct Term {
    std::u32string str;
    size_t term_count;
    Postings postings;
  };

  std::unordered_map<size_t /*document_id*/, Document> documents_;
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
  std::unordered_set<size_t /*document_id*/> removed_document_ids_;
};

template <typename T>
class InMemoryInvertedIndex : public IInvertedIndexWithTextRange<T>,
                             public IMutableInvertedIndex {
public:
  size_t document_count() const override { return base_.document_count(); }

  size_t document_term_count(size_t document_id) const override {
    return base_.document_term_count(document_id);
  }

  double average_document_term_count() const override {
    return base_.average_document_term_count();
  }

  bool term_exists(const std::u32string &str) const override {
    return base_.term_exists(str);
  }

  size_t term_count(const std::u32string &str) const override {
    return base_.term_count(str);
  }

  size_t term_count(const std::u32string &str,
                    size_t document_id) const override {
    return base_.term_count(str, document_id);
  }

  size_t df(const std::u32string &str) const override { return base_.df(str); }

  double tf(const std::u32string &str, size_t document_id) const override {
    return base_.tf(str, document_id);
  }

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override {
    return base_.postings(str);
  }

  bool has_removed_documents() const override {
    return base_.has_removed_documents();
  }

  bool is_document_removed(size_t document_id) const override {
    return base_.is_document_removed(document_id);
  }

  void remove_document(size_t document_id) override {
    base_.remove_document(document_id);
  }

  T text_range(const IPostings &positions, size_t index,
               size_t search_hit_index) const override {
    return searchlib::text_range(text_range_list_, positions, index,
                                 search_hit_index);
  }

  // Callbacks for serializing the text-range value type T. When T is
  // trivially copyable (e.g. the built-in TextRange), these may be left
  // empty and a raw byte copy is used automatically; otherwise the caller
  // must supply both.
  using TextRangeSerializer = std::function<void(std::ostream &, const T &)>;
  using TextRangeDeserializer = std::function<T(std::istream &)>;

  void save(std::ostream &os, const TextRangeSerializer &serialize_value = {},
            IndexFormat format = IndexFormat::Plain) const {
    os.write(detail::kIndexMagic, sizeof(detail::kIndexMagic));
    detail::write_scalar<uint32_t>(os, static_cast<uint32_t>(format));
    detail::write_scalar<uint32_t>(os, format == IndexFormat::Compressed
                                           ? detail::kSchemaVersionCompressed
                                           : detail::kSchemaVersionPlain);

    base_.save(os, format);

    if (format == IndexFormat::Compressed) {
      // A marker distinguishes the structurally compressed section (only
      // available for the built-in TextRange) from the generic fallback,
      // so a load with a mismatched T fails loudly instead of misparsing.
      if constexpr (std::is_same_v<T, TextRange>) {
        detail::write_scalar<uint32_t>(os, 1);
        detail::save_text_ranges_compressed(os, text_range_list_);
      } else {
        detail::write_scalar<uint32_t>(os, 0);
        save_text_ranges_(os, serialize_value);
      }
    } else {
      save_text_ranges_(os, serialize_value);
    }
  }

  void load(std::istream &is,
            const TextRangeDeserializer &deserialize_value = {}) {
    char magic[sizeof(detail::kIndexMagic)];
    is.read(magic, sizeof(magic));
    if (!is || std::memcmp(magic, detail::kIndexMagic, sizeof(magic)) != 0) {
      throw std::runtime_error("searchlib: not a valid index file (bad magic)");
    }
    auto format_type = detail::read_scalar<uint32_t>(is);
    IndexFormat format;
    uint32_t expected_schema_version;
    if (format_type == detail::kFormatTypePlain) {
      format = IndexFormat::Plain;
      expected_schema_version = detail::kSchemaVersionPlain;
    } else if (format_type == detail::kFormatTypeCompressed) {
      format = IndexFormat::Compressed;
      expected_schema_version = detail::kSchemaVersionCompressed;
    } else {
      throw std::runtime_error("searchlib: unsupported index format_type");
    }
    auto schema_version = detail::read_scalar<uint32_t>(is);
    if (schema_version != expected_schema_version) {
      throw std::runtime_error("searchlib: unsupported index schema_version");
    }

    base_.load(is, format);

    text_range_list_.clear();
    if (format == IndexFormat::Compressed) {
      auto structural = detail::read_scalar<uint32_t>(is);
      if (structural) {
        if constexpr (std::is_same_v<T, TextRange>) {
          detail::load_text_ranges_compressed(is, text_range_list_);
        } else {
          throw std::runtime_error(
              "searchlib: index text-range section requires TextRange");
        }
      } else {
        load_text_ranges_(is, deserialize_value);
      }
    } else {
      load_text_ranges_(is, deserialize_value);
    }
  }

  void save(const std::string &path,
            const TextRangeSerializer &serialize_value = {},
            IndexFormat format = IndexFormat::Plain) const {
    std::ofstream os(path, std::ios::binary);
    if (!os) {
      throw std::runtime_error(
          "searchlib: cannot open index file for writing: " + path);
    }
    save(os, serialize_value, format);
  }

  void load(const std::string &path,
            const TextRangeDeserializer &deserialize_value = {}) {
    std::ifstream is(path, std::ios::binary);
    if (!is) {
      throw std::runtime_error(
          "searchlib: cannot open index file for reading: " + path);
    }
    load(is, deserialize_value);
  }

private:
  template <typename> friend class InMemoryIndexer;

  // Generic text-range section: per-document value lists written with
  // save_value_, ordered by document_id for deterministic output.
  void save_text_ranges_(std::ostream &os,
                         const TextRangeSerializer &serialize_value) const {
    detail::write_scalar<uint64_t>(os, text_range_list_.size());
    std::vector<size_t> document_ids;
    document_ids.reserve(text_range_list_.size());
    for (const auto &[document_id, _] : text_range_list_) {
      document_ids.push_back(document_id);
    }
    std::sort(document_ids.begin(), document_ids.end());
    for (auto document_id : document_ids) {
      const auto &values = text_range_list_.at(document_id);
      detail::write_scalar<uint64_t>(os, document_id);
      detail::write_scalar<uint64_t>(os, values.size());
      for (const auto &value : values) {
        save_value_(os, value, serialize_value);
      }
    }
  }

  void load_text_ranges_(std::istream &is,
                         const TextRangeDeserializer &deserialize_value) {
    auto document_count = detail::read_scalar<uint64_t>(is);
    for (uint64_t i = 0; i < document_count; i++) {
      auto document_id = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      auto value_count = detail::read_scalar<uint64_t>(is);
      std::vector<T> values;
      values.reserve(static_cast<size_t>(value_count));
      for (uint64_t j = 0; j < value_count; j++) {
        values.push_back(load_value_(is, deserialize_value));
      }
      text_range_list_[document_id] = std::move(values);
    }
  }

  static void save_value_(std::ostream &os, const T &value,
                          const TextRangeSerializer &serialize_value) {
    if (serialize_value) {
      serialize_value(os, value);
      return;
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
      os.write(reinterpret_cast<const char *>(&value), sizeof(T));
    } else {
      throw std::runtime_error("searchlib: a text-range serializer is "
                               "required for this value type");
    }
  }

  static T load_value_(std::istream &is,
                       const TextRangeDeserializer &deserialize_value) {
    if (deserialize_value) {
      return deserialize_value(is);
    }
    if constexpr (std::is_trivially_copyable_v<T>) {
      T value{};
      is.read(reinterpret_cast<char *>(&value), sizeof(T));
      if (!is) {
        throw std::runtime_error("searchlib: unexpected end of index stream");
      }
      return value;
    } else {
      throw std::runtime_error("searchlib: a text-range deserializer is "
                               "required for this value type");
    }
  }

  InMemoryInvertedIndexBase base_;
  TextRangeList<T> text_range_list_;
};

template <typename T> class InMemoryIndexer : public IIndexer<T> {
public:
  InMemoryIndexer(InMemoryInvertedIndex<T> &index, Normalizer normalizer)
      : index_(index), normalizer_(normalizer) {}

  void index_document(size_t document_id, Tokenizer<T> tokenizer) override {
    // (Re)indexing a document clears any prior logical-deletion tombstone,
    // so that the "update = remove + re-index (same id)" pattern works.
    index_.base_.removed_document_ids_.erase(document_id);

    size_t term_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (index_.base_.term_dictionary_.find(str) ==
          index_.base_.term_dictionary_.end()) {
        index_.base_.term_dictionary_[str] = {str, 0};
      }

      auto &term = index_.base_.term_dictionary_.at(str);
      term.term_count++;
      term.postings.add_term_position(document_id, term_pos);

      index_.text_range_list_[document_id].push_back(std::move(text_range));

      term_count++;
    });

    index_.base_.documents_[document_id] = {term_count};
  }

private:
  InMemoryInvertedIndex<T> &index_;
  Normalizer normalizer_;
};

// A minimal thread-safe wrapper around an InMemoryInvertedIndex<T> for the
// typical embedded pattern: a single background writer indexing/removing
// documents while UI/query threads search concurrently.
//
// Locking is coarse-grained and per-operation, not per-method. This matters
// because search results reference the index: perform_search returns IPostings
// (a bare-term result aliases the term dictionary), and bm25_score / text_range
// read the index during result consumption. A concurrent writer that rehashes
// an unordered_map or mutates a postings vector would then invalidate those
// references. So a whole read operation -- running the query AND consuming its
// results -- must stay inside one shared-lock scope, and a whole write
// operation inside one unique-lock scope.
//
// The read/write callbacks enforce this: run everything that touches the index
// inside the callback and return only materialized values. Do NOT let a
// reference, an IPostings, or an InMemoryIndexer escape the callback -- using
// it afterwards runs outside the lock and is undefined behavior.
//
//   ThreadSafeInvertedIndex<TextRange> index;
//
//   index.write([&](auto &idx) {
//     InMemoryIndexer indexer(idx, normalizer);
//     indexer.index_document(0, UTF8PlainTextTokenizer(text));
//   });
//
//   auto hits = index.read([&](const auto &idx) {
//     auto postings = perform_search(idx, *expr);
//     std::vector<size_t> document_ids;
//     for (size_t i = 0; i < postings->size(); i++) {
//       document_ids.push_back(postings->document_id(i));
//     }
//     return document_ids;  // materialized: safe to use after the lock
//   });
template <typename T> class ThreadSafeInvertedIndex {
public:
  // Run a read-only operation under a shared lock. fn receives
  // `const InMemoryInvertedIndex<T> &`. Multiple readers may proceed
  // concurrently; a writer blocks until they finish.
  template <typename Fn> auto read(Fn &&fn) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return fn(index_);
  }

  // Run a mutating operation under an exclusive lock. fn receives
  // `InMemoryInvertedIndex<T> &`. Use for index_document (via InMemoryIndexer),
  // remove_document, and load. Blocks all readers and other writers.
  template <typename Fn> auto write(Fn &&fn) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return fn(index_);
  }

private:
  InMemoryInvertedIndex<T> index_;
  mutable std::shared_mutex mutex_;
};

} // namespace searchlib
