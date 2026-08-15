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
inline constexpr uint32_t kSchemaVersionPlain = 3;
inline constexpr uint32_t kSchemaVersionCompressed = 3;

// Opaque storage for InMemoryInvertedIndexBase's scope data (a map of
// Elias-Fano encoded position->scope-ordinal sequences, one per
// (scope_name, document_id)). Defined in invertedindex.cpp, which includes
// succinct.h; kept incomplete here so that succinct machinery stays out of
// this public header. shared_ptr<incomplete T> is safe as a class member
// (unlike unique_ptr<incomplete T>) since it never needs T's destructor to
// be visible at the point of use.
class ScopeIndexData;

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

// The same length-prefixed shape for an opaque byte blob.
inline void write_bytes(std::ostream &os, std::string_view bytes) {
  write_scalar<uint64_t>(os, bytes.size());
  os.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

inline std::string read_bytes(std::istream &is) {
  auto n = static_cast<size_t>(read_scalar<uint64_t>(is));
  std::string bytes(n, '\0');
  if (n > 0) {
    is.read(bytes.data(), static_cast<std::streamsize>(n));
    if (!is) {
      throw std::runtime_error("searchlib: unexpected end of index stream");
    }
  }
  return bytes;
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

  // Calls callback once for every term in the dictionary that starts with
  // `prefix`; an empty prefix enumerates the whole dictionary. This is what
  // backs Operation::Prefix.
  //
  // The enumeration order and the cost are both unspecified: an unordered
  // dictionary answers this with a full scan, an ordered one descends
  // straight to the matching subtree. Callers needing a deterministic order
  // must sort the collected terms themselves.
  //
  // The string handed to the callback is only valid for the duration of that
  // call; an implementation may hand out a buffer it reuses for the next
  // term. Copy it to keep it.
  virtual void enumerate_terms_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback) const = 0;

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
// Search Scopes (document/section/paragraph containment)
//-----------------------------------------------------------------------------

// Optional per-index capability, analogous to IMutableInvertedIndex: kept
// separate from IInvertedIndex (rather than discovered via
// dynamic_pointer_cast) so callers wire it in explicitly, with no RTTI
// involved. Maps a (scope_name, document_id, term_pos) triple to the ordinal
// of the structural unit (e.g. paragraph number) that term_pos falls in, so
// that Operation::SameScope can test whether hits from different sub-queries
// co-occur within the same unit. scope_name is an opaque caller-chosen
// identifier (e.g. "section", "paragraph"); this interface does not
// interpret it.
class IScopeIndex {
public:
  virtual ~IScopeIndex() = 0;

  virtual bool has_scope(const std::string &scope_name,
                         size_t document_id) const = 0;

  // The scope ordinal at term_pos. term_pos must be < document_term_count(
  // document_id); behavior is unspecified otherwise. Only meaningful when
  // has_scope(scope_name, document_id) is true.
  virtual size_t scope_id(const std::string &scope_name, size_t document_id,
                          size_t term_pos) const = 0;
};

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation { Term, And, Adjacent, Or, Near, Not, SameScope, Prefix };

struct Expression {
  Operation operation;

  // For Operation::Term the term to look up; for Operation::Prefix the prefix
  // every matching term must start with.
  std::u32string term_str;
  size_t near_operation_distance;
  std::vector<Expression> nodes;

  // Only meaningful for Operation::SameScope: the scope_name passed to
  // IScopeIndex. nodes holds the two or more sub-expressions whose hits must
  // share the same scope ordinal to survive.
  std::string scope_name;
};

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query);

// Same grammar as the Normalizer overload, but reuses the same TermFilter
// chain used to build an index-side Analyzer<T>, so query-time term
// splitting/filtering stays symmetric with index-time (see design section
// 6). Unlike the Analyzer<T> index path, this overload does not throw on
// 1->N: a filter that expands one raw token into several (e.g. synonyms)
// maps to an `Or` of those terms, while a token the raw tokenizer itself
// split into several pieces (e.g. `well-known`) still maps to the implicit
// `Adjacent` phrase, exactly as the Normalizer overload does.
std::optional<Expression> parse_query(TermFilter filter,
                                      std::string_view query);

// scope_index is consulted only for Operation::SameScope nodes; if null,
// such nodes contribute no matches (the same "no match" treatment as an
// empty And/Or operand), rather than throwing.
std::shared_ptr<IPostings> perform_search(const IInvertedIndex &invidx,
                                          const Expression &expr,
                                          const IScopeIndex *scope_index = nullptr);

// Rewrites every Operation::Prefix node into the Or over the terms it expands
// to against invidx, leaving the rest of the tree alone. perform_search does
// this internally, so it is only worth calling explicitly before scoring:
// the scoring functions below take an Expression per hit, and a Prefix node
// costs one dictionary enumeration every time they are called. Expanding once
// and scoring the result is one enumeration per query instead of one per hit.
Expression expand_prefixes(const IInvertedIndex &invidx,
                           const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

//-----------------------------------------------------------------------------
// Ranking support: bounded top-k collection
//-----------------------------------------------------------------------------

// One collected hit: `index` is the position in whatever result collection
// score_fn was scoring (e.g. an index into IPostings, or into a
// FederatedHit vector), not a document id.
struct ScoredHit {
  size_t index;
  double score;
};

// Collects the k highest-scoring hits out of [0, count) using a bounded
// min-heap, i.e. O(count * log k) instead of the naive "score everything,
// then sort everything" O(count * log count). score_fn(i) computes the
// score for element i; it is called exactly once per i. Returns hits
// sorted by descending score, ties broken by ascending index.
template <typename ScoreFn>
std::vector<ScoredHit> top_k(size_t count, size_t k, ScoreFn score_fn) {
  std::vector<ScoredHit> heap;
  if (k == 0 || count == 0) {
    return heap;
  }
  heap.reserve(std::min(count, k));

  // Heap top is the smallest score currently kept, so it's the first
  // candidate to evict when a better-scoring hit shows up.
  auto min_heap_order = [](const ScoredHit &a, const ScoredHit &b) {
    return a.score > b.score;
  };

  for (size_t i = 0; i < count; i++) {
    double score = static_cast<double>(score_fn(i));
    if (heap.size() < k) {
      heap.push_back(ScoredHit{i, score});
      std::push_heap(heap.begin(), heap.end(), min_heap_order);
    } else if (score > heap.front().score) {
      std::pop_heap(heap.begin(), heap.end(), min_heap_order);
      heap.back() = ScoredHit{i, score};
      std::push_heap(heap.begin(), heap.end(), min_heap_order);
    }
  }

  std::sort(heap.begin(), heap.end(), [](const ScoredHit &a, const ScoredHit &b) {
    if (a.score != b.score) {
      return a.score > b.score;
    }
    return a.index < b.index;
  });
  return heap;
}

// Convenience overload for the common case of ranking a single IPostings
// result set, e.g.:
//   auto hits = top_k(*result, 10, [&](size_t i) {
//     return bm25_score(invidx, *expr, *result, i);
//   });
template <typename ScoreFn>
std::vector<ScoredHit> top_k(const IPostings &postings, size_t k,
                             ScoreFn score_fn) {
  return top_k(postings.size(), k, std::move(score_fn));
}

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

class InMemoryInvertedIndexBase : public IInvertedIndex, public IScopeIndex {
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

  void enumerate_terms_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback)
      const override;

  bool has_removed_documents() const override;
  bool is_document_removed(size_t document_id) const override;

  // Logical deletion: mark a document_id as removed. The postings and term
  // statistics are left intact (no physical compaction); searches exclude
  // removed document_ids at their output. Re-indexing the same document_id
  // via InMemoryIndexer clears the tombstone.
  void remove_document(size_t document_id);

  // Search-scope side data (see IScopeIndex). scope_ids.size() must equal
  // document_term_count(document_id) and its values must be monotonically
  // non-decreasing (positions in the same structural unit share a value;
  // later units get strictly larger values). Overwrites any previous
  // registration for the same (scope_name, document_id). Throws
  // std::invalid_argument on a size mismatch or a non-monotone sequence.
  void set_scope_ids(const std::string &scope_name, size_t document_id,
                     const std::vector<size_t> &scope_ids);

  bool has_scope(const std::string &scope_name,
                size_t document_id) const override;
  size_t scope_id(const std::string &scope_name, size_t document_id,
                  size_t term_pos) const override;

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

  // Registers (or replaces) a document's term count. Always go through this
  // rather than assigning into documents_ directly, so that the running total
  // behind average_document_term_count() stays correct.
  void set_document_term_count(size_t document_id, size_t term_count);

  std::unordered_map<size_t /*document_id*/, Document> documents_;
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
  std::unordered_set<size_t /*document_id*/> removed_document_ids_;
  std::shared_ptr<detail::ScopeIndexData> scope_data_;

  // Sum of every documents_ entry's term_count, maintained on write so that
  // average_document_term_count() is O(1). It used to walk documents_ on
  // every call, which bm25_score makes once per scored hit -- that turned
  // scoring into O(hits * documents) and dominated every ranked query.
  // Updated eagerly rather than cached lazily because the accessor is const
  // and runs under ThreadSafeInvertedIndex's shared_lock, where a mutable
  // cache would be a data race.
  size_t total_document_term_count_ = 0;
};

template <typename T>
class InMemoryInvertedIndex : public IInvertedIndexWithTextRange<T>,
                             public IMutableInvertedIndex,
                             public IScopeIndex {
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

  void enumerate_terms_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    base_.enumerate_terms_with_prefix(prefix, callback);
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

  void set_scope_ids(const std::string &scope_name, size_t document_id,
                     const std::vector<size_t> &scope_ids) {
    base_.set_scope_ids(scope_name, document_id, scope_ids);
  }

  bool has_scope(const std::string &scope_name,
                size_t document_id) const override {
    return base_.has_scope(scope_name, document_id);
  }

  size_t scope_id(const std::string &scope_name, size_t document_id,
                  size_t term_pos) const override {
    return base_.scope_id(scope_name, document_id, term_pos);
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

    index_.base_.set_document_term_count(document_id, term_count);
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

//-----------------------------------------------------------------------------
// Multi-Field Schema
//-----------------------------------------------------------------------------

// Groups several independently-built InMemoryInvertedIndex<T> instances
// under named fields (e.g. "title", "body", "tags"), the minimal answer to
// docs/missing_features.ja.md 2.2. Unlike FederatedIndex (whose members have
// member-local document_id spaces, see the note on IPostings::document_id),
// a MultiFieldIndex's fields share one document_id space: the caller indexes
// the same logical document under the same document_id into whichever
// fields it has content for, so grouping hits back into "this document
// matched in title and body" is a plain document_id comparison, no
// per-field id remapping needed.
//
// Field-qualified search ("only search the title field") needs no new API:
// call perform_search(*index.field("title"), expr) directly, exactly as for
// any InMemoryInvertedIndex. There is deliberately no query-string `field:`
// syntax (same precedent as Operation::SameScope / roadmap item 10) --
// field selection is a C++-level choice made by the caller, not the parser.
template <typename T> class MultiFieldIndex {
public:
  // Returns the named field's index, creating an empty one on first use.
  InMemoryInvertedIndex<T> &field(const std::string &name) {
    return fields_[name];
  }

  // Returns nullptr if the field has never been touched via the non-const
  // field(name) overload.
  const InMemoryInvertedIndex<T> *field(const std::string &name) const {
    auto it = fields_.find(name);
    if (it == fields_.end()) {
      return nullptr;
    }
    return &it->second;
  }

  bool has_field(const std::string &name) const {
    return fields_.find(name) != fields_.end();
  }

  // Sorted by name (fields_ is a std::map), so iteration order is
  // deterministic across runs and matches the on-disk field order.
  std::vector<std::string> field_names() const {
    std::vector<std::string> names;
    names.reserve(fields_.size());
    for (const auto &[name, index] : fields_) {
      names.push_back(name);
    }
    return names;
  }

  void save(std::ostream &os,
           const typename InMemoryInvertedIndex<T>::TextRangeSerializer
               &serialize_value = {},
           IndexFormat format = IndexFormat::Plain) const {
    detail::write_scalar<uint64_t>(os, fields_.size());
    for (const auto &[name, index] : fields_) {
      detail::write_scalar<uint64_t>(os, name.size());
      os.write(name.data(), static_cast<std::streamsize>(name.size()));
      index.save(os, serialize_value, format);
    }
  }

  void load(std::istream &is,
           const typename InMemoryInvertedIndex<T>::TextRangeDeserializer
               &deserialize_value = {}) {
    fields_.clear();
    auto count = detail::read_scalar<uint64_t>(is);
    for (uint64_t i = 0; i < count; i++) {
      auto name_length =
          static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      std::string name(name_length, '\0');
      if (name_length > 0) {
        is.read(name.data(), static_cast<std::streamsize>(name_length));
        if (!is) {
          throw std::runtime_error(
              "searchlib: unexpected end of multi-field index stream");
        }
      }
      InMemoryInvertedIndex<T> index;
      index.load(is, deserialize_value);
      fields_[std::move(name)] = std::move(index);
    }
  }

private:
  std::map<std::string, InMemoryInvertedIndex<T>> fields_;
};

// One hit produced by perform_multi_field_search, tagged with the field it
// came from. document_id (via postings->document_id(index_in_postings)) is
// shared across fields -- see the MultiFieldIndex note above -- so callers
// group hits by that id to combine per-field matches/scores for the same
// document.
template <typename T> struct MultiFieldHit {
  std::string field;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
};

// Runs expr independently against every field of index (each field resolves
// its own And/Or/Near/Not cursors via perform_search) and concatenates the
// results tagged by field name. No cross-field score normalization,
// combination, or sorting is performed; callers that need a single combined
// ranking do so themselves (e.g. summing or maxing bm25_score across the
// fields where a document_id appears) -- the same division of
// responsibility as perform_federated_search.
template <typename T>
std::vector<MultiFieldHit<T>>
perform_multi_field_search(const MultiFieldIndex<T> &index,
                          const Expression &expr) {
  std::vector<MultiFieldHit<T>> hits;
  for (const auto &name : index.field_names()) {
    const auto *field_index = index.field(name);
    auto postings = perform_search(*field_index, expr);
    for (size_t i = 0; i < postings->size(); i++) {
      hits.push_back({name, postings, i});
    }
  }
  return hits;
}

} // namespace searchlib
