//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <algorithm>
#include <cassert>
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
// 1 = Elias-Fano compressed postings; see
// docs/postings_compression_design.ja.md), while schema_version tracks
// layout evolution within one format_type, so each format versions
// independently. Both schema versions restarted from 0 when documents
// became ordinal-addressed (see the note on IPostings::document_ordinal):
// nothing had shipped on the earlier numbering, so there is no file to
// migrate.
inline constexpr char kIndexMagic[4] = {'S', 'I', 'D', 'X'};
inline constexpr uint32_t kFormatTypePlain = 0;
inline constexpr uint32_t kFormatTypeCompressed = 1;
inline constexpr uint32_t kSchemaVersionPlain = 0;
inline constexpr uint32_t kSchemaVersionCompressed = 0;

// Opaque storage for InMemoryInvertedIndexBase's scope data (a map of
// Elias-Fano encoded position->scope-ordinal sequences, one per
// (scope_name, ordinal)). Defined in invertedindex.cpp, which includes
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

  // A document is addressed two ways. Its *key* is the caller's own
  // identifier, handed to IIndexer::index_document and stable for the life
  // of the document. Its *ordinal* is assigned by the index, densely and in
  // indexing order, and is what postings, text ranges and tombstones are
  // keyed by -- so a postings list is ascending in it no matter what order
  // the caller chose keys in, and an index is free to renumber (a future
  // merge would). Postings only ever speak ordinals; translate back through
  // the owning index's document_key(). An ordinal is scoped to the
  // IInvertedIndex that produced this IPostings, so code fanning out across
  // indexes must tag results with their originating index.
  virtual size_t document_ordinal(size_t index) const = 0;
  virtual size_t search_hit_count(size_t index) const = 0;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_length(size_t index, size_t search_hit_index) const = 0;
  virtual bool is_term_position(size_t index, size_t term_pos) const = 0;
};

class IInvertedIndex {
public:
  virtual ~IInvertedIndex() = 0;

  virtual size_t document_count() const = 0;

  // Every document argument in this interface is an ordinal (see the note
  // on IPostings::document_ordinal), local to this IInvertedIndex instance.
  virtual size_t document_term_count(size_t ordinal) const = 0;
  virtual double average_document_term_count() const = 0;

  virtual bool term_exists(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str) const = 0;
  virtual size_t term_count(const std::u32string &str,
                            size_t ordinal) const = 0;

  virtual size_t df(const std::u32string &str) const = 0;
  virtual double tf(const std::u32string &str, size_t ordinal) const = 0;

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

  // Calls callback once for every term in the dictionary matching `pattern`,
  // a glob where `*` matches zero or more codepoints and every other
  // codepoint must match literally (no `?` or character classes). This is
  // what backs Operation::Wildcard; a pattern with no `*` at all matches only
  // the identical term, and one that is entirely `*` matches the whole
  // dictionary.
  //
  // Same order/cost/buffer-reuse contract as enumerate_terms_with_prefix: the
  // enumeration order is unspecified, and the string handed to the callback
  // is only valid for the duration of that call.
  virtual void enumerate_terms_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback) const = 0;

  // Calls callback once for every term within `max_edits` Levenshtein edits
  // (insertion, deletion, substitution, each counted at codepoint granularity)
  // of `target`. This is what backs Operation::Fuzzy; max_edits of 0 matches
  // only `target` itself.
  //
  // No cap is applied here -- the query syntax caps `term~N`, but a caller
  // naming a distance directly is trusted to know that the cost grows sharply
  // with it, since a large enough distance matches most of the dictionary.
  //
  // Same order/cost/buffer-reuse contract as enumerate_terms_with_prefix: the
  // enumeration order is unspecified, and the string handed to the callback
  // is only valid for the duration of that call.
  virtual void enumerate_terms_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback) const = 0;

  // Logical (tombstone) deletion support. Read-only indexes report no
  // removals; searches filter out removed ordinals via these hooks.
  // Overridden by indexes that support IMutableInvertedIndex::remove_document.
  virtual bool has_removed_documents() const { return false; }
  virtual bool is_document_removed(size_t ordinal) const { return false; }
};

//-----------------------------------------------------------------------------
// Text Ranges
//-----------------------------------------------------------------------------

// Byte offsets into the original UTF-8 text a term came from. Declared this
// early (rather than next to the tokenizers that produce it) because
// TextSplitter below is spelled in terms of it.
struct TextRange {
  size_t position;
  size_t length;
};

using Normalizer = std::function<std::u32string(const std::u32string &str)>;

template <typename T>
using TextRangeList =
    std::unordered_map<size_t /*ordinal*/, std::vector<T>>;

template <typename T>
using Tokenizer =
    std::function<void(Normalizer normalizer,
                       std::function<void(const std::u32string &str,
                                          size_t term_pos, T text_range)>
                           callback)>;

//-----------------------------------------------------------------------------
// Text splitting (shared by the index side and the query side)
//-----------------------------------------------------------------------------

// Splits raw text into the term strings an index should contain, together with
// the byte range each came from. It is deliberately *not* a Tokenizer<T>: it
// carries no term positions and no Normalizer, because the query side has no
// use for either -- which is what lets one splitter be shared by both sides,
// and is the whole point of the type. Wrap one in SplitterTokenizer to index
// with it, or pass it to parse_query to parse with it.
//
// A splitter must emit strictly non-overlapping ranges in increasing order,
// each spanning the bytes its term was cut from: the text-range machinery
// indexes the emitted ranges by term position, so a splitter that reorders or
// overlaps them corrupts highlighting. The emitted string need not equal those
// bytes -- SplitterTokenizer applies the Normalizer on top of it, and a
// splitter may normalize on its own -- but the range must still point at them.
using TextSplitter = std::function<void(
    std::string_view text,
    const std::function<void(const std::u32string &str, TextRange range)>
        &emit)>;

// The default splitter, and the one every existing entry point already uses
// implicitly: maximal runs of Unicode letters (`unicode::is_letter`) become
// terms and everything else separates them. It cannot segment CJK, where a
// whole space-free sentence is one letter run and therefore one enormous term;
// see searchlib_segment.h for a splitter that can.
TextSplitter utf8_plain_text_splitter();

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

// The key <-> ordinal bridge (see the note on IPostings::document_ordinal),
// on every index that can answer it. Kept off IInvertedIndex so that the
// search and scoring layer never has to know what a key looks like: it
// speaks ordinals only, and callers translate at the edges. Key is whatever
// the caller names documents by; size_t and std::string are supported out
// of the box (see KeyTable), any other type needs a key serializer.
template <typename Key> class IKeyedIndex {
public:
  virtual ~IKeyedIndex(){};

  // Only defined for an ordinal this index handed out.
  virtual const Key &document_key(size_t ordinal) const = 0;

  // The ordinal a key names -- a removed document's tombstoned ordinal
  // included, on which is_document_removed is true -- or nullopt for a key
  // never indexed.
  virtual std::optional<size_t>
  document_ordinal(const Key &document_key) const = 0;
};

template <typename T, typename Key = size_t>
class IInvertedIndexWithTextRange : public IInvertedIndex,
                                    public IKeyedIndex<Key>,
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
// involved. Maps a (scope_name, document ordinal, term_pos) triple to the id
// of the structural unit (e.g. paragraph number) that term_pos falls in, so
// that Operation::SameScope can test whether hits from different sub-queries
// co-occur within the same unit. scope_name is an opaque caller-chosen
// identifier (e.g. "section", "paragraph"); this interface does not
// interpret it.
class IScopeIndex {
public:
  virtual ~IScopeIndex() = 0;

  // `ordinal` is a document ordinal (see IPostings::document_ordinal).
  virtual bool has_scope(const std::string &scope_name,
                         size_t ordinal) const = 0;

  // The scope id at term_pos. term_pos must be < document_term_count(
  // ordinal); behavior is unspecified otherwise. Only meaningful when
  // has_scope(scope_name, ordinal) is true.
  virtual size_t scope_id(const std::string &scope_name, size_t ordinal,
                          size_t term_pos) const = 0;
};

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

enum class Operation {
  Term,
  And,
  Adjacent,
  Or,
  Near,
  Not,
  SameScope,
  Prefix,
  Wildcard,
  Fuzzy
};

struct Expression {
  Operation operation;

  // For Operation::Term the term to look up; for Operation::Prefix the prefix
  // every matching term must start with; for Operation::Wildcard the glob
  // pattern every matching term must satisfy (see
  // IInvertedIndex::enumerate_terms_with_wildcard); for Operation::Fuzzy the
  // term every match must be within near_operation_distance edits of.
  std::u32string term_str;

  // The proximity window for Operation::Near, and -- reusing the same field
  // rather than adding one only two operations would ever read -- the maximum
  // edit distance for Operation::Fuzzy. Unused by every other operation.
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

// Same again, but with the raw-token splitting made explicit instead of
// hardwired to utf8_plain_text_splitter(). Pass the splitter the index was
// built with (see SplitterTokenizer) so that both sides agree on term
// boundaries -- without it, a CJK-segmented index can be searched for 東京 but
// not for 東京タワー, because the query side would leave the latter as one
// term. A null splitter selects utf8_plain_text_splitter(), making the two
// overloads above exactly this one with the default.
//
// The splitter runs where the raw tokenizer used to, so a query token it
// splits into several terms becomes the implicit `Adjacent` phrase (東京タワー
// -> Adjacent(東京, タワー)), while a TermFilter that expands one term into
// several still becomes an `Or`. It is not applied inside a wildcard pattern,
// where `*`-delimited pieces are pattern fragments rather than terms.
std::optional<Expression> parse_query(TextSplitter splitter, TermFilter filter,
                                      std::string_view query);

// scope_index is consulted only for Operation::SameScope nodes; if null,
// such nodes contribute no matches (the same "no match" treatment as an
// empty And/Or operand), rather than throwing.
//
// Lifetime: the result is a view over invidx, not a snapshot of it. A
// bare-term result has always been the term's own postings, and an And/Or
// result reads its operands' positions on demand rather than copying them
// out, so every result stays valid only while invidx is unchanged. Consume
// it -- including bm25_score and text_range, which read invidx anyway --
// before indexing or removing anything. Under ThreadSafeInvertedIndex this
// is already the documented rule: run the query and consume its results
// inside one read() scope.
//
// One result per thread, for the same reason BM25Scorer wants one instance
// per query per thread: an And/Or result answers positions from cursors and
// a memoized row it keeps inside itself, so its const methods write to it.
// Two threads reading one result race even though neither touches the index.
// Concurrent queries each build their own result, so this costs nothing.
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

// Same rationale and mechanics as expand_prefixes, but for Operation::
// Wildcard nodes and IInvertedIndex::enumerate_terms_with_wildcard. The two
// expansions are independent: a tree with both Prefix and Wildcard nodes
// needs both calls to fully expand.
Expression expand_wildcards(const IInvertedIndex &invidx,
                            const Expression &expr);

// Same rationale and mechanics again, for Operation::Fuzzy nodes and
// IInvertedIndex::enumerate_terms_with_edit_distance. Note that every term the
// expansion finds contributes equally to the score: unlike Lucene, a closer
// edit distance is not boosted.
Expression expand_fuzzy(const IInvertedIndex &invidx, const Expression &expr);

size_t term_count_score(const IInvertedIndex &invidx, const Expression &expr,
                        const IPostings &postings, size_t index);

double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                    const IPostings &postings, size_t index);

double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                  const IPostings &postings, size_t index, double k1 = 1.2,
                  double b = 0.75);

// Same BM25 score as bm25_score, but with the per-term state -- the postings
// list and the idf -- resolved once at construction rather than once per
// scored hit. bm25_score reaches both through the term dictionary on every
// call, so ranking H hits of a T-term query hashes a variable-length
// u32string 2*T*H times and recomputes the same T logarithms H times.
//
//   BM25Scorer scorer(invidx, expr);
//   auto hits = top_k(*result, 10,
//                     [&](size_t i) { return scorer(*result, i); });
//
// A Prefix/Wildcard/Fuzzy node is expanded against the dictionary here too,
// once, so scoring one no longer re-enumerates the dictionary for every hit
// and callers need not run expand_prefixes() up front just to keep ranking
// affordable (perform_search expands internally either way).
//
// Lifetime: the scorer borrows invidx and expr, and holds postings that do
// NOT keep the index alive -- IInvertedIndex::postings hands back an
// aliasing shared_ptr with no deleter. Keep it within the index's lifetime,
// and do not use one across a mutation of the index: under
// ThreadSafeInvertedIndex that means constructing, using and destroying it
// inside a single read() callback. One instance per query per thread.
class BM25Scorer {
public:
  BM25Scorer(const IInvertedIndex &invidx, const Expression &expr,
             double k1 = 1.2, double b = 0.75);

  double operator()(const IPostings &postings, size_t index) const;

private:
  struct TermState {
    std::shared_ptr<const IPostings> postings;
    double idf;
    // postings->size(), resolved once here: the scorer's contract already
    // pins the postings for its lifetime, and re-fetching the size through
    // the virtual interface would otherwise happen once per term per hit.
    size_t size;
    // Where the last lookup landed in this term's postings, and what it was
    // looking for. Callers walk a result in ascending index order (that is
    // what top_k does) and results are ordered by document id, so the next
    // lookup almost always resumes at or just after this one instead of
    // searching the list again.
    //
    // The invariant `cursor` is the first entry whose document id is >=
    // `last_ordinal` is what lets a forward lookup answer "this document
    // does not carry the term" without searching at all -- the common case
    // for an Or, where most hits match only some of its terms. Scoring out
    // of order stays correct, just without the shortcut.
    mutable size_t cursor;
    mutable size_t last_ordinal;
  };

  const IInvertedIndex &invidx_;
  std::vector<TermState> terms_;
  double avgdl_;
  double k1_;
  double b_;
};

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
template <typename Key = size_t> class IMutableInvertedIndex {
public:
  virtual ~IMutableInvertedIndex(){};

  // document_key is the caller's key, as given to IIndexer::index_document.
  // A key that names no document is a no-op.
  virtual void remove_document(const Key &document_key) = 0;
};

// One entry in a FederatedIndex. `keyed` is how a hit gets its key back
// (every index in this library is one; a hand-wired member must supply it).
// mutable_index is null for read-only members (e.g. an installed book) and
// non-null for members that support removal (e.g. a notes index), decided
// by the caller at registration time.
template <typename Key = size_t> struct FederationMember {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IKeyedIndex<Key>> keyed;
  std::shared_ptr<IMutableInvertedIndex<Key>> mutable_index;
};

// One hit produced by perform_federated_search: the member it came from,
// its postings entry, and the document's key, resolved through the member
// so that callers never reach across ordinal spaces themselves (an ordinal
// is only meaningful together with `index`, see the note on
// IPostings::document_ordinal).
template <typename Key = size_t> struct FederatedHit {
  std::shared_ptr<IInvertedIndex> index;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
  Key document_key;
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
// There is deliberately no federation-level remove_document: a key alone
// does not say which member holds it. Callers remove through the owning
// member's IMutableInvertedIndex
// (FederationMember::mutable_index->remove_document(key)); a removed
// document then disappears from perform_federated_search automatically,
// since each member's perform_search filters its own tombstones.
template <typename Key = size_t> class FederatedIndex {
public:
  // Registers an index that is its own keyed view and, if it is one, its
  // own mutable view too -- which every InMemoryInvertedIndex and every
  // load_compressed_index result is. Decided at compile time from the
  // pointee's bases; no RTTI. To register a mutable index read-only, use the
  // three-argument form below and withhold the mutable view.
  template <typename Index> void add(std::shared_ptr<Index> index) {
    std::shared_ptr<IKeyedIndex<Key>> keyed = index;
    std::shared_ptr<IMutableInvertedIndex<Key>> mutable_index;
    if constexpr (std::is_base_of_v<IMutableInvertedIndex<Key>, Index>) {
      mutable_index = index;
    }
    add(std::shared_ptr<IInvertedIndex>(index), std::move(keyed),
        std::move(mutable_index));
  }

  // Hand-wired registration, for a member whose views live in different
  // objects. `keyed` is required, since every hit carries a key.
  void add(std::shared_ptr<IInvertedIndex> index,
           std::shared_ptr<IKeyedIndex<Key>> keyed,
           std::shared_ptr<IMutableInvertedIndex<Key>> mutable_index) {
    if (!keyed) {
      throw std::invalid_argument(
          "searchlib: a FederatedIndex member needs its keyed view");
    }
    std::lock_guard<std::mutex> lock(mutex_);
    members_.push_back(
        {std::move(index), std::move(keyed), std::move(mutable_index)});
  }

  void remove(const std::shared_ptr<IInvertedIndex> &index) {
    std::lock_guard<std::mutex> lock(mutex_);
    members_.erase(std::remove_if(members_.begin(), members_.end(),
                                  [&](const auto &member) {
                                    return member.index == index;
                                  }),
                   members_.end());
  }

  std::vector<FederationMember<Key>> snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return members_;
  }

private:
  mutable std::mutex mutex_;
  std::vector<FederationMember<Key>> members_;
};

// Evaluates `expr` independently against every member of `federation`
// (each member resolves its own And/Or/Near/Not cursors internally, see
// perform_search) and concatenates the results, tagged with the
// originating member and the document's key. No cross-member score
// normalization or sorting is performed; callers that need a combined
// ranking must do so themselves.
template <typename Key>
std::vector<FederatedHit<Key>>
perform_federated_search(const FederatedIndex<Key> &federation,
                         const Expression &expr) {
  std::vector<FederatedHit<Key>> hits;
  for (const auto &member : federation.snapshot()) {
    auto postings = perform_search(*member.index, expr);
    for (size_t i = 0; i < postings->size(); i++) {
      hits.push_back(
          {member.index, postings, i,
           member.keyed->document_key(postings->document_ordinal(i))});
    }
  }
  return hits;
}

//-----------------------------------------------------------------------------
// Indexers
//-----------------------------------------------------------------------------

template <typename T, typename Key = size_t> class IIndexer {
public:
  virtual ~IIndexer(){};

  // document_key is chosen by the caller and names the document from then
  // on (see the note on IPostings::document_ordinal). Indexing a key that is
  // already present replaces that document: the old one is tombstoned and
  // the new text gets a fresh ordinal, so nothing of the old text survives.
  virtual void index_document(const Key &document_key,
                              Tokenizer<T> tokenizer) = 0;
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

// Lifts a TextSplitter into a Tokenizer<TextRange>, which is all an index
// needs: the splitter decides the term boundaries, this adds the 0,1,2,...
// term positions on top and applies the Normalizer the Tokenizer<T> contract
// hands it (as stage 0, exactly as UTF8PlainTextTokenizer does -- ignoring it
// would silently drop a normalizer-equipped InMemoryIndexer's normalization).
//
// Indexing with the same splitter a query is parsed with is what keeps the two
// sides' term boundaries identical:
//
//   auto splitter = load_segmenting_splitter("ja-ud-gsd.mod");
//   indexer.index_document(0, SplitterTokenizer(splitter, text));
//   auto expr = parse_query(splitter, nullptr, "東京タワー");
//
// A null splitter behaves as utf8_plain_text_splitter(), matching parse_query.
class SplitterTokenizer {
public:
  SplitterTokenizer(TextSplitter splitter, std::string_view text);

  void operator()(Normalizer normalizer,
                  std::function<void(const std::u32string &str, size_t term_pos,
                                     TextRange text_range)>
                      callback);

private:
  TextSplitter splitter_;
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
  Plain = detail::kFormatTypePlain,
  Compressed = detail::kFormatTypeCompressed,
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
// Key must be the type the index was saved with; size_t and std::string are
// the instantiations compressedindex.cpp provides.
template <typename Key = size_t>
std::shared_ptr<IInvertedIndexWithTextRange<TextRange, Key>>
load_compressed_index(std::istream &is);
template <typename Key = size_t>
std::shared_ptr<IInvertedIndexWithTextRange<TextRange, Key>>
load_compressed_index(const std::string &path);

// The ordinal <-> key table an index keeps beside its ordinal-only core.
// ordinal -> key is a vector, key -> ordinal a hash map; a re-indexed key
// points at its newest ordinal, a removed key still at its tombstoned one.
//
// On disk it is one section, keys in ordinal order. size_t keys that happen
// to be 0..n-1 in that order are the identity -- the common case for a
// caller that just counts -- and collapse to a single flag; any other
// size_t key is a u64, a std::string key is length-prefixed bytes, and
// another type goes through the serializer callbacks.
template <typename Key> class KeyTable {
public:
  using Serializer = std::function<void(std::ostream &, const Key &)>;
  using Deserializer = std::function<Key(std::istream &)>;

  size_t size() const { return keys_.size(); }

  const Key &key(size_t ordinal) const { return keys_.at(ordinal); }

  std::optional<size_t> ordinal(const Key &key) const {
    auto it = ordinals_.find(key);
    if (it == ordinals_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  // Binds `key` to `ordinal`, which must be the next one: the table grows in
  // step with the core's documents.
  void push(const Key &key, size_t ordinal) {
    assert(ordinal == keys_.size());
    keys_.push_back(key);
    ordinals_[key] = ordinal;
  }

  void clear() {
    keys_.clear();
    ordinals_.clear();
  }

  void save(std::ostream &os, const Serializer &serialize_key = {}) const {
    detail::write_scalar<uint64_t>(os, keys_.size());
    if constexpr (std::is_integral_v<Key>) {
      bool identity = true;
      for (size_t i = 0; i < keys_.size() && identity; i++) {
        identity = keys_[i] == static_cast<Key>(i);
      }
      detail::write_scalar<uint32_t>(os, identity ? 1 : 0);
      if (identity) {
        return;
      }
    }
    for (const auto &key : keys_) {
      save_key_(os, key, serialize_key);
    }
  }

  void load(std::istream &is, const Deserializer &deserialize_key = {}) {
    clear();
    auto count = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    keys_.reserve(count);
    if constexpr (std::is_integral_v<Key>) {
      if (detail::read_scalar<uint32_t>(is)) {
        for (size_t i = 0; i < count; i++) {
          push(static_cast<Key>(i), i);
        }
        return;
      }
    }
    for (size_t i = 0; i < count; i++) {
      push(load_key_(is, deserialize_key), i);
    }
  }

private:
  static void save_key_(std::ostream &os, const Key &key,
                        const Serializer &serialize_key) {
    if (serialize_key) {
      serialize_key(os, key);
    } else if constexpr (std::is_integral_v<Key>) {
      detail::write_scalar<uint64_t>(os, static_cast<uint64_t>(key));
    } else if constexpr (std::is_same_v<Key, std::string>) {
      detail::write_bytes(os, key);
    } else {
      throw std::runtime_error(
          "searchlib: a key serializer is required for this key type");
    }
  }

  static Key load_key_(std::istream &is, const Deserializer &deserialize_key) {
    if (deserialize_key) {
      return deserialize_key(is);
    } else if constexpr (std::is_integral_v<Key>) {
      return static_cast<Key>(detail::read_scalar<uint64_t>(is));
    } else if constexpr (std::is_same_v<Key, std::string>) {
      return detail::read_bytes(is);
    } else {
      throw std::runtime_error(
          "searchlib: a key deserializer is required for this key type");
    }
  }

  std::vector<Key> keys_;
  std::unordered_map<Key, size_t> ordinals_;
};

class InMemoryInvertedIndexBase : public IInvertedIndex, public IScopeIndex {
public:
  // Live documents only: a removed one no longer counts here, nor in the
  // average below.
  size_t document_count() const override;

  size_t document_term_count(size_t ordinal) const override;
  double average_document_term_count() const override;

  bool term_exists(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str) const override;
  size_t term_count(const std::u32string &str,
                    size_t ordinal) const override;

  size_t df(const std::u32string &str) const override;
  double tf(const std::u32string &str, size_t ordinal) const override;

  std::shared_ptr<const IPostings>
  postings(const std::u32string &str) const override;

  void enumerate_terms_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback)
      const override;

  void enumerate_terms_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback)
      const override;

  void enumerate_terms_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback)
      const override;

  bool has_removed_documents() const override;
  bool is_document_removed(size_t ordinal) const override;

  // Logical deletion of one ordinal, idempotent. The postings and term
  // statistics are left intact (no physical compaction); searches exclude
  // removed ordinals at their output, and document_count /
  // average_document_term_count count live documents only. Keys live above
  // this class (InMemoryInvertedIndex owns the KeyTable), which is why the
  // core speaks ordinals here as everywhere.
  void tombstone(size_t ordinal);

  // Search-scope side data (see IScopeIndex). scope_ids.size() must equal
  // the document's term count and its values must be monotonically
  // non-decreasing (positions in the same structural unit share a value;
  // later units get strictly larger values). Overwrites any previous
  // registration for the same (scope_name, ordinal). Throws
  // std::invalid_argument on a size mismatch or a non-monotone sequence.
  void set_scope_ids(const std::string &scope_name, size_t ordinal,
                     const std::vector<size_t> &scope_ids);

  bool has_scope(const std::string &scope_name,
                size_t ordinal) const override;
  size_t scope_id(const std::string &scope_name, size_t ordinal,
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

    size_t document_ordinal(size_t index) const override;
    size_t search_hit_count(size_t index) const override;

    size_t term_position(size_t index, size_t search_hit_index) const override;
    size_t term_length(size_t index, size_t search_hit_index) const override;
    bool is_term_position(size_t index, size_t term_pos) const override;

    // Appends term_pos for `ordinal`, which must be >= the last ordinal
    // appended: ordinals are handed out in indexing order and never
    // revisited (re-indexing gets a fresh one), so a postings list is
    // append-only.
    void add_term_position(size_t ordinal, size_t term_pos);

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
    // Ordinals ascending (by construction, see add_term_position), so that
    // document_ordinal(index) is O(1) and lookups by ordinal can
    // binary-search; each document's term positions are the slice
    // [offsets_[i], offsets_[i + 1]) of one concatenated array.
    //
    // The three arrays exist instead of a vector of
    // (ordinal, vector<position>) pairs because that shape put a 24-byte
    // vector header beside every 8-byte ordinal. Walking or galloping over
    // the ordinals then strode 32 bytes and pulled in three quarters of a
    // cache line it never read, which is the dominant cost of an And over a
    // high-df term.
    std::vector<size_t> document_ordinals_;
    std::vector<size_t> offsets_{0}; // document_ordinals_.size() + 1 entries
    std::vector<size_t> positions_;  // every position, concatenated
  };

  struct Document {
    size_t term_count;
  };

  struct Term {
    std::u32string str;
    size_t term_count;
    Postings postings;
  };

  // Hands out the next ordinal: documents are numbered densely in indexing
  // order and never renumbered while the index is live, so ordinals only
  // ever grow. The new document's term count is 0 until
  // set_document_term_count.
  size_t push_document();

  // Records a freshly allocated document's term count. Always go through
  // this rather than assigning into documents_ directly, so that the running
  // total behind average_document_term_count() stays correct.
  void set_document_term_count(size_t ordinal, size_t term_count);

  std::vector<Document> documents_; // indexed by ordinal
  std::unordered_map<std::u32string /*str*/, Term> term_dictionary_;
  std::unordered_set<size_t /*ordinal*/> removed_ordinals_;
  std::shared_ptr<detail::ScopeIndexData> scope_data_;

  // Sum of every live document's term_count, maintained on write so that
  // average_document_term_count() is O(1). It used to walk documents_ on
  // every call, which bm25_score makes once per scored hit -- that turned
  // scoring into O(hits * documents) and dominated every ranked query.
  // Updated eagerly rather than cached lazily because the accessor is const
  // and runs under ThreadSafeInvertedIndex's shared_lock, where a mutable
  // cache would be a data race.
  size_t total_document_term_count_ = 0;
};

template <typename T, typename Key = size_t>
class InMemoryInvertedIndex : public IInvertedIndexWithTextRange<T, Key>,
                             public IMutableInvertedIndex<Key>,
                             public IScopeIndex {
public:
  size_t document_count() const override { return base_.document_count(); }

  size_t document_term_count(size_t ordinal) const override {
    return base_.document_term_count(ordinal);
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
                    size_t ordinal) const override {
    return base_.term_count(str, ordinal);
  }

  size_t df(const std::u32string &str) const override { return base_.df(str); }

  double tf(const std::u32string &str, size_t ordinal) const override {
    return base_.tf(str, ordinal);
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

  void enumerate_terms_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    base_.enumerate_terms_with_wildcard(pattern, callback);
  }

  void enumerate_terms_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback)
      const override {
    base_.enumerate_terms_with_edit_distance(target, max_edits, callback);
  }

  bool has_removed_documents() const override {
    return base_.has_removed_documents();
  }

  bool is_document_removed(size_t ordinal) const override {
    return base_.is_document_removed(ordinal);
  }

  // The key <-> ordinal bridge (see the note on IPostings::document_ordinal).
  // document_ordinal answers for a removed document too (its tombstoned
  // ordinal, on which is_document_removed is true), and nullopt for a key
  // never indexed.
  const Key &document_key(size_t ordinal) const override {
    return keys_.key(ordinal);
  }

  std::optional<size_t>
  document_ordinal(const Key &document_key) const override {
    return keys_.ordinal(document_key);
  }

  // Logical deletion: tombstone the document `document_key` names (a key
  // that names none is a no-op). Re-indexing the key via InMemoryIndexer
  // makes it searchable again, under a fresh ordinal.
  void remove_document(const Key &document_key) override {
    if (auto ordinal = keys_.ordinal(document_key)) {
      base_.tombstone(*ordinal);
    }
  }

  // Search-scope side data (see IScopeIndex), registered by key since that
  // is what the caller has; the contract is InMemoryInvertedIndexBase::
  // set_scope_ids's, plus std::invalid_argument for a key that names no
  // document.
  void set_scope_ids(const std::string &scope_name, const Key &document_key,
                     const std::vector<size_t> &scope_ids) {
    auto ordinal = keys_.ordinal(document_key);
    if (!ordinal) {
      throw std::invalid_argument(
          "searchlib: set_scope_ids: the key names no document");
    }
    base_.set_scope_ids(scope_name, *ordinal, scope_ids);
  }

  bool has_scope(const std::string &scope_name,
                size_t ordinal) const override {
    return base_.has_scope(scope_name, ordinal);
  }

  size_t scope_id(const std::string &scope_name, size_t ordinal,
                  size_t term_pos) const override {
    return base_.scope_id(scope_name, ordinal, term_pos);
  }

  T text_range(const IPostings &positions, size_t index,
               size_t search_hit_index) const override {
    if (text_range_list_.empty()) {
      // Distinguishable from "this one document has none", which the lookup
      // below reports as an out-of-range key: an index with no ranges at all
      // was built (or loaded from an index built) with
      // TextRangeStorage::Skip, and no amount of retrying will help.
      throw std::runtime_error(
          "searchlib: this index holds no text ranges, so text_range() "
          "cannot answer (see TextRangeStorage::Skip)");
    }
    return searchlib::text_range(text_range_list_, positions, index,
                                 search_hit_index);
  }

  // Callbacks for serializing the text-range value type T. When T is
  // trivially copyable (e.g. the built-in TextRange), these may be left
  // empty and a raw byte copy is used automatically; otherwise the caller
  // must supply both.
  using TextRangeSerializer = std::function<void(std::ostream &, const T &)>;
  using TextRangeDeserializer = std::function<T(std::istream &)>;

  // Likewise for the key type: size_t and std::string need none (see
  // KeyTable), anything else needs both.
  using KeySerializer = typename KeyTable<Key>::Serializer;
  using KeyDeserializer = typename KeyTable<Key>::Deserializer;

  void save(std::ostream &os, const TextRangeSerializer &serialize_value = {},
            IndexFormat format = IndexFormat::Plain,
            const KeySerializer &serialize_key = {}) const {
    os.write(detail::kIndexMagic, sizeof(detail::kIndexMagic));
    detail::write_scalar<uint32_t>(os, static_cast<uint32_t>(format));
    detail::write_scalar<uint32_t>(os, format == IndexFormat::Compressed
                                           ? detail::kSchemaVersionCompressed
                                           : detail::kSchemaVersionPlain);

    base_.save(os, format);
    keys_.save(os, serialize_key);

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
            const TextRangeDeserializer &deserialize_value = {},
            const KeyDeserializer &deserialize_key = {}) {
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
    keys_.load(is, deserialize_key);
    if (keys_.size() != base_.documents_.size()) {
      throw std::runtime_error(
          "searchlib: the key section disagrees with the documents section");
    }

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
            IndexFormat format = IndexFormat::Plain,
            const KeySerializer &serialize_key = {}) const {
    std::ofstream os(path, std::ios::binary);
    if (!os) {
      throw std::runtime_error(
          "searchlib: cannot open index file for writing: " + path);
    }
    save(os, serialize_value, format, serialize_key);
  }

  void load(const std::string &path,
            const TextRangeDeserializer &deserialize_value = {},
            const KeyDeserializer &deserialize_key = {}) {
    std::ifstream is(path, std::ios::binary);
    if (!is) {
      throw std::runtime_error(
          "searchlib: cannot open index file for reading: " + path);
    }
    load(is, deserialize_value, deserialize_key);
  }

private:
  template <typename, typename> friend class InMemoryIndexer;

  // Generic text-range section: per-document value lists written with
  // save_value_, ordered by ordinal for deterministic output. The ordinal is
  // written out because the map is sparse: a document with no positions
  // never gets an entry.
  void save_text_ranges_(std::ostream &os,
                         const TextRangeSerializer &serialize_value) const {
    detail::write_scalar<uint64_t>(os, text_range_list_.size());
    std::vector<size_t> ordinals;
    ordinals.reserve(text_range_list_.size());
    for (const auto &[ordinal, _] : text_range_list_) {
      ordinals.push_back(ordinal);
    }
    std::sort(ordinals.begin(), ordinals.end());
    for (auto ordinal : ordinals) {
      const auto &values = text_range_list_.at(ordinal);
      detail::write_scalar<uint64_t>(os, ordinal);
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
      auto ordinal = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
      auto value_count = detail::read_scalar<uint64_t>(is);
      std::vector<T> values;
      values.reserve(static_cast<size_t>(value_count));
      for (uint64_t j = 0; j < value_count; j++) {
        values.push_back(load_value_(is, deserialize_value));
      }
      text_range_list_[ordinal] = std::move(values);
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
  KeyTable<Key> keys_;
  TextRangeList<T> text_range_list_;
};

// Whether an indexer keeps the byte ranges its tokenizer reports alongside
// each term position.
//
// Store is the default, and is what highlighting needs: text_range() answers
// out of them. Skip drops them as they arrive, which is worth having because
// they are not a small part of an index. Measured on the KJV corpus
// concatenated ten times (311,030 documents, 7,914,460 tokens), building it
// both ways:
//
//   build footprint   54.0 -> 35.1 bytes per token   (-35%)
//   Plain file        285.5 -> 160.0 MB              (-44%)
//   Compressed file    34.0 ->  25.6 MB              (-25%)
//
// So a corpus big enough that building it is a memory problem, with a
// workload that never highlights, gets about a third of its build footprint
// back.
//
// An index built with Skip answers every query, count and score exactly as
// one built with Store; only text_range() stops working, and it says so
// rather than answering wrongly. The choice is not recorded in the index
// file because it does not need to be: a saved index simply has no
// text-range section to restore, in either format.
enum class TextRangeStorage {
  Store,
  Skip,
};

template <typename T, typename Key = size_t>
class InMemoryIndexer : public IIndexer<T, Key> {
public:
  InMemoryIndexer(InMemoryInvertedIndex<T, Key> &index, Normalizer normalizer,
                  TextRangeStorage text_ranges = TextRangeStorage::Store)
      : index_(index), normalizer_(normalizer), text_ranges_(text_ranges) {}

  void index_document(const Key &document_key,
                      Tokenizer<T> tokenizer) override {
    // A fresh ordinal every time: a key that already names a document has
    // that one tombstoned first, which is what makes "update = re-index the
    // same key" leave nothing of the old text behind, and what keeps every
    // postings list append-only.
    if (auto previous = index_.keys_.ordinal(document_key)) {
      index_.base_.tombstone(*previous);
    }
    auto ordinal = index_.base_.push_document();
    index_.keys_.push(document_key, ordinal);

    size_t term_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (index_.base_.term_dictionary_.find(str) ==
          index_.base_.term_dictionary_.end()) {
        index_.base_.term_dictionary_[str] = {str, 0};
      }

      auto &term = index_.base_.term_dictionary_.at(str);
      term.term_count++;
      term.postings.add_term_position(ordinal, term_pos);

      if (text_ranges_ == TextRangeStorage::Store) {
        index_.text_range_list_[ordinal].push_back(std::move(text_range));
      }

      term_count++;
    });

    index_.base_.set_document_term_count(ordinal, term_count);
  }

private:
  InMemoryInvertedIndex<T, Key> &index_;
  Normalizer normalizer_;
  TextRangeStorage text_ranges_;
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
//     std::vector<size_t> keys;
//     for (size_t i = 0; i < postings->size(); i++) {
//       keys.push_back(idx.document_key(postings->document_ordinal(i)));
//     }
//     return keys;  // materialized: safe to use after the lock
//   });
template <typename T, typename Key = size_t> class ThreadSafeInvertedIndex {
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
  InMemoryInvertedIndex<T, Key> index_;
  mutable std::shared_mutex mutex_;
};

//-----------------------------------------------------------------------------
// Multi-Field Schema
//-----------------------------------------------------------------------------

// Groups several independently-built InMemoryInvertedIndex<T> instances
// under named fields (e.g. "title", "body", "tags"), the minimal answer to
// docs/missing_features.ja.md 2.2. The fields share one key space: the
// caller indexes the same logical document under the same key into whichever
// fields it has content for. Each field is its own index with its own
// ordinals, though, and a document present in some fields but not others
// lands on different ordinals in each -- so hits are grouped by key, which
// perform_multi_field_search resolves per hit, never by ordinal.
//
// Field-qualified search ("only search the title field") needs no new API:
// call perform_search(*index.field("title"), expr) directly, exactly as for
// any InMemoryInvertedIndex. There is deliberately no query-string `field:`
// syntax (same precedent as Operation::SameScope / roadmap item 10) --
// field selection is a C++-level choice made by the caller, not the parser.
template <typename T, typename Key = size_t> class MultiFieldIndex {
public:
  // Returns the named field's index, creating an empty one on first use.
  InMemoryInvertedIndex<T, Key> &field(const std::string &name) {
    return fields_[name];
  }

  // Returns nullptr if the field has never been touched via the non-const
  // field(name) overload.
  const InMemoryInvertedIndex<T, Key> *field(const std::string &name) const {
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
           const typename InMemoryInvertedIndex<T, Key>::TextRangeSerializer
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
           const typename InMemoryInvertedIndex<T, Key>::TextRangeDeserializer
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
      InMemoryInvertedIndex<T, Key> index;
      index.load(is, deserialize_value);
      fields_[std::move(name)] = std::move(index);
    }
  }

private:
  std::map<std::string, InMemoryInvertedIndex<T, Key>> fields_;
};

// One hit produced by perform_multi_field_search, tagged with the field it
// came from and with the document's key -- the one thing shared across
// fields (see the MultiFieldIndex note above), so callers group hits by
// document_key to combine per-field matches/scores for the same document.
template <typename T, typename Key = size_t> struct MultiFieldHit {
  std::string field;
  std::shared_ptr<IPostings> postings;
  size_t index_in_postings;
  Key document_key;
};

// Runs expr independently against every field of index (each field resolves
// its own And/Or/Near/Not cursors via perform_search) and concatenates the
// results tagged by field name. No cross-field score normalization,
// combination, or sorting is performed; callers that need a single combined
// ranking do so themselves (e.g. summing or maxing bm25_score across the
// fields where a document_key appears) -- the same division of
// responsibility as perform_federated_search.
template <typename T, typename Key>
std::vector<MultiFieldHit<T, Key>>
perform_multi_field_search(const MultiFieldIndex<T, Key> &index,
                          const Expression &expr) {
  std::vector<MultiFieldHit<T, Key>> hits;
  for (const auto &name : index.field_names()) {
    const auto *field_index = index.field(name);
    auto postings = perform_search(*field_index, expr);
    for (size_t i = 0; i < postings->size(); i++) {
      hits.push_back({name, postings, i,
                      field_index->document_key(postings->document_ordinal(i))});
    }
  }
  return hits;
}

} // namespace searchlib
