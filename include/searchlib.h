//
//  searchlib.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//
//  The whole library. Three parts, in this order:
//
//    1. the interface -- every type and function a caller uses, with the
//       reasoning behind each one, and no implementation in the way;
//    2. namespace detail -- the succinct structures, the FST term dictionary,
//       the splitters, the search evaluators and the query grammar;
//    3. the out-of-line definitions of part 1.
//
//  Japanese segmentation is the one thing kept out, in searchlib_segment.h,
//  since it needs a fourth vendored library and a model file.
//

#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <istream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <fstlib.h>
#include <peglib.h>
#include <unicodelib.h>
#include <unicodelib_encodings.h>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

#if defined(_MSC_VER)
#define SEARCHLIB_ALWAYS_INLINE __forceinline
#else
#define SEARCHLIB_ALWAYS_INLINE inline __attribute__((always_inline))
#endif

namespace searchlib {

//-----------------------------------------------------------------------------
// Encoding
//-----------------------------------------------------------------------------

// The library speaks std::u32string throughout (terms, Normalizer,
// TermFilter, Expression::term_str), while documents and queries arrive as
// UTF-8, so these two cross between them. They are here, and not tucked
// away, because a caller reading an Expression or writing a Normalizer
// needs them as much as the implementation does.

inline std::string u8(std::u32string_view u32) {
  return unicode::utf8::encode(u32);
}

inline std::u32string u32(std::string_view u8) {
  return unicode::utf8::decode(u8);
}

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
// migrate. Compressed is at 1 since its expanded sections -- document term
// counts, per-term frequencies, and the postings too short for Elias-Fano --
// became varint instead of fixed-width fields, and the per-term
// representation flag narrowed from a uint32 to one byte with them.
inline constexpr char kIndexMagic[4] = {'S', 'I', 'D', 'X'};
inline constexpr uint32_t kFormatTypePlain = 0;
inline constexpr uint32_t kFormatTypeCompressed = 1;
inline constexpr uint32_t kSchemaVersionPlain = 0;
inline constexpr uint32_t kSchemaVersionCompressed = 1;

// Opaque storage for InMemoryInvertedIndexBase's scope data (a map of
// Elias-Fano encoded position->scope-ordinal sequences, one per
// (scope_name, ordinal)). Defined further down, with the rest of the
// implementation, so that the interface above reads without the succinct
// machinery in the way. shared_ptr<incomplete T> is safe as a class member
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

// Variable-length integer: seven bits per byte, least significant first, the
// top bit of each byte saying whether another follows. A value below 128 takes
// one byte where a fixed-width field takes eight, which is what the Compressed
// format's expanded sections are made of -- a document's term count, a term's
// frequency, the gap between two ordinals. Fixed-width stays for the header
// tags and for the Plain format, whose whole point is decode simplicity, and
// for the one count that opens each section, where there is nothing to gain.
inline void write_varint(std::ostream &os, uint64_t value) {
  while (value >= 0x80) {
    write_scalar<uint8_t>(os, static_cast<uint8_t>(value) | 0x80);
    value >>= 7;
  }
  write_scalar<uint8_t>(os, static_cast<uint8_t>(value));
}

inline uint64_t read_varint(std::istream &is) {
  uint64_t value = 0;
  for (int shift = 0; shift < 64; shift += 7) {
    // One character straight off the buffer rather than read_scalar's
    // istream::read: a varint is read a byte at a time by construction, and
    // read's per-call sentry and gcount bookkeeping is most of what loading
    // these sections costs.
    auto c = is.rdbuf()->sbumpc();
    if (c == std::char_traits<char>::eof()) {
      is.setstate(std::ios::failbit | std::ios::eofbit);
      throw std::runtime_error("searchlib: unexpected end of index stream");
    }
    auto byte = static_cast<uint8_t>(c);
    // The tenth byte reaches bit 63 and can carry nothing above it. Masking
    // and shifting a wider one would drop the excess and hand back a value
    // that was never written, so treat it as the corrupt file it is.
    if (shift == 63 && (byte & 0x7f) > 1) {
      break;
    }
    value |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return value;
    }
  }
  throw std::runtime_error("searchlib: malformed varint in index stream");
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

  // document_ordinal(index), (index+1), ... written into `out`, at most
  // `count` of them; returns how many (fewer than asked only at the end of
  // the list). The point is one virtual call per block rather than per
  // document: an implementation whose values are cheap to decode in order but
  // expensive to address individually -- Elias-Fano, where addressing one
  // value is a select -- overrides this and pays its setup once. The default
  // is the per-element loop, so an implementation that has nothing to gain
  // (a vector) can ignore it.
  // Whether reading a block is actually cheaper per value than addressing
  // values one at a time. Only an implementation that has to work to reach a
  // single value says yes -- Elias-Fano, where addressing one is a select.
  // A vector says no, and the walks then stay on document_ordinal, which for
  // them is already one load: buffering it would cost more than it saves
  // (measured: a window over vector-backed postings makes an AND 22% slower).
  virtual bool prefers_block_reads() const { return false; }

  virtual size_t read_ordinals(size_t index, size_t *out,
                               size_t count) const {
    auto n = size();
    if (index >= n) {
      return 0;
    }
    count = std::min(count, n - index);
    for (size_t i = 0; i < count; i++) {
      out[i] = document_ordinal(index + i);
    }
    return count;
  }

  // search_hit_count(index), (index+1), ... on the same terms as
  // read_ordinals: the scorer reads a term's hit count for every document it
  // scores, and on Elias-Fano a hit count is two selects.
  virtual size_t read_hit_counts(size_t index, size_t *out,
                                 size_t count) const {
    auto n = size();
    if (index >= n) {
      return 0;
    }
    count = std::min(count, n - index);
    for (size_t i = 0; i < count; i++) {
      out[i] = search_hit_count(index + i);
    }
    return count;
  }

  // term_position(index, 0), (index, 1), ...: every position of one entry,
  // into `out`, which holds search_hit_count(index) of them. Phrase matching
  // reads whole position lists, and on Elias-Fano each position is a select.
  virtual void read_term_positions(size_t index, size_t *out) const {
    auto count = search_hit_count(index);
    for (size_t i = 0; i < count; i++) {
      out[i] = term_position(index, i);
    }
  }

  // First index after `from` whose ordinal is >= `ordinal`, or size() when
  // there is none. Requires document_ordinal(from) < ordinal, which is what
  // every caller has just observed. The default gallops forward, which on a
  // vector is O(log gap) loads; Elias-Fano overrides it with next_geq, one
  // bucket lookup instead of a select per probe. Defined with the other
  // out-of-line bodies, after detail::gallop_lower_bound.
  virtual size_t advance(size_t from, size_t ordinal) const;

  virtual size_t term_position(size_t index, size_t search_hit_index) const = 0;
  virtual size_t term_length(size_t index, size_t search_hit_index) const = 0;
  virtual bool is_term_position(size_t index, size_t term_pos) const = 0;
};

// One operand read in order. Where the operand says a block is cheaper than
// an element (prefers_block_reads: Elias-Fano), ordinals and hit counts come
// in through read_ordinals / read_hit_counts a window at a time; anywhere
// else every read is the operand's own call, so a vector-backed operand
// pays one predictable branch and nothing more. The reads on one window
// only move forward, which is how BM25Scorer walks: a result in ascending
// order, each term's cursor following it.
//
// Same interface as IPostings for what it covers, so find_from_cursor takes
// either.
class PostingsWindow {
public:
  PostingsWindow() = default;
  explicit PostingsWindow(const IPostings *postings)
      : postings_(postings), buffered_(postings->prefers_block_reads()) {}

  size_t document_ordinal(size_t index) {
    if (!buffered_) {
      return postings_->document_ordinal(index);
    }
    return ordinals_[locate_(index)];
  }

  size_t search_hit_count(size_t index) {
    if (!buffered_) {
      return postings_->search_hit_count(index);
    }
    auto i = locate_(index);
    if (!counts_filled_) {
      postings_->read_hit_counts(base_, counts_, filled_);
      counts_filled_ = true;
    }
    return counts_[i];
  }

  // IPostings::advance through the window: a skip of a few documents lands
  // inside the block already read, and one past it refills from where the
  // operand's own advance lands, so the read that follows is a hit.
  size_t advance(size_t from, size_t ordinal) {
    if (!buffered_) {
      return postings_->advance(from, ordinal);
    }
    auto end = base_ + filled_;
    for (auto i = from + 1; i < end; i++) {
      if (ordinals_[i - base_] >= ordinal) {
        return i;
      }
    }
    auto index = postings_->advance(std::max(from, end - (end > 0 ? 1 : 0)),
                                    ordinal);
    filled_ = postings_->read_ordinals(index, ordinals_, kWindow);
    base_ = index;
    counts_filled_ = false;
    return index;
  }

private:
  static constexpr size_t kWindow = 8;

  // The slot of `index` in the window, refilling a block when it is
  // outside: the select is the cost, the values after it come at a decode
  // each, and the scorer's next read is almost always the next index.
  size_t locate_(size_t index) {
    if (index >= base_ && index - base_ < filled_) {
      return index - base_;
    }
    filled_ = postings_->read_ordinals(index, ordinals_, kWindow);
    base_ = index;
    counts_filled_ = false;
    return 0;
  }

  const IPostings *postings_ = nullptr;
  bool buffered_ = false;
  size_t base_ = 0;
  size_t filled_ = 0;
  bool counts_filled_ = false;
  size_t ordinals_[kWindow];
  size_t counts_[kWindow];
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

// One term and the byte range it was cut from, as a splitter hands them out.
using SplitEmit =
    std::function<void(const std::u32string &str, TextRange range)>;

// Splits raw text into the term strings an index should contain, together with
// the byte range each came from. It is deliberately *not* a Tokenizer<T>: it
// carries no term positions and no Normalizer, because the query side has no
// use for either -- which is what lets one splitter be shared by both sides,
// and is the whole point of the type. Wrap one in SplitterTokenizer to index
// with it, or pass it to parse_query to parse with it.
//
// A splitter must emit ranges whose starts never decrease, each spanning the
// bytes its term was cut from. Terms whose ranges start at the same byte are
// alternatives at one term position -- a compound beside its parts, a surface
// form beside its lemma -- which is Lucene's positionIncrement == 0; the first
// one's range is the position's range. A term starting after the previous one
// takes the next position, whether or not the two ranges overlap. The
// text-range machinery indexes one range per term position, so a splitter
// whose starts go back corrupts highlighting. The emitted string need not
// equal the bytes -- SplitterTokenizer applies the Normalizer on top of it,
// and a splitter may normalize on its own -- but the range must still point
// at them.
using TextSplitter =
    std::function<void(std::string_view text, const SplitEmit &emit)>;

// A word segmenter for the scripts UAX #29 leaves to a dictionary. Called with
// the whole text and the offset of a segment whose script it claimed, it emits
// the words it finds from there and returns how many bytes it consumed; the
// default splitter resumes after them. Seeing the whole text is what lets a
// statistical model read as much context as it wants, and it also means the
// same shape serves a splitter that handles everything itself: consume
// `text.size() - offset`.
//
// The caller does not trust the answer. A span whose `consumed` is zero, runs
// past the text or does not end on an extended grapheme cluster boundary is
// dropped whole -- its words too, since a segmenter that gets the span wrong
// gives no reason to believe its words -- and the walk resumes at the next
// grapheme cluster. Within a valid span, a word is dropped on its own when it
// lies outside the span, is empty, starts before the word before it, or is
// not cut on grapheme cluster boundaries; one starting where the word before
// it started stacks on its position (see TextSplitter). Nothing is reported:
// the query side runs this from inside a parser action, and both sides
// degrading identically is what keeps their term boundaries in agreement.
using Segmenter = std::function<size_t(std::string_view text, size_t offset,
                                       const SplitEmit &emit)>;

// The default splitter, and the one every entry point uses when handed none:
// UAX #29 word boundaries (unicode::word_length) over the whole text, keeping
// the segments that contain a letter or a number. So `2.0`, `don't`, `U.S.A`
// and `1,234.56` are each one term, punctuation and spaces are not terms, and
// a run of Katakana is one term while Han, Hiragana, Thai and the other
// scripts written without spaces come out one scalar per term -- which is
// where UAX #29 itself says a dictionary should take over. The second form
// hands every segment whose first scalar `claims` accepts to `segmenter`
// instead (see Segmenter for the contract); the first is the second with
// nothing claimed. searchlib_segment.h builds a Japanese segmenter on the
// second form, and its `claims` is the same predicate that bounds the run it
// hands the model, so "starts a span" and "belongs to a span" cannot drift.
TextSplitter utf8_plain_text_splitter();
TextSplitter utf8_plain_text_splitter(Segmenter segmenter,
                                      std::function<bool(char32_t)> claims);

// Cuts every term `base` emits into the pieces `decompose` returns for it,
// each with its own byte range. This is the shape a word-level morphological
// analyzer plugs in as: for a language written with spaces the default
// splitter has already cut the text into words, and `decompose` turns each
// word into its morphemes. They are a sequence, so they take consecutive
// positions and a query for the whole word becomes an implicit phrase over
// them (see parse_query) -- which is also why this is a splitter and not a
// TermFilter: a filter's several outputs mean alternatives (synonyms), and
// those belong query-side.
//
// `decompose` sees the term's bytes, not the string `base` emitted for it,
// and must answer with pieces that are those bytes in order (a segmentation,
// possibly leaving bytes out between pieces); that is what makes the ranges
// exact, and a piece not found in the term at or after the previous one is
// an error rather than a guessed range. An analyzer that emits forms unlike
// the surface text (a lemma for an inflected verb) should be wrapped as a
// TextSplitter of its own, with the analyzer's offsets. An empty answer
// drops the term. A null `base` means utf8_plain_text_splitter().
using Decomposer =
    std::function<std::vector<std::string>(std::string_view term)>;
TextSplitter subword_splitter(TextSplitter base, Decomposer decompose);

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
// Several outputs from a filter are alternatives (a synonym set): parse_query
// turns them into an Or, and the index-side Analyzer<T> stacks them on one
// term position, as Lucene's SynonymFilter does. Splitting one word into a
// *sequence* of pieces is subword_splitter's job. It is intentionally
// independent of the text-range type T so that the same string logic can be
// reused by parse_query (design section 6).
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

namespace detail {
struct MaxScore;
}

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
  // bm25_top_k's MaxScore walk reads the terms and scores by ordinal.
  friend struct detail::MaxScore;

  // operator() for a document given by ordinal rather than by its place in
  // a result, with the same arithmetic, so the two agree to the bit. Same
  // rule as operator(): ordinals ascending across calls.
  double score_ordinal_(size_t ordinal) const;

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
    // postings->prefers_block_reads(), resolved once like `size` (see
    // find_from_cursor).
    bool block_reads;
  };

  // The BM25 arithmetic over one document, reading the result and the i-th
  // term through whatever operator() hands it: the operands themselves, or
  // their windows.
  template <typename Result, typename TermReader>
  double score_(Result &result, size_t index, TermReader term_reader) const;

  const IInvertedIndex &invidx_;
  std::vector<TermState> terms_;
  // Whether any term reads in blocks. Resolved once, because the windowed
  // body costs a vector-backed term on every hit (measured: +40% on a
  // single-term result) and the walks' rule applies -- pick the path once,
  // outside the loop.
  bool windowed_ = false;
  // The terms' windows (PostingsWindow), parallel to terms_ and built only
  // when windowed_: the inner loop walks terms_ once per hit, and a window's
  // buffers inside TermState put each term on three cache lines instead of
  // one (measured: +13% on a two-term Or's scoring).
  mutable std::vector<PostingsWindow> windows_;
  // The result being scored, read forward as well. A Term query's result is
  // the term's own postings, so on the compressed backend every ordinal it
  // is asked for would otherwise be a select.
  mutable const IPostings *result_ = nullptr;
  mutable PostingsWindow result_window_;
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

namespace detail {

// top_k's bounded min-heap, on its own so that bm25_top_k keeps the same k
// hits top_k would. Which of several equal lowest scores an eviction drops
// depends on the heap's layout, so agreeing on ties means feeding the same
// hits, in the same order, to the same heap.
class TopKCollector {
public:
  TopKCollector(size_t k, size_t count) : k_(k) {
    heap_.reserve(std::min(count, k));
  }

  // Whether k hits are held; only then does threshold() mean anything.
  bool full() const { return heap_.size() == k_; }

  // The lowest score held. Once full(), a hit gets in only by scoring
  // strictly more than this.
  double threshold() const { return heap_.front().score; }

  void offer(size_t index, double score) {
    if (heap_.size() < k_) {
      heap_.push_back(ScoredHit{index, score});
      std::push_heap(heap_.begin(), heap_.end(), min_heap_order);
    } else if (score > heap_.front().score) {
      std::pop_heap(heap_.begin(), heap_.end(), min_heap_order);
      heap_.back() = ScoredHit{index, score};
      std::push_heap(heap_.begin(), heap_.end(), min_heap_order);
    }
  }

  // The hits held, by descending score, ties by ascending index.
  std::vector<ScoredHit> take_sorted() {
    std::sort(heap_.begin(), heap_.end(),
              [](const ScoredHit &a, const ScoredHit &b) {
                if (a.score != b.score) {
                  return a.score > b.score;
                }
                return a.index < b.index;
              });
    return std::move(heap_);
  }

private:
  // Heap top is the smallest score currently kept, so it's the first
  // candidate to evict when a better-scoring hit shows up.
  static bool min_heap_order(const ScoredHit &a, const ScoredHit &b) {
    return a.score > b.score;
  }

  size_t k_;
  std::vector<ScoredHit> heap_;
};

} // namespace detail

// Collects the k highest-scoring hits out of [0, count) using a bounded
// min-heap, i.e. O(count * log k) instead of the naive "score everything,
// then sort everything" O(count * log count). score_fn(i) computes the
// score for element i; it is called exactly once per i. Returns hits
// sorted by descending score, ties broken by ascending index.
template <typename ScoreFn>
std::vector<ScoredHit> top_k(size_t count, size_t k, ScoreFn score_fn) {
  if (k == 0 || count == 0) {
    return {};
  }
  detail::TopKCollector collector(k, count);
  for (size_t i = 0; i < count; i++) {
    collector.offer(i, static_cast<double>(score_fn(i)));
  }
  return collector.take_sorted();
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

// What bm25_top_k returns: the ranked hits, and a result to read them from.
struct RankedResult {
  // Holds every ranked document with the hits and positions perform_search
  // would give it, so it reads exactly like a perform_search result
  // (text_range included). It may hold other matching documents too, so its
  // size() is not the number of matches.
  std::shared_ptr<IPostings> postings;
  // Best first, ties by ascending index; each index is into `postings`.
  std::vector<ScoredHit> hits;
};

// The k best documents for expr by BM25, ranked:
//
//   auto ranked = bm25_top_k(invidx, *expr, 10);
//   for (const auto &hit : ranked.hits) {
//     auto key = invidx.document_key(
//         ranked.postings->document_ordinal(hit.index));
//     // hit.score, invidx.text_range(*ranked.postings, hit.index, 0), ...
//   }
//
// The same documents, scores (to the bit) and order, ties included, as
//
//   auto result = perform_search(invidx, expr);
//   BM25Scorer scorer(invidx, expr, k1, b);
//   top_k(*result, k, [&](size_t i) { return scorer(*result, i); });
//
// but, for a query that is an Or of terms (a bare term, and Prefix,
// Wildcard and Fuzzy, which expand to one, included), without scoring every
// match: it runs MaxScore, skipping every document whose best possible
// score cannot beat the k-th best found so far, and every document that
// carries only terms too weak to beat it. Any other query -- one with an
// And, a phrase, a Near or a Not -- is answered exactly as above.
//
// Takes no IScopeIndex, so a query with a SameScope node matches nothing
// here, as it does in perform_search without one. Lifetime and threading as
// for perform_search and BM25Scorer: the result aliases the index, and one
// call's result belongs to one thread.
RankedResult bm25_top_k(const IInvertedIndex &invidx, const Expression &expr,
                        size_t k, double k1 = 1.2, double b = 0.75);

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
// term positions on top -- one per range start, so terms starting at one
// byte share a position (TextSplitter) -- and applies the Normalizer the
// Tokenizer<T> contract
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
// Position policy: only tokens that survive the chain get 0,1,2,... term
// positions; dropped tokens do NOT consume a position (gaps are closed).
// This keeps the term_pos == text_range array-index invariant that
// InMemoryIndexer and the text-range machinery (including the Compressed
// on-disk layout) depend on -- see design section 1.1. The known trade-off
// is that a phrase spanning removed stop-words can false-match (e.g. "apple
// of the tree" indexes as "apple tree"); gap-preservation is a future step.
//
// Tokens the base tokenizer put on one position stay on one position, and a
// filter that emits more than once for one token (1->N, synonyms) puts every
// output there too: alternatives at a position, the same stacking a splitter
// expresses with equal range starts (see TextSplitter). Cutting a word into a
// sequence of pieces is a different thing and has its own home,
// subword_splitter, where each piece gets a position and a range.
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
    // The output position opens with the first token that survives at an
    // input position and advances when the input position moves on, so
    // drops close the gap (term_pos stays == text_range array index) and a
    // stack stays a stack.
    constexpr auto npos = std::numeric_limits<size_t>::max();
    size_t in_pos = npos;
    size_t term_pos = 0;
    bool opened = false;
    base_tokenizer_(normalizer, [&](const std::u32string &str, size_t pos,
                                    T text_range) {
      if (pos != in_pos) {
        if (opened) {
          term_pos++;
          opened = false;
        }
        in_pos = pos;
      }
      auto emit = [&](std::u32string out) {
        callback(out, term_pos, text_range);
        opened = true;
      };
      if (filter_) {
        filter_(str, emit);
      } else {
        emit(str);
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
// document). Other T fall back to the generic fixed-width section even in
// the Compressed format.
void save_text_ranges_compressed(std::ostream &os,
                                 const TextRangeList<TextRange> &list);
void load_text_ranges_compressed(std::istream &is,
                                 TextRangeList<TextRange> &list);

} // namespace detail

// Read-only backend for a Compressed-format index file that keeps the
// Elias-Fano postings and text-range structures compressed in
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
// external locking. Key must be the type the index was saved with, and one
// KeyTable reads without help: an integral type or std::string. There is no
// key-deserializer parameter here, so any other Key throws on load.
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
    size_t read_ordinals(size_t index, size_t *out,
                         size_t count) const override;
    size_t search_hit_count(size_t index) const override;

    size_t term_position(size_t index, size_t search_hit_index) const override;
    void read_term_positions(size_t index, size_t *out) const override;
    size_t term_length(size_t index, size_t search_hit_index) const override;
    bool is_term_position(size_t index, size_t term_pos) const override;

    // Appends term_pos for `ordinal`, which must be >= the last ordinal
    // appended: ordinals are handed out in indexing order and never
    // revisited (re-indexing gets a fresh one), so a postings list is
    // append-only. Returns false, appending nothing, when the entry is
    // already the last one: the same term stacked on one position twice.
    bool add_term_position(size_t ordinal, size_t term_pos);

    void save(std::ostream &os) const;
    void load(std::istream &is);

    // The same data as save(), delta-coded and varint-packed: ordinals
    // ascend, and so do the positions within one entry, so what gets written
    // are small gaps rather than 64-bit values. This is what the Compressed
    // format uses for the postings below kCompressedPostingsThreshold, where
    // Elias-Fano's fixed overhead would cost more than it saves. They load
    // into the very same three arrays, so this changes the file and nothing
    // else.
    void save_varint(std::ostream &os) const;
    void load_varint(std::istream &is);

    // Elias-Fano encoding of the same data as four monotone sequences:
    // the ordinals, the end offsets into the concatenated position array,
    // the positions made monotone by a per-entry base, and those bases
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

    // Positions must arrive in order, each either the next one or the one
    // just handed out: the text-range vector below is appended to once per
    // position but read back by term position (see the free text_range()),
    // so a position skipped or revisited would quietly hand a term another
    // term's range. The first term at a position brings the position's
    // range; the ones stacked on it are alternatives there and bring none.
    // Analyzer<T> keeps to this by construction; checking here covers a
    // hand-written Tokenizer<T>, which does not go through it.
    size_t position_count = 0;
    tokenizer(normalizer_, [&](const auto &str, auto term_pos,
                               auto text_range) {
      if (term_pos == position_count) {
        if (text_ranges_ == TextRangeStorage::Store) {
          index_.text_range_list_[ordinal].push_back(std::move(text_range));
        }
        position_count++;
      } else if (term_pos + 1 != position_count) {
        throw std::runtime_error(
            "searchlib: a tokenizer must emit term positions in order, each "
            "the next one or the one just emitted (a stacked term), because "
            "one term position holds exactly one text range; to cut a word "
            "into a sequence of pieces use subword_splitter");
      }

      if (index_.base_.term_dictionary_.find(str) ==
          index_.base_.term_dictionary_.end()) {
        index_.base_.term_dictionary_[str] = {str, 0};
      }

      // A term stacked on itself (a filter that answered the same string
      // twice) is one occurrence, not two.
      auto &term = index_.base_.term_dictionary_.at(str);
      if (term.postings.add_term_position(ordinal, term_pos)) {
        term.term_count++;
      }
    });

    // A document is as long as its positions: alternatives stacked on one
    // do not make the text longer.
    index_.base_.set_document_term_count(ordinal, position_count);
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

//-----------------------------------------------------------------------------
// Implementation details. Nothing below this line is API.
//-----------------------------------------------------------------------------

namespace detail {

//-----------------------------------------------------------------------------
// Succinct structures (Elias-Fano postings)
//-----------------------------------------------------------------------------

// Succinct-data-structure primitives for the Elias-Fano compressed postings
// (see docs/postings_compression_design.ja.md).

inline size_t popcount64(uint64_t x) {
#if defined(_MSC_VER)
  return static_cast<size_t>(__popcnt64(x));
#else
  return static_cast<size_t>(__builtin_popcountll(x));
#endif
}

inline size_t ctz64(uint64_t x) {
#if defined(_MSC_VER)
  unsigned long index;
  _BitScanForward64(&index, x);
  return static_cast<size_t>(index);
#else
  return static_cast<size_t>(__builtin_ctzll(x));
#endif
}

// Position of the k-th (0-based) set bit within a word. k must be less than
// popcount64(x).
inline size_t select_in_word(uint64_t x, size_t k) {
  for (size_t i = 0; i < k; i++) {
    x &= x - 1; // clear the lowest set bit
  }
  return ctz64(x);
}

// A plain bit vector with O(1) rank and O(log(n/512)) select. Constructed by
// set()-ing bits, then build() finalizes the rank index; the vector is
// immutable (and therefore safe to share across reader threads) afterwards.
//
// The rank index is one cumulative uint64_t per 512-bit superblock, so a
// rank is one table lookup plus at most 8 popcounts. Selects binary-search
// the superblock table and then scan within one superblock; at the scale
// this library targets that is a handful of steps and profiling should come
// before any fancier select structure.
class BitVector {
public:
  BitVector() = default;

  explicit BitVector(size_t bit_count)
      : bit_count_(bit_count), words_((bit_count + 63) / 64, 0) {}

  size_t size() const { return bit_count_; }

  void set(size_t i) { words_[i / 64] |= uint64_t(1) << (i % 64); }

  bool get(size_t i) const { return (words_[i / 64] >> (i % 64)) & 1; }

  // Builds the rank index. Must be called after the last set() and before
  // any rank/select query.
  void build() {
    auto superblock_count = superblock_count_();
    rank_.assign(superblock_count + 1, 0);
    uint64_t count = 0;
    for (size_t w = 0; w < words_.size(); w++) {
      if (w % kWordsPerSuperblock == 0) {
        rank_[w / kWordsPerSuperblock] = count;
      }
      count += popcount64(words_[w]);
    }
    rank_[superblock_count] = count;
    ones_ = static_cast<size_t>(count);
  }

  size_t ones() const { return ones_; }
  size_t zeros() const { return bit_count_ - ones_; }

  // Calls fn(position) for every 1 bit at or after `from`, in order, stopping
  // when fn returns false. This is what lets a run of values be read in one
  // pass over the words: select1 is O(log n) *per call* because it restarts
  // its superblock search, so walking a list through it costs as much as
  // jumping around at random (measured: 21.8ns/value sequential against
  // 32.9ns random, where one pass is 1.0ns).
  template <typename Fn> void for_each_one_from(size_t from, Fn fn) const {
    if (from >= bit_count_) {
      return;
    }
    auto w = from / 64;
    // The unused tail of the last word is zero-filled, so no masking is
    // needed at the end -- only at the start, to skip bits before `from`.
    auto word = words_[w] & (~uint64_t(0) << (from % 64));
    while (true) {
      while (word) {
        auto position = w * 64 + static_cast<size_t>(ctz64(word));
        word &= word - 1;
        if (!fn(position)) {
          return;
        }
      }
      if (++w >= words_.size()) {
        return;
      }
      word = words_[w];
    }
  }

  // Writes the bit count and raw words; the rank index is rebuilt on load,
  // so the on-disk form stays minimal and trivially deterministic.
  void save(std::ostream &os) const {
    write_scalar<uint64_t>(os, bit_count_);
    for (auto word : words_) {
      write_scalar<uint64_t>(os, word);
    }
  }

  void load(std::istream &is) {
    bit_count_ = static_cast<size_t>(read_scalar<uint64_t>(is));
    words_.assign((bit_count_ + 63) / 64, 0);
    for (auto &word : words_) {
      word = read_scalar<uint64_t>(is);
    }
    build();
  }

  // Number of 1 bits in [0, i). i may equal size().
  size_t rank1(size_t i) const {
    auto word_index = i / 64;
    auto count = static_cast<size_t>(rank_[i / kSuperblockBits]);
    for (auto w = (i / kSuperblockBits) * kWordsPerSuperblock; w < word_index;
         w++) {
      count += popcount64(words_[w]);
    }
    auto rem = i % 64;
    if (rem > 0) {
      count += popcount64(words_[word_index] & ((uint64_t(1) << rem) - 1));
    }
    return count;
  }

  // Number of 0 bits in [0, i). i may equal size().
  size_t rank0(size_t i) const { return i - rank1(i); }

  // Position of the k-th (0-based) 1 bit. k must be less than ones().
  size_t select1(size_t k) const {
    // Superblock containing the (k+1)-th one: largest s with rank_[s] <= k.
    auto sb = superblock_search_(
        [&](size_t s) { return static_cast<size_t>(rank_[s]); }, k);

    auto remaining = k - static_cast<size_t>(rank_[sb]);
    for (auto w = sb * kWordsPerSuperblock; w < words_.size(); w++) {
      auto count = popcount64(words_[w]);
      if (remaining < count) {
        return w * 64 + select_in_word(words_[w], remaining);
      }
      remaining -= count;
    }
    return bit_count_; // unreachable when k < ones()
  }

  // Position of the k-th (0-based) 0 bit. k must be less than zeros().
  size_t select0(size_t k) const {
    // Zeros before superblock s. The final superblock boundary may lie past
    // bit_count_; clamp so that the unused tail of the last word (always
    // zero-filled but outside the logical bit range) is not counted.
    auto zeros_at = [&](size_t s) {
      auto boundary = s * kSuperblockBits;
      if (boundary > bit_count_) {
        boundary = bit_count_;
      }
      return boundary - static_cast<size_t>(rank_[s]);
    };
    auto sb = superblock_search_(zeros_at, k);

    auto remaining = k - zeros_at(sb);
    for (auto w = sb * kWordsPerSuperblock; w < words_.size(); w++) {
      auto valid = bit_count_ - w * 64;
      if (valid > 64) {
        valid = 64;
      }
      auto count = valid - popcount64(words_[w]);
      if (remaining < count) {
        return w * 64 + select_in_word(~words_[w], remaining);
      }
      remaining -= count;
    }
    return bit_count_; // unreachable when k < zeros()
  }

private:
  static constexpr size_t kSuperblockBits = 512;
  static constexpr size_t kWordsPerSuperblock = kSuperblockBits / 64;

  size_t superblock_count_() const {
    return (words_.size() + kWordsPerSuperblock - 1) / kWordsPerSuperblock;
  }

  // Largest superblock s in [0, superblock_count) whose cumulative count
  // (per `cumulative`) is <= k. Relies on the sentinel entry at
  // superblock_count being > k.
  template <typename Cumulative>
  size_t superblock_search_(Cumulative cumulative, size_t k) const {
    size_t low = 1;
    auto high = superblock_count_();
    while (low < high) {
      auto mid = low + (high - low) / 2;
      if (cumulative(mid) > k) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    return low - 1;
  }

  size_t bit_count_ = 0;
  size_t ones_ = 0;
  std::vector<uint64_t> words_;
  std::vector<uint64_t> rank_;
};

// Elias-Fano encoding of a monotone (non-decreasing) uint64_t sequence,
// using close to the information-theoretic minimum n*(2 + log2(U/n)) bits.
// Each value is split into `low_bits_` low bits (packed fixed-width) and the
// remaining high bits (unary-coded bucket sizes in a BitVector), so that
// access(i) is one select1 and next_geq(target) is two select0 plus a scan
// of one bucket (~2 entries on average). Immutable after construction.
class EliasFano {
public:
  EliasFano() = default;

  // values must be sorted (duplicates allowed) with every element less than
  // universe.
  EliasFano(const std::vector<uint64_t> &values, uint64_t universe)
      : size_(values.size()), universe_(universe) {
    if (size_ == 0) {
      return;
    }

    // Low-bit width l = floor(log2(U/n)) keeps the high-bits vector at most
    // ~2n + 1 bits long (bucket count <= 2n).
    auto ratio = universe_ / size_;
    while (low_bits_ + 1 < 64 && (uint64_t(1) << (low_bits_ + 1)) <= ratio) {
      low_bits_++;
    }

    auto bucket_count = static_cast<size_t>((universe_ - 1) >> low_bits_) + 1;
    high_ = BitVector(size_ + bucket_count);
    low_words_.assign((size_ * low_bits_ + 63) / 64 + 1, 0);

    for (size_t i = 0; i < size_; i++) {
      auto value = values[i];
      high_.set(static_cast<size_t>(value >> low_bits_) + i);
      if (low_bits_ > 0) {
        auto bit = i * low_bits_;
        auto low = value & low_mask_();
        low_words_[bit / 64] |= low << (bit % 64);
        if (bit % 64 + low_bits_ > 64) {
          low_words_[bit / 64 + 1] |= low >> (64 - bit % 64);
        }
      }
    }
    high_.build();
  }

  size_t size() const { return size_; }
  uint64_t universe() const { return universe_; }

  // Only (size, universe) and the raw words are stored; the low-bit width
  // and array lengths are recomputed on load exactly as the constructor
  // derives them, so save/load round-trips bit-for-bit.
  void save(std::ostream &os) const {
    write_scalar<uint64_t>(os, size_);
    write_scalar<uint64_t>(os, universe_);
    if (size_ == 0) {
      return;
    }
    high_.save(os);
    for (auto word : low_words_) {
      write_scalar<uint64_t>(os, word);
    }
  }

  void load(std::istream &is) {
    size_ = static_cast<size_t>(read_scalar<uint64_t>(is));
    universe_ = read_scalar<uint64_t>(is);
    low_bits_ = 0;
    high_ = BitVector();
    low_words_.clear();
    if (size_ == 0) {
      return;
    }

    auto ratio = universe_ / size_;
    while (low_bits_ + 1 < 64 && (uint64_t(1) << (low_bits_ + 1)) <= ratio) {
      low_bits_++;
    }

    high_.load(is);
    auto bucket_count = static_cast<size_t>((universe_ - 1) >> low_bits_) + 1;
    if (high_.size() != size_ + bucket_count || high_.ones() != size_) {
      throw std::runtime_error("searchlib: corrupt Elias-Fano data");
    }

    low_words_.assign((size_ * low_bits_ + 63) / 64 + 1, 0);
    for (auto &word : low_words_) {
      word = read_scalar<uint64_t>(is);
    }
  }

  // The i-th value. i must be less than size().
  uint64_t access(size_t i) const {
    auto high = static_cast<uint64_t>(high_.select1(i) - i);
    return (high << low_bits_) | low_(i);
  }

  // Reads the values at index, index+1, ... into `out`, at most `count` of
  // them, and returns how many were written (fewer only at the end). One
  // select1 enters the high-bits vector and the rest of the run is a forward
  // scan, so a caller walking a sequence pays the select once per block
  // instead of once per value.
  template <typename T> size_t read(size_t index, T *out, size_t count) const {
    if (index >= size_) {
      return 0;
    }
    count = std::min(count, size_ - index);
    if (count == 0) {
      return 0;
    }
    size_t written = 0;
    high_.for_each_one_from(high_.select1(index), [&](size_t position) {
      auto i = index + written;
      // The i-th value's high part is (position of the i-th one) - i, the
      // same identity access() uses; only the way the position is found
      // differs.
      out[written] = static_cast<T>(
          ((static_cast<uint64_t>(position - i)) << low_bits_) | low_(i));
      return ++written < count;
    });
    return written;
  }

  // Index of the first value >= target, or size() if none. The bucket that
  // could contain target is located with two select0 calls, then scanned
  // linearly.
  size_t next_geq(uint64_t target) const {
    if (size_ == 0 || target >= universe_) {
      return target >= universe_ ? size_ : 0;
    }

    auto bucket = static_cast<size_t>(target >> low_bits_);
    // Ones before a bucket = position of its preceding zero terminator
    // minus the number of zeros before that terminator.
    auto begin =
        bucket == 0 ? 0 : high_.select0(bucket - 1) - (bucket - 1);
    auto end = high_.select0(bucket) - bucket;

    auto low_target = target & low_mask_();
    for (auto i = begin; i < end; i++) {
      if (low_(i) >= low_target) {
        return i;
      }
    }
    // The first value of any later bucket has a larger high part, so `end`
    // (== size() when the tail is empty) is the answer.
    return end;
  }

private:
  uint64_t low_mask_() const {
    return low_bits_ == 0 ? 0 : (uint64_t(1) << low_bits_) - 1;
  }

  uint64_t low_(size_t i) const {
    if (low_bits_ == 0) {
      return 0;
    }
    auto bit = i * low_bits_;
    auto value = low_words_[bit / 64] >> (bit % 64);
    if (bit % 64 + low_bits_ > 64) {
      value |= low_words_[bit / 64 + 1] << (64 - bit % 64);
    }
    return value & low_mask_();
  }

  size_t size_ = 0;
  uint64_t universe_ = 0;
  size_t low_bits_ = 0;
  BitVector high_;
  std::vector<uint64_t> low_words_;
};

//-----------------------------------------------------------------------------
// FST term dictionary
//-----------------------------------------------------------------------------

// The Compressed format stores the term dictionary as an FST over the UTF-8
// encoded term strings instead of writing each term out as a length-prefixed
// uint32-per-character string.
//
// fst::compile with need_output assigns each key a sequential uint32 in key
// order, and the terms are written in ascending term-string order already (a
// format invariant, see InMemoryInvertedIndexBase::save). UTF-8 byte order
// matches codepoint order, so the FST's ordinal for a term is exactly its
// position in that sequence: whatever per-term records follow the FST can be
// indexed directly by the ordinal, with no separate mapping.
//
// Everything about the section lives here because three places have to agree
// on it: InMemoryInvertedIndexBase's writer and reader, and the compressed
// backend's independent reader.
//
// Layout:
//   uint64  fst_byte_size
//   char[fst_byte_size]  fst byte code (empty when there are no terms)

// Builds and writes the FST section. `keys` must be UTF-8, in ascending byte
// order, and unique; each key's ordinal is its index in `keys`.
inline void write_term_dictionary_fst(std::ostream &os,
                                      const std::vector<std::string> &keys) {
  if (keys.empty()) {
    write_scalar<uint64_t>(os, 0);
    return;
  }

  std::ostringstream fst_os;
  auto [result, error_index] =
      fst::compile(keys, fst_os, /*need_output=*/true, /*sorted=*/true);
  if (result != fst::Result::Success) {
    // EmptyKey is reachable from ordinary user code, since a Normalizer may
    // map a token to the empty string; the other two would mean save() broke
    // its own sorted-and-unique invariant.
    const char *reason = result == fst::Result::EmptyKey ? "an empty term"
                         : result == fst::Result::DuplicateKey
                             ? "a duplicate term"
                             : "unsorted terms";
    throw std::runtime_error(
        "searchlib: cannot build the term dictionary FST (" +
        std::string(reason) + " at index " + std::to_string(error_index) +
        ")");
  }

  write_bytes(os, fst_os.str());
}

// Drives a glob match ('*' = zero or more codepoints, every other codepoint
// literal) through an FST via fst::map::custom_search. Modeled on fstlib's
// own LevenshteinAutomaton: bit i of the state tracks whether the codepoints
// consumed so far can match pattern[0:i), the same subset-construction DP
// that InMemoryInvertedIndexBase's non-incremental matches_wildcard runs in
// one shot, just updated one codepoint at a time as custom_search descends
// the FST. Bytes are buffered until a full UTF-8 codepoint decodes (the FST
// visits one byte per arc) so a star never gets tested against half a
// multi-byte character.
//
// The whole DP row is a bitmask, so one codepoint costs a handful of word
// operations instead of a loop over the pattern, and -- what matters far more
// -- a pattern short enough to fit in one word carries its state inline,
// which is what keeps the per-arc copy below from allocating.
class WildcardAutomaton {
public:
  // The pattern, compiled once per query into the masks step() needs. Every
  // copy the traversal makes points at the same one, hence immutable.
  struct Program {
    size_t size = 0;       // pattern length in codepoints, after collapsing
    size_t word_count = 1; // words needed for size + 1 bits
    std::vector<uint64_t> star;
    // Which positions carry which literal. ASCII gets a directly indexed
    // table because that is the common case by far; anything above it falls
    // back to a binary search over the distinct codepoints the pattern
    // actually mentions, which is at most one per position.
    std::vector<uint64_t> ascii; // 128 rows of word_count words
    std::vector<char32_t> wide;  // sorted, >= U+0080 only
    std::vector<uint64_t> wide_masks;
    std::vector<uint64_t> no_match; // word_count zeros

    explicit Program(const std::u32string &raw) {
      // "**" means what "*" means, and collapsing the runs is what lets
      // close_over_stars() below be a single shift-or: with no two star bits
      // adjacent, a bit the closure sets can never feed another star.
      std::u32string pattern;
      for (auto cp : raw) {
        if (cp == U'*' && !pattern.empty() && pattern.back() == U'*') {
          continue;
        }
        pattern += cp;
      }

      size = pattern.size();
      word_count = (size + 1 + 63) / 64;
      star.assign(word_count, 0);
      no_match.assign(word_count, 0);
      ascii.assign(128 * word_count, 0);

      for (auto cp : pattern) {
        if (cp >= 0x80) { wide.push_back(cp); }
      }
      std::sort(wide.begin(), wide.end());
      wide.erase(std::unique(wide.begin(), wide.end()), wide.end());
      wide_masks.assign(wide.size() * word_count, 0);

      // Position i stands for pattern[i - 1], so the masks start at bit 1.
      // Bit 0 is position 0, the empty prefix, which no pattern codepoint
      // owns and so no mask touches.
      for (size_t i = 1; i <= size; i++) {
        auto cp = pattern[i - 1];
        auto bit = uint64_t(1) << (i % 64);
        if (cp == U'*') {
          star[i / 64] |= bit;
        } else if (cp < 0x80) {
          ascii[size_t(cp) * word_count + i / 64] |= bit;
        } else {
          auto it = std::lower_bound(wide.begin(), wide.end(), cp);
          wide_masks[size_t(it - wide.begin()) * word_count + i / 64] |= bit;
        }
      }
    }

    const uint64_t *literal_mask(char32_t cp) const {
      if (cp < 0x80) { return ascii.data() + size_t(cp) * word_count; }
      auto it = std::lower_bound(wide.begin(), wide.end(), cp);
      if (it == wide.end() || *it != cp) { return no_match.data(); }
      return wide_masks.data() + size_t(it - wide.begin()) * word_count;
    }
  };

  explicit WildcardAutomaton(const Program &program) : program_(&program) {
    state_.init(program_->word_count);
    auto *row = state_.data();
    row[0] = 1; // the empty prefix matches pattern[0:0)
    close_over_stars(row);
  }

  // The Program has to outlive every copy of the automaton, so binding one
  // to a temporary is a mistake worth refusing outright.
  explicit WildcardAutomaton(Program &&) = delete;

  // custom_search's FST traversal copies the automaton once per arc it
  // visits (fstlib's depth_first_visit, one copy per sibling byte-edge), so
  // program_ is a bare pointer to a Program the caller keeps alive for the
  // whole traversal, so that copy stays a word copy: a shared_ptr would put
  // an atomic increment and an acquire-release decrement on every arc, and
  // the shared ownership would never be used -- every copy the traversal
  // makes is destroyed inside the scope that built the Program.
  WildcardAutomaton(const WildcardAutomaton &) = default;

  void step(char c) {
    // The buffer stops at 4 bytes because decode_codepoint only ever inspects
    // the lead byte's shape: once 4 bytes have failed to decode, no further
    // byte can make it succeed, and the automaton is permanently
    // non-matching either way. Refusing the extra bytes keeps this state
    // bounded instead of accumulating the rest of a malformed key.
    if (u8len_ < sizeof(u8bytes_)) { u8bytes_[u8len_++] = c; }
    char32_t cp;
    if (unicode::utf8::decode_codepoint(u8bytes_, u8len_, cp) == 0) {
      return; // mid-codepoint; wait for more bytes
    }
    u8len_ = 0;

    // One step of the DP, on every position at once:
    //   literal position i: new[i] = old[i - 1] && pattern[i - 1] == cp
    //                             -> (old << 1) & literal_mask(cp)
    //   star position i:    new[i] = old[i] || new[i - 1]
    //                             -> (old & star) then close_over_stars
    // Bit 0 falls out as zero on its own: nothing shifts into it, and no star
    // bit sits there.
    const auto *literal = program_->literal_mask(cp);
    const auto *star = program_->star.data();
    auto *row = state_.data();

    if (program_->word_count == 1) {
      auto old = row[0];
      auto next = ((old << 1) & literal[0]) | (old & star[0]);
      next |= (next << 1) & star[0];
      row[0] = next;
      live_ = next;
      return;
    }

    uint64_t carry = 0;
    for (size_t w = 0; w < program_->word_count; w++) {
      auto old = row[w];
      auto shifted = (old << 1) | carry;
      carry = old >> 63;
      row[w] = (shifted & literal[w]) | (old & star[w]);
    }
    close_over_stars(row);
  }

  bool is_match() const {
    if (u8len_ > 0) { return false; }
    return (state_.data()[program_->size / 64] >> (program_->size % 64)) & 1;
  }

  // A whole-row test that costs nothing to keep: custom_search calls this
  // once per arc, just like step(), and step() has the row in registers
  // anyway.
  bool can_match() const { return live_ != 0; }

private:
  // The DP row. depth_first_visit copies the automaton once per arc, so a
  // std::vector here would mean a malloc/free pair per arc; a pattern of up
  // to 63 codepoints -- every pattern anyone types -- keeps its row inline
  // instead. Longer patterns spill to the heap so the class stays general.
  //
  // data() is recomputed from word_count_ rather than cached in a member
  // pointer, so there is nothing to fix up after a copy and the copy
  // constructor can stay defaulted.
  //
  // There are deliberately no move operations. A defaulted move would take
  // heap_ out of a spilled row while leaving word_count_ still claiming it,
  // so data() would return nullptr; WildcardAutomaton's user-declared copy
  // constructor suppresses its own implicit moves, which is what keeps that
  // unreachable today.
  class Row {
  public:
    // Sizes the row once, at construction; cells are not carried across the
    // inline/heap boundary, so this is not a general resize.
    void init(size_t word_count) {
      word_count_ = word_count;
      if (word_count > 1) { heap_.assign(word_count, 0); }
    }

    uint64_t *data() { return word_count_ <= 1 ? &inline_ : heap_.data(); }
    const uint64_t *data() const {
      return word_count_ <= 1 ? &inline_ : heap_.data();
    }

  private:
    size_t word_count_ = 1;
    uint64_t inline_ = 0;
    std::vector<uint64_t> heap_; // empty unless word_count_ > 1
  };

  // A star matches the empty string, so a live position immediately below a
  // star makes that star live too. Collapsed runs mean one shift-or reaches
  // the fixed point; the carry hands bit 63 of a word to bit 0 of the next.
  //
  // The OR of the row is accumulated here rather than rescanned in
  // can_match(), which the traversal calls once per arc just like step().
  // The constructor's initial live_ comes from this same pass.
  void close_over_stars(uint64_t *row) {
    const auto *star = program_->star.data();
    uint64_t carry = 0;
    uint64_t live = 0;
    for (size_t w = 0; w < program_->word_count; w++) {
      auto next = row[w] | (((row[w] << 1) | carry) & star[w]);
      carry = next >> 63;
      row[w] = next;
      live |= next;
    }
    live_ = live;
  }

  const Program *program_;
  Row state_;
  uint64_t live_ = 0;   // the OR of state_, so can_match() need not rescan
  char u8bytes_[4]{};   // bytes of a not-yet-fully-decoded codepoint
  uint8_t u8len_ = 0;
};

// The FST section as read back from a stream. It owns both the byte code and
// the fst::map built over it; fst::map keeps a bare pointer into the bytes
// rather than copying them, so the two must never be separated. Hence this
// type is neither copyable nor movable, which also propagates to whatever
// holds it as a member.
class TermDictionaryFst {
public:
  TermDictionaryFst() = default;
  TermDictionaryFst(const TermDictionaryFst &) = delete;
  TermDictionaryFst &operator=(const TermDictionaryFst &) = delete;
  TermDictionaryFst(TermDictionaryFst &&) = delete;
  TermDictionaryFst &operator=(TermDictionaryFst &&) = delete;

  void load(std::istream &is) {
    bytes_ = read_bytes(is);
    if (bytes_.empty()) {
      return; // no terms, so no FST was written
    }
    map_ = std::make_unique<fst::map<uint32_t>>(bytes_.data(), bytes_.size());
    if (!*map_) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }
  }

  // Exact lookup. Returns false for an absent term, or when the dictionary
  // is empty.
  bool find(const std::u32string &str, uint32_t &ordinal) const {
    return map_ && map_->exact_match_search(u8(str), ordinal);
  }

  void enumerate_with_prefix(
      const std::u32string &prefix,
      const std::function<void(const std::u32string &str)> &callback) const {
    if (!map_) {
      return;
    }
    // One buffer reused across hits: a u32string longer than the small-string
    // buffer (4 codepoints on libc++) would otherwise cost a malloc/free pair
    // per term. Callbacks receive it by reference and must not retain it,
    // which is what IInvertedIndex::enumerate_terms_with_prefix promises.
    std::u32string term;
    map_->predictive_search(u8(prefix),
                            [&](const std::string &key, const uint32_t &) {
                              term.clear();
                              unicode::utf8::decode(key, term);
                              callback(term);
                            });
  }

  void enumerate_with_wildcard(
      const std::u32string &pattern,
      const std::function<void(const std::u32string &str)> &callback) const {
    // The Program outlives the traversal, which is what lets the automaton
    // (and every copy custom_search makes of it) hold a bare pointer to it.
    WildcardAutomaton::Program program(pattern);
    enumerate_with_automaton(WildcardAutomaton(program), callback);
  }

  void enumerate_with_edit_distance(
      const std::u32string &target, size_t max_edits,
      const std::function<void(const std::u32string &str)> &callback) const {
    // fst::map::edit_distance_search would do the same walk, but it collects
    // every hit into a vector first, and it rejects an empty needle outright;
    // going through custom_search keeps this in the same streaming,
    // one-reused-buffer shape as the enumerators above, and lets an empty
    // `target` mean what the interface says it means (every term of length <=
    // max_edits), which is what the in-memory backend answers too. The
    // automaton is fstlib's own, so nothing is reimplemented here.
    enumerate_with_automaton(
        fst::LevenshteinAutomaton(u8(target), max_edits, /*insert_cost=*/1,
                                  /*delete_cost=*/1, /*replace_cost=*/1),
        callback);
  }

  // Recovers the ordinal -> term mapping. Used by readers that need the term
  // strings themselves (the in-memory index rebuilds its hash map from them);
  // readers that only look terms up keep this object instead and never
  // materialize the vocabulary.
  std::vector<std::u32string> terms(size_t term_count) const {
    std::vector<std::u32string> result(term_count);
    if (term_count == 0) {
      return result;
    }
    if (!map_) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }

    // enumerate's order is unspecified, hence indexing by the ordinal rather
    // than appending. Every ordinal has to be claimed exactly once: a
    // duplicate would overwrite one term and leave another slot empty, which
    // would silently attach a record to the wrong term.
    std::vector<bool> claimed(term_count, false);
    size_t seen = 0;
    map_->enumerate([&](const std::string &key, const uint32_t &ordinal) {
      if (ordinal >= term_count || claimed[ordinal]) {
        throw std::runtime_error("searchlib: corrupt term dictionary FST");
      }
      claimed[ordinal] = true;
      result[ordinal] = u32(key);
      seen++;
    });
    if (seen != term_count) {
      throw std::runtime_error("searchlib: corrupt term dictionary FST");
    }

    return result;
  }

private:
  // The shape both automaton-driven enumerators share: one buffer refilled per
  // hit rather than a fresh u32string each time, since a term longer than the
  // small-string buffer (4 codepoints on libc++) would otherwise cost a
  // malloc/free pair. Callbacks receive it by reference and must not retain
  // it, which is what the IInvertedIndex::enumerate_terms_with_* contract
  // promises. (enumerate_with_prefix stays separate: it descends to a subtree
  // with predictive_search instead of walking with an automaton.)
  template <typename Automaton>
  void enumerate_with_automaton(
      const Automaton &automaton,
      const std::function<void(const std::u32string &str)> &callback) const {
    if (!map_) {
      return;
    }
    std::u32string term;
    map_->custom_search(automaton,
                        [&](const std::string &key, const uint32_t &) {
                          term.clear();
                          unicode::utf8::decode(key, term);
                          callback(term);
                        });
  }

  std::string bytes_;
  std::unique_ptr<fst::map<uint32_t>> map_; // points into bytes_
};

//-----------------------------------------------------------------------------
// Text splitting
//-----------------------------------------------------------------------------

// The one definition of "what a raw term is" in this library: a UAX #29 word
// segment containing a letter or a number. UTF8PlainTextTokenizer,
// utf8_plain_text_splitter() and the segmenting splitter all go through
// for_each_word below, so there is no second place where the rule could drift.
//
// The text is decoded once into scalars with a byte offset per scalar, and
// the boundaries come from unicode::word_length handed the remainder from each
// boundary -- not from is_word_boundary at every position, which unicodelib's
// segment_length explains is quadratic over a run of flags; both documents and
// query strings arrive from outside.
//
// An ill-formed byte -- a truncated sequence, a bad continuation byte, an
// overlong encoding, a surrogate, a value past U+10FFFF -- becomes U+FFFD and
// the walk steps over that one byte. It has no letter, so it is never a term
// and it separates whatever it sits between; and the offset table stays true,
// so nothing spins on `pos += 0` (reachable from a query string) and no range
// crosses bytes that were never decoded.
struct WordScratch {
  std::u32string cps;          // the text's scalars
  std::vector<size_t> offsets; // offsets[k] = byte offset of cps[k]; one more
                               // entry holds text.size(), so every scalar
                               // boundary in [0, text.size()] is in the table
  std::u32string str;          // the term being emitted
  std::vector<std::pair<std::u32string, TextRange>> delegated; // one span's
                                                               // words, held
                                                               // until checked

  void decode(std::string_view text) {
    cps.clear();
    offsets.clear();
    size_t pos = 0;
    while (pos < text.size()) {
      char32_t cp;
      auto len =
          unicode::utf8::decode_codepoint(&text[pos], text.size() - pos, cp);
      if (len == 0) {
        cp = 0xFFFD;
        len = 1;
      }
      cps += cp;
      offsets.push_back(pos);
      pos += len;
    }
    offsets.push_back(pos);
  }

  // The scalar index at a byte offset, or npos when the offset is not a
  // scalar boundary (or lies outside the text).
  size_t scalar_at(size_t byte_offset) const {
    auto it = std::lower_bound(offsets.begin(), offsets.end(), byte_offset);
    if (it == offsets.end() || *it != byte_offset) {
      return std::string::npos;
    }
    return static_cast<size_t>(it - offsets.begin());
  }

  TextRange range(size_t from, size_t to) const {
    return TextRange{offsets[from], offsets[to] - offsets[from]};
  }
};

// The thread's WordScratch, reused across calls: decoding allocates its
// buffers otherwise, and on test/t_kjv.tsv (31,103 short documents) that is 9%
// of the whole index build. A lease moves it out for the duration of a walk
// and back after, so a walk re-entered from inside -- a Segmenter calling the
// default splitter back for a span it does not handle -- finds an empty
// scratch, allocates its own and hands that back: re-entry is merely slower.
struct WordScratchLease {
  WordScratch scratch = std::move(slot());

  WordScratchLease() = default;
  WordScratchLease(const WordScratchLease &) = delete;
  WordScratchLease &operator=(const WordScratchLease &) = delete;
  ~WordScratchLease() { slot() = std::move(scratch); }

  static WordScratch &slot() {
    thread_local WordScratch scratch;
    return scratch;
  }
};

inline bool is_word_like(const char32_t *cps, size_t n) {
  for (size_t i = 0; i < n; i++) {
    if (unicode::is_letter(cps[i]) || unicode::is_number(cps[i])) {
      return true;
    }
  }
  return false;
}

// What utf8_plain_text_splitter(segmenter, claims) hands for_each_word.
struct Delegation {
  Segmenter segmenter;
  std::function<bool(char32_t)> claims;
};

// Runs the segmenter from scalar `i` and forwards what survives the checks
// listed at Segmenter, then returns the scalar index to resume at. Grapheme
// boundaries are tested relative to `i`, which is a word boundary and so a
// grapheme boundary too -- and that keeps the test linear, for the reason
// unicodelib's segment_length gives.
template <typename Callback>
size_t delegate_word(std::string_view text, WordScratch &scratch, size_t i,
                     const Delegation &delegation, Callback &callback) {
  const auto *cps = scratch.cps.data() + i;
  auto n = scratch.cps.size() - i;
  auto offset = scratch.offsets[i];

  auto &buffer = scratch.delegated;
  buffer.clear();
  auto consumed = delegation.segmenter(
      text, offset, [&](const std::u32string &str, TextRange range) {
        buffer.emplace_back(str, range);
      });

  auto end = consumed == 0 ? std::string::npos
                           : scratch.scalar_at(offset + consumed);
  if (end == std::string::npos ||
      !unicode::is_grapheme_boundary(cps, n, end - i)) {
    return i + unicode::grapheme_length(cps, n);
  }

  // Starts may repeat (a stack) and may not go back; the span's own start
  // is the first floor.
  auto last_start = offset;
  for (const auto &[str, range] : buffer) {
    auto stop = range.position + range.length;
    if (range.length == 0 || range.position < last_start ||
        stop > offset + consumed) {
      continue;
    }
    auto from = scratch.scalar_at(range.position);
    auto to = scratch.scalar_at(stop);
    if (from == std::string::npos || to == std::string::npos ||
        !unicode::is_grapheme_boundary(cps, n, from - i) ||
        !unicode::is_grapheme_boundary(cps, n, to - i)) {
      continue;
    }
    callback(str, range);
    last_start = range.position;
  }
  return end;
}

// Calls callback(str, range) once per word with the word's scalars and its
// byte offsets into `text`. Words come out with starts that never decrease:
// the default rule's never overlap, a segmenter's may stack (see
// TextSplitter). `delegation` may be null.
//
// A template taking the callback by deduced type, rather than a plain function
// taking a std::function, because this is the indexing hot path and the
// indirection dominates it (measured on the letter-run predecessor: 22.4 ms
// tokenizing test/t_kjv.tsv as written, 34.0 ms behind a `const
// std::function&`). Taken by forwarding reference rather than by value so that
// a caller handing over a std::function (utf8_plain_text_splitter's `emit`,
// and parse_query's per-token one) does not pay a copy of it -- that copy
// heap-allocates, because the emitters here capture more than the inline
// buffer holds.
template <typename Callback>
void for_each_word(std::string_view text, const Delegation *delegation,
                   Callback &&callback) {
  WordScratchLease lease;
  auto &scratch = lease.scratch;
  scratch.decode(text);
  const auto &cps = scratch.cps;
  auto n = cps.size();

  size_t i = 0;
  while (i < n) {
    if (delegation && delegation->claims(cps[i])) {
      i = delegate_word(text, scratch, i, *delegation, callback);
      continue;
    }
    auto len = unicode::word_length(cps.data() + i, n - i);
    if (is_word_like(cps.data() + i, len)) {
      scratch.str.assign(cps.data() + i, len);
      callback(scratch.str, scratch.range(i, i + len));
    }
    i += len;
  }
}


//-----------------------------------------------------------------------------
// Inverted index internals
//-----------------------------------------------------------------------------

// Terms with fewer postings entries than this skip Elias-Fano even in the
// Compressed format and are delta+varint coded instead (Postings::save_varint):
// term frequencies are Zipf
// distributed, so most terms have tiny postings where the Elias-Fano
// structures' fixed overhead would exceed the savings. The chosen
// representation is recorded per term in the file, so this threshold can be
// tuned without breaking compatibility.
//
// Measured on KJV (31,103 documents, 12,816 terms): Elias-Fano first pays for
// itself at 9 entries, and the file shrinks 5.14 -> 4.29 MB (-16.7%) moving
// the threshold from 64 to 16, against 4.18 MB (-18.7%) at 9. A term that
// crosses into Elias-Fano costs 1.5-2x to read back -- a 60-entry term's
// top-10 goes from 1.12 to 1.71 us -- so the last two percent of file size
// are not worth the extra 1,215 terms paying that. See
// docs/postings_compression_design.ja.md section 6.2.
inline constexpr size_t kCompressedPostingsThreshold = 16;

// Definition of the type declared opaque up in the interface. One Elias-Fano
// sequence per (scope_name, ordinal), each mapping term_pos -> scope ordinal
// via EliasFano::access.
class ScopeIndexData {
public:
  std::unordered_map<std::string, std::unordered_map<size_t, EliasFano>>
      by_name;
};

// Assumes document_ordinal(index) is monotonically increasing in index,
// which holds for every IPostings this library produces: Postings is
// append-only in ordinal order, and SearchResult only ever appends documents
// in ascending order via its cursor-merge algorithms.
inline size_t find_postings_index_for_ordinal(const IPostings &p,
                                              size_t ordinal) {
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

// True if `term` matches the glob `pattern` (`*` = zero or more codepoints,
// every other codepoint literal). Standard two-pointer greedy matcher with
// backtracking to the most recent `*`: linear in practice, quadratic only on
// adversarial inputs (many stars each forced to backtrack).
inline bool matches_wildcard(const std::u32string &pattern,
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
inline bool within_edit_distance(const std::u32string &a,
                                 const std::u32string &b, size_t max_edits,
                                 std::vector<size_t> &prev,
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

//-----------------------------------------------------------------------------
// Compressed (read-only) index backend
//-----------------------------------------------------------------------------

// IPostings served directly from the four Elias-Fano sequences written by
// Postings::save_compressed, without expanding them (see
// docs/postings_compression_design.ja.md section 5). Every accessor is an
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

  bool prefers_block_reads() const override { return true; }

  size_t read_ordinals(size_t index, size_t *out,
                       size_t count) const override {
    return ordinals_.read(index, out, count);
  }

  size_t search_hit_count(size_t index) const override {
    return static_cast<size_t>(end_offsets_.access(index) - begin_(index));
  }

  size_t read_hit_counts(size_t index, size_t *out,
                         size_t count) const override {
    // The end offsets of the run in one pass, then differences in place:
    // one select for the run and one for the offset before it, instead of
    // two per entry.
    count = end_offsets_.read(index, out, count);
    auto previous = begin_(index);
    for (size_t i = 0; i < count; i++) {
      auto end = out[i];
      out[i] = end - previous;
      previous = end;
    }
    return count;
  }

  size_t advance(size_t, size_t ordinal) const override {
    return ordinals_.next_geq(ordinal);
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return static_cast<size_t>(
        positions_.access(static_cast<size_t>(begin_(index)) +
                          search_hit_index) -
        bases_.access(index));
  }

  void read_term_positions(size_t index, size_t *out) const override {
    // The entry's slice of the monotonized sequence in one pass, then its
    // base off each: one select to enter the run instead of one per value.
    auto begin = begin_(index);
    auto count = static_cast<size_t>(end_offsets_.access(index) - begin);
    positions_.read(static_cast<size_t>(begin), out, count);
    auto base = bases_.access(index);
    for (size_t i = 0; i < count; i++) {
      out[i] -= static_cast<size_t>(base);
    }
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return 1;
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    // A probe for the position before an entry's first term arrives as a
    // wrapped-around huge term_pos, which a vector-backed binary search
    // harmlessly misses. Here it would wrap
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

  EliasFano ordinals_;
  EliasFano end_offsets_;
  EliasFano bases_;
  EliasFano positions_;
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
  EliasFano ordinals_;
  EliasFano end_offsets_;
  EliasFano bases_;
  EliasFano positions_;
  EliasFano cumulative_lengths_;
};

// The read-only index behind load_compressed_index. Documents and term
// statistics are small and kept expanded (bm25 needs them anyway); the
// memory heavyweights -- postings and text ranges -- stay Elias-Fano
// coded. Terms below the compression threshold were written as varints and
// are expanded into ordinary Postings. Immutable after load, hence trivially safe to
// share across reader threads.
template <typename Key>
class CompressedInvertedIndex
    : public IInvertedIndexWithTextRange<TextRange, Key> {
public:
  void load(std::istream &is) {
    char magic[sizeof(kIndexMagic)];
    is.read(magic, sizeof(magic));
    if (!is || std::memcmp(magic, kIndexMagic, sizeof(magic)) != 0) {
      throw std::runtime_error("searchlib: not a valid index file (bad magic)");
    }
    if (read_scalar<uint32_t>(is) != kFormatTypeCompressed) {
      throw std::runtime_error(
          "searchlib: the compressed backend requires a Compressed-format "
          "index file");
    }
    if (read_scalar<uint32_t>(is) != kSchemaVersionCompressed) {
      throw std::runtime_error("searchlib: unsupported index schema_version");
    }

    // Section order mirrors InMemoryInvertedIndexBase::load, then the key
    // section, then the text-range section of
    // InMemoryInvertedIndex<TextRange, Key>::load. Document records are in
    // ordinal order, so the vector indexes by ordinal directly.
    auto document_count = read_scalar<uint64_t>(is);
    term_counts_.reserve(static_cast<size_t>(document_count));
    for (uint64_t i = 0; i < document_count; i++) {
      term_counts_.push_back(static_cast<size_t>(read_varint(is)));
    }

    // Term dictionary: the FST stays as the dictionary rather than being
    // expanded into a hash map, so lookups run over the byte code in place
    // and prefix enumeration descends a subtree instead of scanning. The
    // per-term records that follow are in FST-ordinal order (see
    // TermDictionaryFst), so the ordinal indexes terms_ directly.
    auto term_count = static_cast<size_t>(read_scalar<uint64_t>(is));
    term_dictionary_.load(is);

    terms_.resize(term_count);
    for (size_t i = 0; i < term_count; i++) {
      auto &term = terms_[i];
      term.term_count = static_cast<size_t>(read_varint(is));
      if (read_scalar<uint8_t>(is)) {
        auto postings = std::make_shared<EFPostings>();
        postings->load(is);
        term.postings = std::move(postings);
      } else {
        // Below the threshold: varint on disk, expanded into the ordinary
        // Postings here, exactly as InMemoryInvertedIndexBase::load does.
        auto postings =
            std::make_shared<InMemoryInvertedIndexBase::Postings>();
        postings->load_varint(is);
        term.postings = std::move(postings);
      }
    }

    auto removed_count = read_scalar<uint64_t>(is);
    removed_ordinals_.reserve(static_cast<size_t>(removed_count));
    for (uint64_t i = 0; i < removed_count; i++) {
      removed_ordinals_.insert(
          static_cast<size_t>(read_scalar<uint64_t>(is)));
    }

    // Scope-index section (see InMemoryInvertedIndexBase::save): read and
    // discard, since IScopeIndex is not exposed by this read-only backend
    // yet (v1 limitation, see docs/missing_features.ja.md 3.4.1). Skipping
    // it explicitly keeps the following sections aligned.
    auto scope_name_count = read_scalar<uint64_t>(is);
    for (uint64_t i = 0; i < scope_name_count; i++) {
      auto name_size = static_cast<size_t>(read_scalar<uint64_t>(is));
      std::string name(name_size, '\0');
      is.read(name.data(), static_cast<std::streamsize>(name_size));

      auto doc_count = read_scalar<uint64_t>(is);
      for (uint64_t j = 0; j < doc_count; j++) {
        read_scalar<uint64_t>(is); // ordinal
        EliasFano discarded;
        discarded.load(is);
      }
    }

    keys_.load(is);
    if (keys_.size() != term_counts_.size()) {
      throw std::runtime_error(
          "searchlib: the key section disagrees with the documents section");
    }

    if (read_scalar<uint32_t>(is) != 1) {
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
    auto i = find_postings_index_for_ordinal(*p, ordinal);
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
    auto i = find_postings_index_for_ordinal(*p, ordinal);
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
  // is required: the FST points into its own byte buffer (see
  // TermDictionaryFst).
  TermDictionaryFst term_dictionary_;
  std::vector<CompressedTerm> terms_; // indexed by the FST's ordinal

  std::unordered_set<size_t> removed_ordinals_;
  EFTextRanges text_ranges_;
  double average_document_term_count_ = 0.0;
};

//-----------------------------------------------------------------------------
// Search evaluation
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------

// Decorator that hides logically-deleted (tombstoned) documents from a search
// result. It maps external indices to the underlying result's indices, skipping
// any entry whose ordinal was removed from the index. Applied once at the
// top of perform_search, so it uniformly covers Term/And/Or/Adjacent/Near.
class FilteredPostings : public IPostings {
public:
  FilteredPostings(const IInvertedIndex &inverted_index,
                   std::shared_ptr<IPostings> postings)
      : postings_(std::move(postings)) {
    auto count = postings_->size();
    live_indices_.reserve(count);
    for (size_t i = 0; i < count; i++) {
      if (!inverted_index.is_document_removed(postings_->document_ordinal(i))) {
        live_indices_.push_back(i);
      }
    }
  }

  ~FilteredPostings() override = default;

  size_t size() const override { return live_indices_.size(); }

  size_t document_ordinal(size_t index) const override {
    return postings_->document_ordinal(live_indices_[index]);
  }

  size_t search_hit_count(size_t index) const override {
    return postings_->search_hit_count(live_indices_[index]);
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return postings_->term_position(live_indices_[index], search_hit_index);
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return postings_->term_length(live_indices_[index], search_hit_index);
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    return postings_->is_term_position(live_indices_[index], term_pos);
  }

private:
  std::shared_ptr<IPostings> postings_;
  std::vector<size_t> live_indices_;
};

//-----------------------------------------------------------------------------

// Result of an And/Or/Adjacent/Near/SameScope operation.
//
// The hits live in four dense arrays rather than one heap object per matched
// document: the document ids, an offset table indexing into a single
// concatenated arena of term positions, and the term lengths running parallel
// to that arena. Building a result therefore allocates on the order of its
// total size instead of once per hit -- the previous shape put a Position and
// two vectors on the heap for every document, which on a 3,490-hit union came
// to 25,579 allocations and dominated the search phase.
//
// This is the same layout the compressed backend's EFPostings already uses,
// minus the Elias-Fano encoding.
class SearchResult : public IPostings {
public:
  ~SearchResult() override = default;

  size_t size() const override { return ordinals_.size(); }

  size_t document_ordinal(size_t index) const override {
    return ordinals_[index];
  }

  size_t search_hit_count(size_t index) const override {
    return offsets_[index + 1] - offsets_[index];
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    return positions_[offsets_[index] + search_hit_index];
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    return lengths_[offsets_[index] + search_hit_index];
  }

  bool is_term_position(size_t index, size_t term_pos) const override {
    // Positions within one document are appended in ascending order by every
    // operation, the same invariant the old per-document vector relied on.
    return std::binary_search(positions_.begin() + offsets_[index],
                              positions_.begin() + offsets_[index + 1],
                              term_pos);
  }

  // Appends one matched document, taking its hits from the scratch buffers
  // the operation filled, and leaving those cleared for the next document.
  void push_back(size_t ordinal, std::vector<size_t> &term_positions,
                 std::vector<size_t> &term_lengths) {
    ordinals_.push_back(ordinal);
    positions_.insert(positions_.end(), term_positions.begin(),
                      term_positions.end());
    lengths_.insert(lengths_.end(), term_lengths.begin(), term_lengths.end());
    offsets_.push_back(positions_.size());
    term_positions.clear();
    term_lengths.clear();
  }

private:
  std::vector<size_t> ordinals_;
  std::vector<size_t> offsets_{0}; // size() + 1 entries
  std::vector<size_t> positions_;  // every hit, concatenated
  std::vector<size_t> lengths_;    // parallel to positions_
};

//-----------------------------------------------------------------------------

// Dispatches an expression to its operation handler without applying the
// tombstone filter. Used internally (including for recursive sub-expressions)
// so that removed documents are filtered exactly once, at the public entry.
// scope_index is threaded through purely so that a nested Operation::SameScope
// node can reach it; every other operation just forwards it unused.
//
// `filter`, when given, is a promise from the caller: it will intersect what
// comes back with filter's documents, so the result may leave out any
// document filter does not name. It is how an intersection hands its most
// selective operand to the others -- `leviathan (the | and)` then answers
// the Or at leviathan's 40 documents instead of unioning two lists of
// 240,000 and throwing nearly all of it away. It is only ever a hint: a node
// with no cheaper way to honor it answers in full, which is always correct.
inline std::shared_ptr<IPostings>
perform_search_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr, const IScopeIndex *scope_index,
                         const IPostings *filter = nullptr);

inline auto positings_list(const IInvertedIndex &inverted_index,
                           const std::vector<Expression> &nodes,
                           const IScopeIndex *scope_index,
                           const IPostings *filter = nullptr) {
  std::vector<std::shared_ptr<IPostings>> positings_list;
  for (const auto &expr : nodes) {
    positings_list.push_back(
        perform_search_operation(inverted_index, expr, scope_index, filter));
  }
  return positings_list;
}

// An upper bound on how many documents expr can match, cheap enough to take
// before evaluating it: a Term's df, the smallest operand of an
// intersection, the sum of a union's. It only picks which operand of an
// intersection runs first and filters the rest, so it has to rank well, not
// be exact. Prefix, Wildcard and Fuzzy would have to expand against the
// dictionary to answer, and evaluating them expands again, so they rank last
// rather than pay that twice.
inline size_t estimate_matches(const IInvertedIndex &inverted_index,
                               const Expression &expr) {
  constexpr auto unknown = std::numeric_limits<size_t>::max();
  switch (expr.operation) {
  case Operation::Term:
    return inverted_index.df(expr.term_str);
  case Operation::And:
  case Operation::Adjacent:
  case Operation::Near:
  case Operation::SameScope: {
    auto bound = unknown;
    for (const auto &node : expr.nodes) {
      if (node.operation != Operation::Not) {
        bound = std::min(bound, estimate_matches(inverted_index, node));
      }
    }
    return bound;
  }
  case Operation::Or: {
    size_t bound = 0;
    for (const auto &node : expr.nodes) {
      auto matches = estimate_matches(inverted_index, node);
      if (matches > unknown - bound) {
        return unknown;
      }
      bound += matches;
    }
    return bound;
  }
  default:
    return unknown;
  }
}

// The tighter of two filters, either of which may be absent.
inline const IPostings *tighter_filter(const IPostings *a, const IPostings *b) {
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  return b->size() < a->size() ? b : a;
}

// The operands of an intersection, evaluated, plus the filter they were
// evaluated under.
struct IntersectionOperands {
  std::vector<std::shared_ptr<IPostings>> postings;
  const IPostings *filter;
};

// Evaluates an intersection's operands, the most selective first, and hands
// each later one the tighter of the caller's filter and the first one's
// result, since the intersection keeps only documents both of those name.
// A Term costs nothing to evaluate -- its result is its own postings -- so
// Terms are taken first and ranked by their real size; anything else is
// ranked by estimate_matches before it runs. The operands come back in the
// order given, because Adjacent and Near read them by slot.
inline IntersectionOperands
intersection_operands(const IInvertedIndex &inverted_index,
                      const std::vector<Expression> &nodes,
                      const IScopeIndex *scope_index, const IPostings *filter) {
  IntersectionOperands operands{
      std::vector<std::shared_ptr<IPostings>>(nodes.size()), filter};
  auto &postings = operands.postings;

  auto lead = nodes.size();
  auto lead_matches = std::numeric_limits<size_t>::max();
  for (size_t i = 0; i < nodes.size(); i++) {
    size_t matches;
    if (nodes[i].operation == Operation::Term) {
      postings[i] =
          perform_search_operation(inverted_index, nodes[i], scope_index);
      matches = postings[i]->size();
    } else {
      matches = estimate_matches(inverted_index, nodes[i]);
    }
    if (lead == nodes.size() || matches < lead_matches) {
      lead = i;
      lead_matches = matches;
    }
  }
  if (lead == nodes.size()) {
    return operands;
  }

  if (!postings[lead]) {
    postings[lead] = perform_search_operation(inverted_index, nodes[lead],
                                              scope_index, filter);
  }
  operands.filter = tighter_filter(filter, postings[lead].get());
  for (size_t i = 0; i < nodes.size(); i++) {
    if (!postings[i]) {
      postings[i] = perform_search_operation(inverted_index, nodes[i],
                                             scope_index, operands.filter);
    }
  }
  return operands;
}

// Defined below, after the windows; OrdinalWindows::advance gallops through
// it for a slot without a window.
template <typename P>
inline size_t gallop_lower_bound(P &postings, size_t cursor, size_t size,
                                 size_t ordinal);

// Windows over the operands of one walk. The walks read a cursor's ordinal,
// compare it, and then step the cursor by one -- which through the virtual
// interface is one Elias-Fano select per read, and select restarts its
// superblock search every time. Reading a block instead turns a run of those
// into one call (measured 21.5ns -> 1.7ns per ordinal on the compressed
// backend).
//
// The blocks live in one scratch allocation held here, and only operands that
// say they gain from block reads get one. That keeps a walk over vector-backed
// postings -- where no operand does, because addressing one value is already a
// single load -- carrying no buffers and no extra cache line per operand: an
// earlier version buffered unconditionally and made an in-memory union 13%
// slower to make a compressed one 4x faster.
class OrdinalWindows {
public:
  explicit OrdinalWindows(
      const std::vector<std::shared_ptr<IPostings>> &positings_list) {
    size_t buffered = 0;
    for (const auto &postings : positings_list) {
      buffered += postings->prefers_block_reads() ? 1 : 0;
    }

    // The operand pointers stay in a dense array of their own: the direct
    // read walks it once per output document, and packing eight to a cache
    // line rather than interleaving it with block state is worth more than
    // the second indirection costs.
    postings_.reserve(positings_list.size());
    for (const auto &postings : positings_list) {
      postings_.push_back(postings.get());
    }
    if (buffered == 0) {
      return;
    }

    scratch_.resize(buffered * kWindow);
    blocks_.resize(positings_list.size());
    size_t next = 0;
    for (size_t slot = 0; slot < positings_list.size(); slot++) {
      if (positings_list[slot]->prefers_block_reads()) {
        blocks_[slot].buffer = scratch_.data() + next;
        next += kWindow;
      }
    }
  }

  // Requires index < the operand's size(), same as document_ordinal, and that
  // reads on one slot move forward.
  size_t at(size_t slot_index, size_t index) {
    auto &block = blocks_[slot_index];
    if (block.buffer == nullptr) {
      return postings_[slot_index]->document_ordinal(index);
    }
    if (index >= block.base && index - block.base < block.filled) {
      return block.buffer[index - block.base];
    }
    // A miss that continues where the window ended is a walk, so refill a
    // block. Any other miss is a jump -- a gallop probe, an operand skipping
    // ahead -- and a block read there decodes values nothing will ask for.
    auto count = index == block.base + block.filled ? kWindow : size_t(1);
    block.filled =
        postings_[slot_index]->read_ordinals(index, block.buffer, count);
    block.base = index;
    return block.buffer[0];
  }

  // The same read for a walk where no slot buffers. The walks pick between
  // this and at() once, outside their loop: leaving the test inside cost an
  // in-memory union 27%, which is more than buffering saves where it does
  // not apply.
  size_t direct(size_t slot_index, size_t index) const {
    return postings_[slot_index]->document_ordinal(index);
  }

  // The first index after `cursor` whose ordinal is >= ordinal, or `size`:
  // IPostings::advance for a slot of the walk. Requires at(slot, cursor) <
  // ordinal. A buffered slot looks in its window first -- a skip of a few
  // documents lands inside the block already read, at no call at all --
  // and past it asks the operand, then refills from where that landed, so
  // the read that follows a skip is a hit rather than another select. A
  // slot without a window gallops, as skip_cursors always has.
  size_t advance(size_t slot_index, size_t cursor, size_t size,
                 size_t ordinal) {
    auto *postings = postings_[slot_index];
    auto &block = blocks_[slot_index];
    if (block.buffer == nullptr) {
      return gallop_lower_bound(*postings, cursor, size, ordinal);
    }
    auto end = block.base + block.filled;
    for (auto i = cursor + 1; i < end; i++) {
      if (block.buffer[i - block.base] >= ordinal) {
        return i;
      }
    }
    if (end >= size) {
      return size;
    }
    // Everything up to `end` is known to be below `ordinal`, so the
    // operand resumes from the last of it.
    auto index = postings->advance(std::max(cursor, end - 1), ordinal);
    block.filled = postings->read_ordinals(index, block.buffer, kWindow);
    block.base = index;
    return index;
  }

  // False when no operand asked for blocks.
  bool buffered() const { return !blocks_.empty(); }
  // Whether this one operand did.
  bool buffered(size_t slot_index) const {
    return !blocks_.empty() && blocks_[slot_index].buffer != nullptr;
  }

  // Kept in step with the operand list the union walk prunes.
  void erase(size_t slot_index) {
    postings_.erase(postings_.begin() + slot_index);
    if (!blocks_.empty()) {
      blocks_.erase(blocks_.begin() + slot_index);
    }
  }

  size_t size() const { return postings_.size(); }

private:
  static constexpr size_t kWindow = 8;

  struct Block {
    size_t *buffer = nullptr;
    size_t base = 0;
    size_t filled = 0;
  };

  std::vector<const IPostings *> postings_;
  std::vector<Block> blocks_; // empty when nothing buffers
  std::vector<size_t> scratch_;
};

template <typename Read>
inline size_t min_slots_scan(size_t slot_count, Read read,
                             std::vector<size_t> &slots) {
  slots.clear();
  slots.push_back(0);

  // The running minimum only changes when a smaller id resets `slots`, so it
  // lives in a local instead of being re-read through the virtual interface
  // on every iteration (this runs once per output document of every union).
  auto prev = read(0);
  for (size_t slot = 1; slot < slot_count; slot++) {
    auto curr = read(slot);

    if (curr < prev) {
      slots.clear();
      slots.push_back(slot);
      prev = curr;
    } else if (curr == prev) {
      slots.push_back(slot);
    }
  }

  return prev;
}



template <typename Read>
inline std::pair<size_t /*min*/, size_t /*max*/>
min_max_slots_scan(size_t slot_count, Read read) {
  auto min = read(0);
  auto max = min;

  for (size_t slot = 1; slot < slot_count; slot++) {
    auto id = read(slot);
    if (id < min) {
      min = id;
    } else if (id > max) {
      max = id;
    }
  }

  return std::make_pair(min, max);
}



// First index in [low, high) whose ordinal is >= ordinal. A template over
// the operand, so that a PostingsWindow serves as well as an IPostings.
template <typename P>
inline size_t lower_bound_ordinal(P &postings, size_t low,
                                      size_t high, size_t ordinal) {
  while (low < high) {
    auto mid = low + (high - low) / 2;
    if (postings.document_ordinal(mid) < ordinal) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }
  return low;
}

// First index in (cursor, size] whose ordinal is >= ordinal, found by
// galloping forward from cursor: O(log gap) accesses instead of a linear
// scan's O(gap), which pays off when an AND operand skips far ahead, and
// instead of a plain binary search's O(log size), which the scorer's
// step-or-two advances would not amortize. This is IPostings::advance's
// default, right for an operand whose document_ordinal(index) is O(1) random
// access; Elias-Fano, where each probe is a select, overrides advance with
// its own next_geq instead. Requires postings.document_ordinal(cursor) <
// ordinal.
template <typename P>
inline size_t gallop_lower_bound(P &postings, size_t cursor, size_t size,
                                 size_t ordinal) {
  size_t step = 1;
  auto low = cursor + 1; // document_ordinal(cursor) is known to be < target
  auto high = cursor + step;
  while (high < size && postings.document_ordinal(high) < ordinal) {
    low = high + 1;
    step *= 2;
    high = cursor + step;
  }
  if (high > size) {
    high = size;
  }
  // `high` itself is either a known match or `size`.
  return lower_bound_ordinal(postings, low, high, ordinal);
}

// find_postings_index_for_ordinal, but resuming from where the previous
// lookup landed. Every caller walks documents in ascending id order, so the
// answer is usually at or just past the cursor. `size` is the postings size,
// resolved once by the caller rather than re-fetched through the virtual
// interface on every lookup.
//
// Both `cursor` and `last_ordinal` are updated so that the invariant
// "cursor is the first entry whose document id is >= last_ordinal" holds
// on entry and on exit. That invariant carries the whole optimization: when
// the walk is moving forward, everything before the cursor is already known
// to be below the target, so a cursor sitting past the target proves the
// document is absent without any search at all. Returns `size` when the
// document is absent.
//
// The inlining is pinned rather than left to the compiler. Both callers are
// hot loops -- BM25Scorer's per-hit term lookup and LazyMergeResult's
// per-method operand lookup -- and with one caller clang inlined this on its
// own. Adding the second made it decide otherwise, turning the scorer's
// 172-instruction body into a 68-instruction one plus a call and costing 22%
// of the scoring phase of a two-term Or, on a change that touched no scoring
// code at all.
//
// `block_reads` is the operand's prefers_block_reads(), resolved once by the
// caller like `size`: it picks the operand's own advance (Elias-Fano's
// next_geq) over the inline gallop. A vector operand keeps the gallop and
// pays no virtual call for it -- routing it through advance cost a two-term
// Or's scoring 20%.
template <typename P>
SEARCHLIB_ALWAYS_INLINE size_t
find_from_cursor(P &postings, size_t size, bool block_reads, size_t &cursor,
                 size_t &last_ordinal, size_t ordinal) {
  if (size == 0) {
    return 0;
  }

  if (ordinal < last_ordinal) {
    // The caller is walking out of ascending order, so the invariant says
    // nothing about entries before the cursor and they have to be searched.
    // Galloping forward would never find the target either. The clamp
    // matters: an exhausted cursor sits at size, and cursor + 1 would run
    // the search one entry past the end.
    auto high = std::min(cursor + 1, size);
    cursor = lower_bound_ordinal(postings, 0, high, ordinal);
  } else if (cursor < size && postings.document_ordinal(cursor) < ordinal) {
    cursor = block_reads ? postings.advance(cursor, ordinal)
                         : gallop_lower_bound(postings, cursor, size, ordinal);
  }
  // Otherwise the cursor is already the lower bound for this document: it
  // either sits on it, or sits past it (absent), or the list is exhausted.
  last_ordinal = ordinal;

  return (cursor < size && postings.document_ordinal(cursor) == ordinal) ? cursor
                                                                       : size;
}

//-----------------------------------------------------------------------------

// Result of an And/Or operation: the document ids that matched, viewed over
// the operand postings they matched in.
//
// Positions are never copied out. BM25 ranking reads only ordinal from a
// result -- it takes each term's frequency from that term's own postings --
// and the consumers that do read positions (text_range for highlighting, a
// nested Adjacent/Near) touch a handful of documents each. Building them all
// up front, the way SearchResult does, was measured at 37% of the search
// phase of a two-term And and 57% of a two-term Or, nearly all of it thrown
// away. This is the same move that made a bare-term result stop wrapping the
// term's postings, one level up.
//
// What a view costs instead is finding the document again in each operand.
// Two things keep that from being a search:
//
//   - One forward cursor per operand, exactly BM25Scorer::TermState's
//     mechanism: every consumer walks a result in ascending index order, so
//     locating a document resumes from the previous one. Without it a full
//     walk would pay k binary searches per document.
//   - A one-row memo, because a document's hits are always read together: a
//     term_position(index, 0..n) run costs one merge rather than n.
//
// Only term_position/term_length need that merge at all; size, ordinal,
// search_hit_count and is_term_position are answered from the operands
// directly.
//
// Unlike SearchResult, this aliases its operands rather than owning a
// snapshot of them, so it stays valid only as long as the index does -- the
// same lifetime a bare-term result has always had. See the note on
// perform_search up in the interface.
class LazyMergeResult : public IPostings {
public:
  explicit LazyMergeResult(std::vector<std::shared_ptr<IPostings>> operands)
      : operands_(std::move(operands)), cursors_(operands_.size()) {
    for (size_t slot = 0; slot < operands_.size(); slot++) {
      cursors_[slot].size = operands_[slot]->size();
      cursors_[slot].block_reads = operands_[slot]->prefers_block_reads();
    }
  }

  ~LazyMergeResult() override = default;

  // Appends one matched document. Callers append in ascending id order,
  // which is the order the operand cursors are built to exploit.
  void push_back(size_t ordinal) { ordinals_.push_back(ordinal); }

  size_t size() const override { return ordinals_.size(); }

  size_t document_ordinal(size_t index) const override {
    return ordinals_[index];
  }

  // The sum of the operands' counts, no merge needed: merging reorders the
  // hits, it never adds or drops one.
  size_t search_hit_count(size_t index) const override {
    locate_row(index);
    size_t count = 0;
    for (const auto &row : row_slots_) {
      count += row.hit_count;
    }
    return count;
  }

  size_t term_position(size_t index, size_t search_hit_index) const override {
    merge_row(index);
    return row_positions_[search_hit_index];
  }

  size_t term_length(size_t index, size_t search_hit_index) const override {
    merge_row(index);
    return row_lengths_[search_hit_index];
  }

  // A merged row is the union of the operands' positions as a *set*, so
  // membership is answerable operand by operand -- again without merging.
  // Worth keeping off the memo path because Adjacent asks this once per
  // candidate position of its shortest operand.
  bool is_term_position(size_t index, size_t term_pos) const override {
    locate_row(index);
    for (size_t slot = 0; slot < operands_.size(); slot++) {
      const auto &row = row_slots_[slot];
      if (row.hit_count > 0 &&
          operands_[slot]->is_term_position(row.index, term_pos)) {
        return true;
      }
    }
    return false;
  }

private:
  static constexpr size_t kNone = static_cast<size_t>(-1);

  // Where this operand's entry for the last document asked about sits, and
  // which document that was; see find_from_cursor for the invariant tying
  // the two together. `size` is the operand's size, resolved once here
  // rather than re-fetched through the virtual interface per lookup.
  struct OperandCursor {
    size_t size = 0;
    bool block_reads = false;
    size_t cursor = 0;
    size_t last_ordinal = 0;
  };

  // One operand's state for the row being merged: where its entry for this
  // document sits, how many hits that entry has, and how many of them the
  // merge has already emitted.
  struct RowSlot {
    size_t index;
    size_t hit_count;
    size_t hit_cursor;
  };

  // This operand's index for `ordinal`, or its size if the operand does
  // not carry the document -- possible under Or, never under And.
  size_t locate(size_t slot, size_t ordinal) const {
    auto &cursor = cursors_[slot];
    return find_from_cursor(*operands_[slot], cursor.size, cursor.block_reads,
                            cursor.cursor, cursor.last_ordinal, ordinal);
  }

  // Resolves every operand's entry for one document, and its hit count,
  // into row_slots_ -- unless they are already there. Every method that
  // reads hits needs exactly this, and a consumer touches one document many
  // times in a row (Adjacent probes one position at a time), so it is
  // memoized on its own rather than recomputed per call or folded into the
  // merge that only term_position/term_length need.
  void locate_row(size_t index) const {
    if (located_index_ == index) {
      return;
    }

    // Each memo is dropped before the state it names is touched, not after
    // the refill finishes: everything in between is a virtual call into an
    // operand, so "this window cannot end early" would be an assumption
    // about other IPostings implementations rather than a local fact.
    located_index_ = kNone;
    row_slots_.clear();
    for (size_t slot = 0; slot < operands_.size(); slot++) {
      auto i = locate(slot, ordinals_[index]);
      auto count =
          i < cursors_[slot].size ? operands_[slot]->search_hit_count(i) : 0;
      row_slots_.push_back(RowSlot{i, count, 0});
    }
    located_index_ = index;
  }

  // Merges one document's hits into row_positions_/row_lengths_, unless they
  // are already there. Ascending by position with ties going to the earlier
  // operand: the order the eager merge emitted, which the operations that
  // consume positions (and a materialized row's binary search) relied on.
  void merge_row(size_t index) const {
    if (cached_index_ == index) {
      return;
    }

    auto slot_count = operands_.size();
    cached_index_ = kNone; // see locate_row
    row_positions_.clear();
    row_lengths_.clear();
    locate_row(index);
    for (auto &row : row_slots_) {
      row.hit_cursor = 0;
    }

    while (true) {
      auto min_slot = kNone;
      size_t min_term_pos = kNone;
      size_t min_term_length = 0;

      for (size_t slot = 0; slot < slot_count; slot++) {
        const auto &row = row_slots_[slot];
        if (row.hit_cursor == row.hit_count) {
          continue;
        }

        // By reference: copying the shared_ptr costs an atomic increment and
        // decrement, and this is the innermost loop of the merge.
        const auto &p = operands_[slot];

        auto term_pos = p->term_position(row.index, row.hit_cursor);
        if (term_pos < min_term_pos) {
          min_slot = slot;
          min_term_pos = term_pos;
          min_term_length = p->term_length(row.index, row.hit_cursor);
        }
      }

      if (min_slot == kNone) {
        break;
      }

      row_positions_.push_back(min_term_pos);
      row_lengths_.push_back(min_term_length);
      row_slots_[min_slot].hit_cursor++;
    }

    cached_index_ = index;
  }

  std::vector<size_t> ordinals_;
  std::vector<std::shared_ptr<IPostings>> operands_;

  mutable std::vector<OperandCursor> cursors_;

  // The memoized row and the scratch the merge that produced it used.
  mutable size_t located_index_ = kNone;
  mutable size_t cached_index_ = kNone;
  mutable std::vector<size_t> row_positions_;
  mutable std::vector<size_t> row_lengths_;
  mutable std::vector<RowSlot> row_slots_;
};

//-----------------------------------------------------------------------------

// Moves every cursor below `ordinal` up to it, reading and skipping through
// what the walk hands it (its windows, or the operands directly -- the same
// split as min_max_slots_scan's `read`). True when a cursor ran out.
template <typename Read, typename Advance>
inline bool skip_cursors(const std::vector<size_t> &sizes,
                         std::vector<size_t> &cursors, Read read,
                         Advance advance, size_t ordinal) {
  for (size_t slot = 0; slot < sizes.size(); slot++) {
    auto &cursor = cursors[slot];
    auto size = sizes[slot];

    if (cursor < size && read(slot, cursor) < ordinal) {
      cursor = advance(slot, cursor, size, ordinal);
    }

    if (cursor == size) {
      return true;
    }
  }
  return false;
}

inline bool increment_all_cursors(const std::vector<size_t> &sizes,
                                  std::vector<size_t> &cursors) {
  for (size_t slot = 0; slot < sizes.size(); slot++) {
    cursors[slot]++;
    if (cursors[slot] == sizes[slot]) {
      return true;
    }
  }
  return false;
}

inline void
increment_cursors(std::vector<std::shared_ptr<IPostings>> &positings_list,
                  std::vector<size_t> &sizes, std::vector<size_t> &cursors,
                  OrdinalWindows &windows,
                  const std::vector<size_t> &slots) {
  for (int i = slots.size() - 1; i >= 0; i--) {
    auto slot = slots[i];
    cursors[slot]++;
    if (cursors[slot] == sizes[slot]) {
      cursors.erase(cursors.begin() + slot);
      sizes.erase(sizes.begin() + slot);
      positings_list.erase(positings_list.begin() + slot);
      windows.erase(slot);
    }
  }
}

// The document-id walk shared by every intersecting operation: advances the
// cursors in ascending document id order and calls fn(cursors, ordinal)
// once for each document that appears in all of them, with every cursor
// parked on that document so fn can read the operands' hits without
// searching for it again. Only the cursors are handed out: they live inside
// this walk, while every caller already holds the list it passed in.
//
// Separate from intersect_postings because not every And-shaped operation
// wants a materialized result: an And only needs the ids, while
// Adjacent/Near/SameScope also synthesize positions per document.
template <typename T>
inline void for_each_intersection(
    const std::vector<std::shared_ptr<IPostings>> &positings_list, T fn) {
  if (positings_list.empty()) {
    return;
  }

  // An empty postings list never intersects with others. The sizes are
  // resolved once here rather than re-fetched through the virtual interface
  // by every cursor advance of every iteration.
  std::vector<size_t> sizes;
  sizes.reserve(positings_list.size());
  for (const auto &postings : positings_list) {
    auto size = postings->size();
    if (size == 0) {
      return;
    }
    sizes.push_back(size);
  }

  std::vector<size_t> cursors(positings_list.size(), 0);
  OrdinalWindows windows(positings_list);

  // Same reason as the union walk above for writing this twice.
  auto done = false;
  if (windows.buffered()) {
    auto read = [&](size_t slot, size_t index) { return windows.at(slot, index); };
    auto advance = [&](size_t slot, size_t cursor, size_t size, size_t ordinal) {
      return windows.advance(slot, cursor, size, ordinal);
    };
    while (!done) {
      auto [min, max] = min_max_slots_scan(
          windows.size(), [&](size_t slot) { return read(slot, cursors[slot]); });
      if (min == max) {
        fn(cursors, min);
        done = increment_all_cursors(sizes, cursors);
      } else {
        done = skip_cursors(sizes, cursors, read, advance, max);
      }
    }
  } else {
    auto read = [&](size_t slot, size_t index) {
      return windows.direct(slot, index);
    };
    auto advance = [&](size_t slot, size_t cursor, size_t size, size_t ordinal) {
      return gallop_lower_bound(*positings_list[slot], cursor, size, ordinal);
    };
    while (!done) {
      auto [min, max] = min_max_slots_scan(
          windows.size(), [&](size_t slot) { return read(slot, cursors[slot]); });
      if (min == max) {
        fn(cursors, min);
        done = increment_all_cursors(sizes, cursors);
      } else {
        done = skip_cursors(sizes, cursors, read, advance, max);
      }
    }
  }
}

// positings_list with the filter appended as one more, non-owning, last slot:
// the caller holds the filter for the whole walk, and the cast is as safe as
// perform_term_operation's -- IPostings is all-const.
inline std::vector<std::shared_ptr<IPostings>>
with_filter_slot(const std::vector<std::shared_ptr<IPostings>> &positings_list,
                 const IPostings &filter) {
  auto walk = positings_list;
  walk.emplace_back(std::shared_ptr<IPostings>(),
                    const_cast<IPostings *>(&filter));
  return walk;
}

// for_each_intersection under a caller's filter (see perform_search_operation).
// When the filter is more selective than every operand, it joins the walk as
// one more operand and leads it: `rare "of the"` then checks the phrase at
// rare's documents rather than at every document the two words share. It
// goes last, so the operands keep their slots and fn, which reads only
// those, never sees it. A less selective filter is left out, since it would
// only add a cursor to every step.
template <typename T>
inline void for_each_filtered_intersection(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const IPostings *filter, T fn) {
  if (filter && !positings_list.empty() &&
      std::all_of(positings_list.begin(), positings_list.end(),
                  [&](const auto &p) { return filter->size() < p->size(); })) {
    for_each_intersection(with_filter_slot(positings_list, *filter), fn);
  } else {
    for_each_intersection(positings_list, fn);
  }
}

template <typename T>
inline std::shared_ptr<IPostings> intersect_postings(
    const std::vector<std::shared_ptr<IPostings>> &positings_list,
    const IPostings *filter, T make_positions) {
  auto result = std::make_shared<SearchResult>();

  // Filled and cleared once per matched document rather than reallocated:
  // make_positions appends into these and reports whether the document
  // survived, and push_back leaves them empty again.
  std::vector<size_t> term_positions;
  std::vector<size_t> term_lengths;

  for_each_filtered_intersection(
      positings_list, filter, [&](const auto &cursors, size_t ordinal) {
        if (make_positions(positings_list, cursors, ordinal, term_positions,
                           term_lengths)) {
          result->push_back(ordinal, term_positions, term_lengths);
        } else {
          term_positions.clear();
          term_lengths.clear();
        }
      });

  return result;
}

inline std::shared_ptr<IPostings>
union_postings(std::vector<std::shared_ptr<IPostings>> &&positings_list) {
  // The result views every operand, so it takes its own copy of the list
  // before the walk below starts dropping the ones that run out
  // (increment_cursors erases them). Built from the leftovers instead, it
  // would answer with the positions of only the terms that survived to the
  // end. The copy is k shared_ptrs, once per query.
  auto result = std::make_shared<LazyMergeResult>(positings_list);

  std::vector<size_t> cursors(positings_list.size(), 0);
  // Resolved once, then kept in step with the list as operands are dropped:
  // the walk's exhaustion check runs once per slot per output document.
  std::vector<size_t> sizes;
  sizes.reserve(positings_list.size());
  for (const auto &postings : positings_list) {
    sizes.push_back(postings->size());
  }
  std::vector<size_t> slots; // reused across documents
  OrdinalWindows windows(positings_list);

  // Two loops rather than one with the read behind a lambda or a per-document
  // test: this is the union's innermost loop, and every attempt to share it
  // cost the unbuffered path 12% to 40%. The buffered one is entered only
  // when an operand actually gains from block reads.
  if (windows.buffered()) {
    while (!positings_list.empty()) {
      result->push_back(min_slots_scan(
          windows.size(),
          [&](size_t slot) { return windows.at(slot, cursors[slot]); },
          slots));
      increment_cursors(positings_list, sizes, cursors, windows, slots);
      assert(positings_list.size() == cursors.size());
    }
  } else {
    while (!positings_list.empty()) {
      result->push_back(min_slots_scan(
          windows.size(),
          [&](size_t slot) { return windows.direct(slot, cursors[slot]); },
          slots));
      increment_cursors(positings_list, sizes, cursors, windows, slots);
      assert(positings_list.size() == cursors.size());
    }
  }

  return result;
}

// Whether to answer a union under a filter by probing (probe_union_postings
// below) rather than walking every operand end to end: when the filter's
// documents times the operands, the most advances probing can take, is
// fewer than the operands' entries, which the full union reads one by one.
// Probing stops at the first operand that names a document, so it usually
// takes fewer. On KJV x10, `X (the | and)` probed beats the full union at
// every X up to `of` (181,220 against 479,590 entries), taking 0.59x its
// time on the in-memory backend and 0.92x on the compressed one, which is
// where this line sits.
inline bool probing_pays(const IPostings &filter,
                         const std::vector<std::shared_ptr<IPostings>> &list) {
  size_t total = 0;
  for (const auto &postings : list) {
    total += postings->size();
  }
  return filter.size() * list.size() < total;
}

inline std::shared_ptr<IPostings>
probe_union_postings(std::vector<std::shared_ptr<IPostings>> &&positings_list,
                     const IPostings &filter) {
  auto result = std::make_shared<LazyMergeResult>(positings_list);
  if (positings_list.empty()) {
    return result;
  }

  // Everything is read through windows, the filter included as the last
  // slot, for the same reason the other walks are: on Elias-Fano a
  // document_ordinal or an advance through the virtual interface restarts
  // its search every time. Probing a filter of 38,770 against two operands
  // that way took 12.7ms, slower than the 11.2ms full union it was meant to
  // beat; through windows it takes 4.8ms.
  auto operands = positings_list.size();
  auto walk = with_filter_slot(positings_list, filter);
  std::vector<size_t> sizes;
  sizes.reserve(walk.size());
  for (const auto &postings : walk) {
    sizes.push_back(postings->size());
  }
  std::vector<size_t> cursors(operands, 0);
  OrdinalWindows windows(walk);

  // A cursor is only ever a lower bound, so stopping at the first operand
  // that names the document leaves the others where advance can resume.
  auto probe = [&](auto read, auto advance) {
    for (size_t i = 0; i < sizes[operands]; i++) {
      auto ordinal = read(operands, i);
      for (size_t slot = 0; slot < operands; slot++) {
        auto &cursor = cursors[slot];
        if (cursor < sizes[slot] && read(slot, cursor) < ordinal) {
          cursor = advance(slot, cursor, sizes[slot], ordinal);
        }
        if (cursor < sizes[slot] && read(slot, cursor) == ordinal) {
          result->push_back(ordinal);
          break;
        }
      }
    }
  };
  // Instantiated once per read, as the other walks write their loop twice.
  if (windows.buffered()) {
    probe([&](size_t slot, size_t index) { return windows.at(slot, index); },
          [&](size_t slot, size_t cursor, size_t size, size_t ordinal) {
            return windows.advance(slot, cursor, size, ordinal);
          });
  } else {
    probe(
        [&](size_t slot, size_t index) { return windows.direct(slot, index); },
        [&](size_t slot, size_t cursor, size_t size, size_t ordinal) {
          return gallop_lower_bound(*walk[slot], cursor, size, ordinal);
        });
  }

  return result;
}

//-----------------------------------------------------------------------------

inline std::shared_ptr<IPostings>
perform_term_operation(const IInvertedIndex &inverted_index,
                       const Expression &expr) {
  // Handed back directly rather than wrapped. A bare-term result *is* the
  // term's postings -- every IPostings method, term_length included, already
  // answers the way a Term node should -- so a forwarding wrapper would only
  // put a second virtual dispatch in front of every access on the And, Or and
  // phrase paths. Measured at 40% on a scan of a high-df term.
  //
  // The const_pointer_cast is safe: IPostings is an all-const interface and
  // nothing writes through the pointer. It exists only because perform_search
  // returns a non-const shared_ptr. Lifetime is unchanged -- this is the same
  // pointer the wrapper used to hold, non-owning or not (see
  // IInvertedIndex::postings).
  return std::const_pointer_cast<IPostings>(
      inverted_index.postings(expr.term_str));
}

inline std::shared_ptr<IPostings>
perform_and_operation(const IInvertedIndex &inverted_index,
                      const Expression &expr, const IScopeIndex *scope_index,
                      const IPostings *filter) {
  std::vector<Expression> positive_nodes;
  std::vector<Expression> negative_nodes;
  for (const auto &node : expr.nodes) {
    if (node.operation == Operation::Not) {
      negative_nodes.push_back(node.nodes[0]);
    } else {
      positive_nodes.push_back(node);
    }
  }

  auto positive = intersection_operands(inverted_index, positive_nodes,
                                        scope_index, filter);
  const auto &positive_postings_list = positive.postings;

  // A negative operand is only ever asked about documents every positive one
  // names, so it can take the same filter they did.
  auto negative_postings_list = positings_list(inverted_index, negative_nodes,
                                               scope_index, positive.filter);
  std::vector<size_t> negative_cursors(negative_postings_list.size(), 0);

  auto result = std::make_shared<LazyMergeResult>(positive_postings_list);

  for_each_filtered_intersection(
      positive_postings_list, filter, [&](const auto &, size_t ordinal) {
        // Exclude documents that appear in any negative postings. Both sides
        // are iterated in ascending document id order, so a negative cursor
        // only moves forward -- and it jumps rather than steps, because the
        // positive side is typically the sparse one (`rare -common`), where
        // stepping walked the whole negative list: 494us -> 2us on KJV x10.
        for (size_t slot = 0; slot < negative_postings_list.size(); slot++) {
          const auto &p = negative_postings_list[slot];
          auto &cursor = negative_cursors[slot];
          auto size = p->size();
          if (cursor < size && p->document_ordinal(cursor) < ordinal) {
            cursor = p->advance(cursor, ordinal);
          }
          if (cursor < size && p->document_ordinal(cursor) == ordinal) {
            return;
          }
        }

        result->push_back(ordinal);
      });

  return result;
}

inline std::shared_ptr<IPostings> perform_adjacent_operation(
    const IInvertedIndex &inverted_index, const Expression &expr,
    const IScopeIndex *scope_index, const IPostings *filter) {
  // Every operand's positions for the document, read whole
  // (read_term_positions) into buffers reused across documents. The check
  // is then a merge: the shortest list leads, and each other list's pointer
  // only moves forward, since positions ascend -- instead of probing one
  // position at a time, which on Elias-Fano was a bucket lookup per probe.
  std::vector<std::vector<size_t>> positions;
  std::vector<size_t> pointers;

  return intersect_postings(
      intersection_operands(inverted_index, expr.nodes, scope_index, filter)
          .postings,
      filter,
      [&](const auto &positings_list, const auto &cursors, size_t /*ordinal*/,
          auto &term_positions, auto &term_lengths) {
        auto slot_count = positings_list.size();
        positions.resize(slot_count);
        size_t target_slot = 0;
        for (size_t slot = 0; slot < slot_count; slot++) {
          const auto &p = positings_list[slot];
          auto &list = positions[slot];
          list.resize(p->search_hit_count(cursors[slot]));
          p->read_term_positions(cursors[slot], list.data());
          if (list.size() < positions[target_slot].size()) {
            target_slot = slot;
          }
        }
        pointers.assign(slot_count, 0);

        for (auto term_pos : positions[target_slot]) {
          auto adjacent = true;
          for (size_t slot = 0; adjacent && slot < slot_count; slot++) {
            if (slot == target_slot) {
              continue;
            }
            // The position this operand has to hold, term_pos + (slot -
            // target_slot). Below zero it is nowhere, and the pointer stays
            // put: a later term_pos may still land in this list.
            size_t wanted;
            if (slot > target_slot) {
              wanted = term_pos + (slot - target_slot);
            } else if (term_pos < target_slot - slot) {
              adjacent = false;
              break;
            } else {
              wanted = term_pos - (target_slot - slot);
            }
            const auto &list = positions[slot];
            auto &i = pointers[slot];
            while (i < list.size() && list[i] < wanted) {
              i++;
            }
            adjacent = i < list.size() && list[i] == wanted;
          }
          if (adjacent) {
            term_positions.push_back(term_pos - target_slot);
            term_lengths.push_back(slot_count);
          }
        }

        return !term_positions.empty();
      });
}

inline std::shared_ptr<IPostings>
perform_or_operation(const IInvertedIndex &inverted_index,
                     const Expression &expr, const IScopeIndex *scope_index,
                     const IPostings *filter) {
  auto operands =
      positings_list(inverted_index, expr.nodes, scope_index, filter);
  // Both unions below take the operands with the empty ones already gone.
  operands.erase(std::remove_if(operands.begin(), operands.end(),
                                [](const auto &postings) {
                                  return postings->size() == 0;
                                }),
                 operands.end());
  if (filter && probing_pays(*filter, operands)) {
    return probe_union_postings(std::move(operands), *filter);
  }
  return union_postings(std::move(operands));
}

// A Prefix node is answered by expanding it against the index's dictionary
// and unioning the matching terms, i.e. `foo*` behaves exactly like an Or
// over every term starting with `foo`. The expansion happens at search time
// rather than in parse_query because the parser has no index to consult, and
// a parsed Expression is meant to stay reusable across indexes.
inline std::shared_ptr<IPostings>
perform_prefix_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr, const IScopeIndex *scope_index,
                         const IPostings *filter) {
  return perform_search_operation(inverted_index,
                                  expand_prefixes(inverted_index, expr),
                                  scope_index, filter);
}

// A Wildcard node is answered the same way a Prefix node is: expand against
// the dictionary and union the matches. Kept as its own operation (rather
// than folding into Prefix) because the two backends answer them with
// different mechanisms -- literal-prefix descent vs. an automaton walk -- and
// mixing that behind one enumerate_terms_with_prefix call would lose the
// cheaper path for the common trailing-`*` case.
inline std::shared_ptr<IPostings> perform_wildcard_operation(
    const IInvertedIndex &inverted_index, const Expression &expr,
    const IScopeIndex *scope_index, const IPostings *filter) {
  return perform_search_operation(inverted_index,
                                  expand_wildcards(inverted_index, expr),
                                  scope_index, filter);
}

// And likewise for Fuzzy, which differs from the two above only in which
// dictionary enumeration it expands through.
inline std::shared_ptr<IPostings>
perform_fuzzy_operation(const IInvertedIndex &inverted_index,
                        const Expression &expr, const IScopeIndex *scope_index,
                        const IPostings *filter) {
  return perform_search_operation(
      inverted_index, expand_fuzzy(inverted_index, expr), scope_index, filter);
}

inline std::shared_ptr<IPostings>
perform_near_operation(const IInvertedIndex &inverted_index,
                       const Expression &expr, const IScopeIndex *scope_index,
                       const IPostings *filter) {
  // Reused across candidate documents (assign() below), same as the And
  // path's scratch buffers: the callback runs once per document where all
  // cursors align, and this was its one remaining per-document allocation.
  std::vector<size_t> search_hit_cursors;

  return intersect_postings(
      intersection_operands(inverted_index, expr.nodes, scope_index, filter)
          .postings,
      filter,
      [&](const auto &positings_list, const auto &cursors, size_t /*ordinal*/,
          auto &term_positions, auto &term_lengths) {
        search_hit_cursors.assign(positings_list.size(), 0);

        auto done = false;
        while (!done) {
          // TODO: performance improvement by reusing values as many as
          // possible
          std::map<size_t /*term_pos*/,
                   std::pair<size_t /*slot*/, size_t /*term_length*/>>
              slots_by_term_pos;
          {
            auto slot = 0;
            for (const auto &p : positings_list) {
              auto index = cursors[slot];
              auto hit_index = search_hit_cursors[slot];
              auto term_pos = p->term_position(index, hit_index);
              auto term_length = p->term_length(index, hit_index);
              slots_by_term_pos[term_pos] = std::pair(slot, term_length);
              slot++;
            }
          }

          // Not `near`: windows.h defines that name as a macro.
          auto within_distance = true;
          {
            auto it = slots_by_term_pos.begin();
            auto it_prev = it;
            ++it;
            while (it != slots_by_term_pos.end()) {
              auto [prev_term_pos, prev_item] = *it_prev;
              auto [prev_slot, prev_term_count] = prev_item;
              auto [term_pos, item] = *it;
              auto delta = term_pos - (prev_term_pos + prev_term_count - 1);
              if (delta > expr.near_operation_distance) {
                within_distance = false;
                break;
              }
              it_prev = it;
              ++it;
            }
          }

          if (within_distance) {
            // Skip all search hit cursors
            for (auto [term_pos, item] : slots_by_term_pos) {
              auto [slot, term_length] = item;
              term_positions.push_back(term_pos);
              term_lengths.push_back(term_length);
              search_hit_cursors[slot]++;

              if (search_hit_cursors[slot] ==
                  positings_list[slot]->search_hit_count(cursors[slot])) {
                done = true;
              }
            }
          } else {
            // Skip search hit cursor for the smallest slot
            auto slot = slots_by_term_pos.begin()->second.first;
            search_hit_cursors[slot]++;

            if (search_hit_cursors[slot] ==
                positings_list[slot]->search_hit_count(cursors[slot])) {
              done = true;
            }
          }
        }

        return !term_positions.empty();
      });
}

// Like perform_near_operation, but keeps only the combinations of hits (one
// per node) that fall in the same structural unit (e.g. paragraph), per
// scope_index->scope_id(), instead of within a fixed position distance. If
// scope_index is null, or a candidate document has no scope data registered
// for expr.scope_name, it contributes no matches -- the same "no match"
// treatment as an empty And/Or operand, not an error.
inline std::shared_ptr<IPostings> perform_same_scope_operation(
    const IInvertedIndex &inverted_index, const Expression &expr,
    const IScopeIndex *scope_index, const IPostings *filter) {
  if (!scope_index) {
    return std::make_shared<SearchResult>();
  }

  // Reused across candidate documents; see perform_near_operation.
  std::vector<size_t> search_hit_cursors;

  return intersect_postings(
      intersection_operands(inverted_index, expr.nodes, scope_index, filter)
          .postings,
      filter,
      [&](const auto &positings_list, const auto &cursors, size_t ordinal,
          auto &term_positions, auto &term_lengths) {
        if (!scope_index->has_scope(expr.scope_name, ordinal)) {
          return false;
        }

        search_hit_cursors.assign(positings_list.size(), 0);

        auto done = false;
        while (!done) {
          std::map<size_t /*term_pos*/,
                   std::pair<size_t /*slot*/, size_t /*term_length*/>>
              slots_by_term_pos;
          {
            auto slot = 0;
            for (const auto &p : positings_list) {
              auto index = cursors[slot];
              auto hit_index = search_hit_cursors[slot];
              auto term_pos = p->term_position(index, hit_index);
              auto term_length = p->term_length(index, hit_index);
              slots_by_term_pos[term_pos] = std::pair(slot, term_length);
              slot++;
            }
          }

          std::optional<size_t> reference_scope_id;
          auto same_scope = true;
          for (const auto &[term_pos, item] : slots_by_term_pos) {
            auto sid =
                scope_index->scope_id(expr.scope_name, ordinal, term_pos);
            if (!reference_scope_id) {
              reference_scope_id = sid;
            } else if (*reference_scope_id != sid) {
              same_scope = false;
              break;
            }
          }

          if (same_scope) {
            for (const auto &[term_pos, item] : slots_by_term_pos) {
              auto [slot, term_length] = item;
              term_positions.push_back(term_pos);
              term_lengths.push_back(term_length);
              search_hit_cursors[slot]++;

              if (search_hit_cursors[slot] ==
                  positings_list[slot]->search_hit_count(cursors[slot])) {
                done = true;
              }
            }
          } else {
            // Skip search hit cursor for the smallest slot, same tie-break as
            // perform_near_operation.
            auto slot = slots_by_term_pos.begin()->second.first;
            search_hit_cursors[slot]++;

            if (search_hit_cursors[slot] ==
                positings_list[slot]->search_hit_count(cursors[slot])) {
              done = true;
            }
          }
        }

        return !term_positions.empty();
      });
}

//-----------------------------------------------------------------------------

inline std::shared_ptr<IPostings>
perform_search_operation(const IInvertedIndex &inverted_index,
                         const Expression &expr, const IScopeIndex *scope_index,
                         const IPostings *filter) {
  switch (expr.operation) {
  case Operation::Term:
    return perform_term_operation(inverted_index, expr);
  case Operation::And:
    return perform_and_operation(inverted_index, expr, scope_index, filter);
  case Operation::Adjacent:
    return perform_adjacent_operation(inverted_index, expr, scope_index,
                                      filter);
  case Operation::Or:
    return perform_or_operation(inverted_index, expr, scope_index, filter);
  case Operation::Near:
    return perform_near_operation(inverted_index, expr, scope_index, filter);
  case Operation::SameScope:
    return perform_same_scope_operation(inverted_index, expr, scope_index,
                                        filter);
  case Operation::Prefix:
    return perform_prefix_operation(inverted_index, expr, scope_index, filter);
  case Operation::Wildcard:
    return perform_wildcard_operation(inverted_index, expr, scope_index,
                                      filter);
  case Operation::Fuzzy:
    return perform_fuzzy_operation(inverted_index, expr, scope_index, filter);
  default:
    return nullptr;
  }
}

template <typename T>
void enumerate_terms(const IInvertedIndex &invidx, const Expression &expr,
                     T fn) {
  if (expr.operation == Operation::Term) {
    fn(expr.term_str);
  } else if (expr.operation == Operation::Prefix) {
    // Score a prefix node as the Or it expands to, so that `foo*` and a
    // hand-written Or over the same terms score identically.
    invidx.enumerate_terms_with_prefix(expr.term_str, fn);
  } else if (expr.operation == Operation::Wildcard) {
    // Same rationale as Operation::Prefix above.
    invidx.enumerate_terms_with_wildcard(expr.term_str, fn);
  } else if (expr.operation == Operation::Fuzzy) {
    // Same again; every term within the distance scores as an equal Or
    // branch, with no boost for being a closer match.
    invidx.enumerate_terms_with_edit_distance(
        expr.term_str, expr.near_operation_distance, fn);
  } else if (expr.operation == Operation::Not) {
    // Excluded terms do not contribute to scores.
  } else {
    for (const auto &node : expr.nodes) {
      enumerate_terms(invidx, node, fn);
    }
  }
}

//-----------------------------------------------------------------------------
// Query parsing
//-----------------------------------------------------------------------------

// `Not` is allowed only as a direct child of `And`, and `And` must have at
// least one positive child, since the index cannot enumerate all documents.
inline bool is_valid_expression(const Expression &expr) {
  switch (expr.operation) {
  case Operation::Term:
    return true;
  case Operation::Not:
    return false;
  case Operation::And: {
    size_t positive_count = 0;
    for (const auto &node : expr.nodes) {
      if (node.operation == Operation::Not) {
        if (!is_valid_expression(node.nodes[0])) {
          return false;
        }
      } else {
        if (!is_valid_expression(node)) {
          return false;
        }
        positive_count++;
      }
    }
    return positive_count > 0;
  }
  default:
    for (const auto &node : expr.nodes) {
      if (!is_valid_expression(node)) {
        return false;
      }
    }
    return true;
  }
}

inline const size_t DEFAULT_NEAR_SIZE = 4;

// Fuzzy queries are capped rather than taken at face value: the digits come
// straight from an end-user query string, and a large edit distance defeats
// the pruning both backends rely on (the FST automaton's can_match() stops
// rejecting subtrees, the in-memory scan's length filter stops rejecting
// terms), so `x~9` would quietly turn into "match most of the dictionary".
// Lucene caps fuzzy queries at 2 for the same reason. The C++ API
// (enumerate_terms_with_edit_distance) is left uncapped, since a caller
// naming a distance directly is not untrusted input.
inline const size_t MAX_FUZZY_EDITS = 2;

// Saturating parse of the digits after `~`. The grammar guarantees they are
// digits, so the only thing to defend against is a value too large to hold --
// `apple~99999999999999999999` must clamp, not overflow or throw. The break is
// tested before the multiply, so `value` is at most MAX_FUZZY_EDITS going in
// and MAX_FUZZY_EDITS * 10 + 9 coming out, nowhere near size_t's range however
// many digits follow.
inline size_t parse_max_edits(std::string_view digits) {
  size_t value = 0;
  for (auto c : digits) {
    if (value > MAX_FUZZY_EDITS) {
      break;
    }
    value = value * 10 + static_cast<size_t>(c - '0');
  }
  return std::min(value, MAX_FUZZY_EDITS);
}

// Applies `convert` to the term a trailing operator (`*`, `~N`) was attached
// to. Only the last term converts, so a token the raw tokenizer split into an
// implicit phrase (`well-kno*`, `well-known~1`) keeps its leading terms exact.
// Anything else -- an Or a filter produced from synonyms, a Prefix from
// `app*~2` -- is left alone and the operator silently dropped, since a prefix
// of, or a distance around, a synonym set has no useful meaning.
//
// An empty term means the filter dropped the whole token (a stop word). It has
// to stay a Term, which matches nothing: as a Prefix the empty string
// enumerates the entire dictionary, and as a Fuzzy it matches every term of
// length <= N, so `the*` and `the~2` would silently become near-match-alls.
template <typename Convert>
inline Expression convert_operator_target(Expression expr, Convert convert) {
  auto *target = &expr;
  if (expr.operation == Operation::Adjacent && !expr.nodes.empty()) {
    target = &expr.nodes.back();
  }
  if (target->operation == Operation::Term && !target->term_str.empty()) {
    convert(*target);
  }
  return expr;
}

inline Expression as_prefix(Expression expr) {
  return convert_operator_target(
      std::move(expr), [](Expression &e) { e.operation = Operation::Prefix; });
}

inline Expression as_fuzzy(Expression expr, size_t max_edits) {
  return convert_operator_target(std::move(expr), [&](Expression &e) {
    e.operation = Operation::Fuzzy;
    e.near_operation_distance = max_edits;
  });
}

// Shared grammar for both parse_query overloads; only TERM handling (how a
// raw query token becomes an Expression) differs between them.
inline std::optional<Expression> parse_query_impl(
    const std::function<Expression(std::string_view)> &term_handler,
    std::string_view query) {
  // FUZZY has to precede TERM (PEG picks the first alternative that matches,
  // and TERM alone would consume `apple` out of `apple~2` and leave `~2` to be
  // read as a NEAR operator -- which is exactly how `apple~2` used to parse).
  // It is one `<...>` token so that `%whitespace` cannot creep between the
  // term and its `~N`, keeping `apple~2` a fuzzy query while `apple ~ 2` stays
  // the NEAR it has always been. Requiring digits is what protects the other
  // established spellings: `apple~tree` has none, so it stays NEAR. The
  // trailing lookahead rejects `apple~2x`, where the digits are not the end of
  // the token -- without it, FUZZY would take `apple~2` and orphan the `x`.
  // That lookahead has to sit inside the `<...>`: outside it, %whitespace is
  // skipped first and it would test the start of the *next* token instead,
  // which breaks `apple~2 banana`.
  static peg::parser parser(R"(
    ROOT        <- OR?
    OR          <- AND ('|' AND)*
    AND         <- NOT+
    NOT         <- '-' PRIMARY / NEAR
    NEAR        <- PRIMARY ('~' PRIMARY)*
    PRIMARY     <- PHRASE / FUZZY / TERM / '(' OR ')'
    PHRASE      <- '"' TERM+ '"'
    FUZZY       <- < (![-"|~() \t\r\n] .) (!["|~() \t\r\n] .)* '~' [0-9]+ !(!["|~() \t\r\n] .) >
    TERM        <- < (![-"|~() \t\r\n] .) (!["|~() \t\r\n] .)* >
    %whitespace <- [ \t]*
  )");

  parser["ROOT"] =
      [&](const peg::SemanticValues &vs) -> std::optional<Expression> {
    if (!vs.empty()) {
      return std::any_cast<Expression>(vs[0]);
    }
    return std::nullopt;
  };

  auto list_handler = [&](Operation operation) {
    return [=](const peg::SemanticValues &vs) {
      if (vs.size() == 1) {
        return std::any_cast<Expression>(vs[0]);
      }
      return Expression{operation, std::u32string(), DEFAULT_NEAR_SIZE,
                        vs.transform<Expression>()};
    };
  };
  parser["OR"] = list_handler(Operation::Or);
  parser["AND"] = list_handler(Operation::And);
  parser["NEAR"] = list_handler(Operation::Near);

  parser["PHRASE"] = [=](const peg::SemanticValues &vs) {
    // Flatten implicit phrases made from a single token (e.g. `well-known`).
    std::vector<Expression> nodes;
    for (const auto &v : vs) {
      auto expr = std::any_cast<Expression>(v);
      if (expr.operation == Operation::Adjacent) {
        nodes.insert(nodes.end(), expr.nodes.begin(), expr.nodes.end());
      } else {
        nodes.push_back(expr);
      }
    }
    if (nodes.size() == 1) {
      return nodes[0];
    }
    return Expression{Operation::Adjacent, std::u32string(), DEFAULT_NEAR_SIZE,
                      std::move(nodes)};
  };

  parser["NOT"] = [](const peg::SemanticValues &vs) {
    if (vs.choice() == 0) {
      return Expression{Operation::Not, std::u32string(), 0,
                        {std::any_cast<Expression>(vs[0])}};
    }
    return std::any_cast<Expression>(vs[0]);
  };

  parser["TERM"] = [&](const peg::SemanticValues &vs) {
    return term_handler(vs.token());
  };

  // The `~N` is stripped here rather than inside term_handler because, unlike
  // the `*` of a prefix query, `~` is a grammar operator: term_handler never
  // sees a token containing one. The literal part still goes through
  // term_handler, so a fuzzy term is tokenized and filtered exactly like a
  // plain one.
  parser["FUZZY"] = [&](const peg::SemanticValues &vs) {
    auto token = vs.token();
    auto tilde = token.rfind('~');
    return as_fuzzy(term_handler(token.substr(0, tilde)),
                    parse_max_edits(token.substr(tilde + 1)));
  };

  // parser.log = [](size_t line, size_t col, const std::string& msg) {
  //   std::cerr << line << ":" << col << ": " << msg << "\n";
  // };

  std::optional<Expression> expr;
  if (!parser.parse(query, expr)) {
    return std::nullopt;
  }

  if (expr && !is_valid_expression(*expr)) {
    return std::nullopt;
  }

  return expr;
}

} // namespace detail

//-----------------------------------------------------------------------------
// Definitions of everything declared in the interface above
//-----------------------------------------------------------------------------

inline size_t IPostings::advance(size_t from, size_t ordinal) const {
  return detail::gallop_lower_bound(*this, from, size(), ordinal);
}

inline IPostings::~IPostings() = default;

inline IInvertedIndex::~IInvertedIndex() = default;

inline IScopeIndex::~IScopeIndex() = default;

inline size_t InMemoryInvertedIndexBase::Postings::size() const {
  return document_ordinals_.size();
}

inline size_t
InMemoryInvertedIndexBase::Postings::document_ordinal(size_t index) const {
  return document_ordinals_[index];
}

// Straight out of the vector: the default would reach every element through
// the virtual document_ordinal, which is the cost a block read exists to
// avoid, and would make refilling a window here cost more than not having one.
inline size_t InMemoryInvertedIndexBase::Postings::read_ordinals(
    size_t index, size_t *out, size_t count) const {
  if (index >= document_ordinals_.size()) {
    return 0;
  }
  count = std::min(count, document_ordinals_.size() - index);
  std::copy(document_ordinals_.begin() + index,
            document_ordinals_.begin() + index + count, out);
  return count;
}

inline size_t
InMemoryInvertedIndexBase::Postings::search_hit_count(size_t index) const {
  return offsets_[index + 1] - offsets_[index];
}

inline size_t InMemoryInvertedIndexBase::Postings::term_position(
           size_t index, size_t search_hit_index) const {
  return positions_[offsets_[index] + search_hit_index];
}

inline void InMemoryInvertedIndexBase::Postings::read_term_positions(
           size_t index, size_t *out) const {
  std::copy(positions_.begin() + offsets_[index],
            positions_.begin() + offsets_[index + 1], out);
}

inline size_t InMemoryInvertedIndexBase::Postings::term_length(
           size_t index, size_t search_hit_index) const {
  return 1;
}

inline bool InMemoryInvertedIndexBase::Postings::is_term_position(
           size_t index, size_t term_pos) const {
  return std::binary_search(positions_.begin() + offsets_[index],
                            positions_.begin() + offsets_[index + 1], term_pos);
}

inline bool
InMemoryInvertedIndexBase::Postings::add_term_position(size_t ordinal,
                                                       size_t term_pos) {
  // Ordinals arrive in indexing order and each document's positions in
  // ascending order, so this is append-only: extend the current document's
  // slice, or open a new one. An ordinal below the last would mean a
  // document was revisited, which push_document rules out -- there used
  // to be a sorted-insert path here for callers indexing out of order, and
  // it is gone with them.
  if (!document_ordinals_.empty() && document_ordinals_.back() == ordinal) {
    if (positions_.back() == term_pos) {
      return false;
    }
    positions_.push_back(term_pos);
    offsets_.back() = positions_.size();
    return true;
  }
  assert(document_ordinals_.empty() || document_ordinals_.back() < ordinal);
  document_ordinals_.push_back(ordinal);
  positions_.push_back(term_pos);
  offsets_.push_back(positions_.size());
  return true;
}

inline void InMemoryInvertedIndexBase::Postings::save(std::ostream &os) const {
  detail::write_scalar<uint64_t>(os, document_ordinals_.size());
  for (size_t i = 0; i < document_ordinals_.size(); i++) {
    detail::write_scalar<uint64_t>(os, document_ordinals_[i]);
    detail::write_scalar<uint64_t>(os, offsets_[i + 1] - offsets_[i]);
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      detail::write_scalar<uint64_t>(os, positions_[j]);
    }
  }
}

inline void InMemoryInvertedIndexBase::Postings::load(std::istream &is) {
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

inline void
InMemoryInvertedIndexBase::Postings::save_varint(std::ostream &os) const {
  detail::write_varint(os, document_ordinals_.size());
  size_t previous_ordinal = 0;
  for (size_t i = 0; i < document_ordinals_.size(); i++) {
    detail::write_varint(os, document_ordinals_[i] - previous_ordinal);
    previous_ordinal = document_ordinals_[i];
    detail::write_varint(os, offsets_[i + 1] - offsets_[i]);
    size_t previous_position = 0;
    for (auto j = offsets_[i]; j < offsets_[i + 1]; j++) {
      detail::write_varint(os, positions_[j] - previous_position);
      previous_position = positions_[j];
    }
  }
}

inline void InMemoryInvertedIndexBase::Postings::load_varint(std::istream &is) {
  auto entry_count = detail::read_varint(is);
  document_ordinals_.clear();
  offsets_.assign(1, 0);
  positions_.clear();
  document_ordinals_.reserve(static_cast<size_t>(entry_count));
  offsets_.reserve(static_cast<size_t>(entry_count) + 1);
  size_t previous_ordinal = 0;
  for (uint64_t i = 0; i < entry_count; i++) {
    previous_ordinal += static_cast<size_t>(detail::read_varint(is));
    document_ordinals_.push_back(previous_ordinal);
    auto position_count = detail::read_varint(is);
    size_t previous_position = 0;
    for (uint64_t j = 0; j < position_count; j++) {
      previous_position += static_cast<size_t>(detail::read_varint(is));
      positions_.push_back(previous_position);
    }
    offsets_.push_back(positions_.size());
  }
}

inline void InMemoryInvertedIndexBase::Postings::save_compressed(
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

inline void
InMemoryInvertedIndexBase::Postings::load_compressed(std::istream &is) {
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

namespace detail {

// The text-range section compressed with the same monotonization scheme as
// the postings: token positions are strictly increasing within a document,
// so a per-document base turns their concatenation into one monotone
// Elias-Fano sequence, and token lengths (>= 1) become monotone as a
// cumulative sum. Five global EF sequences cover the whole section, so the
// per-document overhead is negligible even for many small documents.
inline void save_text_ranges_compressed(std::ostream &os,
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

inline void load_text_ranges_compressed(std::istream &is,
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

inline size_t InMemoryInvertedIndexBase::document_count() const {
  return documents_.size() - removed_ordinals_.size();
}

inline size_t
InMemoryInvertedIndexBase::document_term_count(size_t ordinal) const {
  return documents_.at(ordinal).term_count;
}

inline size_t InMemoryInvertedIndexBase::push_document() {
  documents_.push_back(Document{0});
  return documents_.size() - 1;
}

inline void
InMemoryInvertedIndexBase::set_document_term_count(size_t ordinal,
                                                   size_t term_count) {
  auto &document = documents_.at(ordinal);
  total_document_term_count_ -= document.term_count;
  document.term_count = term_count;
  total_document_term_count_ += term_count;
}

inline double InMemoryInvertedIndexBase::average_document_term_count() const {
  auto live = document_count();
  if (live == 0) {
    return 0.0;
  }
  return static_cast<double>(total_document_term_count_) /
         static_cast<double>(live);
}

inline void InMemoryInvertedIndexBase::tombstone(size_t ordinal) {
  if (removed_ordinals_.insert(ordinal).second) {
    total_document_term_count_ -= documents_.at(ordinal).term_count;
  }
}

inline bool
InMemoryInvertedIndexBase::term_exists(const std::u32string &str) const {
  return term_dictionary_.find(str) != term_dictionary_.end();
}

inline size_t
InMemoryInvertedIndexBase::term_count(const std::u32string &str) const {
  auto it = term_dictionary_.find(str);
  return it != term_dictionary_.end() ? it->second.term_count : 0;
}

inline size_t InMemoryInvertedIndexBase::term_count(const std::u32string &str,
                                                    size_t ordinal) const {
  auto p = postings(str);
  auto i = detail::find_postings_index_for_ordinal(*p, ordinal);
  if (i < p->size()) {
    return p->search_hit_count(i);
  }
  return 0;
}

inline size_t InMemoryInvertedIndexBase::df(const std::u32string &str) const {
  return postings(str)->size();
}

inline double InMemoryInvertedIndexBase::tf(const std::u32string &str,
                                            size_t ordinal) const {
  auto p = postings(str);
  auto i = detail::find_postings_index_for_ordinal(*p, ordinal);
  if (i < p->size()) {
    return static_cast<double>(p->search_hit_count(i)) /
           static_cast<double>(document_term_count(ordinal));
  }
  return 0.0;
}

inline std::shared_ptr<const IPostings>
       InMemoryInvertedIndexBase::postings(const std::u32string &str) const {
  static const auto empty_postings = std::make_shared<const Postings>();
  auto it = term_dictionary_.find(str);
  if (it != term_dictionary_.end()) {
    return std::shared_ptr<const IPostings>(
        std::shared_ptr<void>(), &it->second.postings);
  }
  return empty_postings;
}

inline void InMemoryInvertedIndexBase::enumerate_terms_with_prefix(
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

inline void InMemoryInvertedIndexBase::enumerate_terms_with_wildcard(
    const std::u32string &pattern,
    const std::function<void(const std::u32string &str)> &callback) const {
  // Same full-scan tradeoff as enumerate_terms_with_prefix: term_dictionary_
  // is a hash map, so every term has to be tested against the pattern.
  for (const auto &[str, term] : term_dictionary_) {
    if (detail::matches_wildcard(pattern, str)) {
      callback(str);
    }
  }
}

inline void InMemoryInvertedIndexBase::enumerate_terms_with_edit_distance(
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
    if (detail::within_edit_distance(str, target, max_edits, prev, curr)) {
      callback(str);
    }
  }
}

inline bool InMemoryInvertedIndexBase::has_removed_documents() const {
  return !removed_ordinals_.empty();
}

inline bool
InMemoryInvertedIndexBase::is_document_removed(size_t ordinal) const {
  return removed_ordinals_.find(ordinal) != removed_ordinals_.end();
}

inline void InMemoryInvertedIndexBase::set_scope_ids(
           const std::string &scope_name, size_t ordinal,
           const std::vector<size_t> &scope_ids) {
  if (scope_ids.size() != document_term_count(ordinal)) {
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
      ordinal, detail::EliasFano(values, universe));
}

inline bool InMemoryInvertedIndexBase::has_scope(const std::string &scope_name,
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

inline size_t InMemoryInvertedIndexBase::scope_id(const std::string &scope_name,
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

inline void InMemoryInvertedIndexBase::save(std::ostream &os,
                                            IndexFormat format) const {
  // Documents section, in ordinal order. The ordinal is the position, so
  // each record is just the term count (the keys are a section of their own,
  // written by the owner of the KeyTable right after this core); a
  // tombstoned document is written like any other, since its ordinal must
  // keep its slot for the postings that still name it. Compressed writes the
  // counts as varint -- a document is tens of terms long, so a fixed u64
  // spends seven bytes a document on leading zeros.
  auto compressed_format = format == IndexFormat::Compressed;
  detail::write_scalar<uint64_t>(os, documents_.size());
  for (const auto &document : documents_) {
    if (compressed_format) {
      detail::write_varint(os, document.term_count);
    } else {
      detail::write_scalar<uint64_t>(os, document.term_count);
    }
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
    // which is the term's position in this sorted sequence (see
    // TermDictionaryFst).
    std::vector<std::string> keys;
    keys.reserve(terms.size());
    for (const auto *term : terms) {
      keys.push_back(u8(term->str));
    }
    detail::write_term_dictionary_fst(os, keys);
  }

  for (const auto *term : terms) {
    if (!compressed_format) {
      detail::write_u32string(os, term->str);
      detail::write_scalar<uint64_t>(os, term->term_count);
      term->postings.save(os);
      continue;
    }
    detail::write_varint(os, term->term_count);
    // Per-term representation flag; see kCompressedPostingsThreshold. One
    // byte, since it says one thing: Elias-Fano, or the varint postings used
    // below the threshold.
    bool elias_fano =
        term->postings.size() >= detail::kCompressedPostingsThreshold;
    detail::write_scalar<uint8_t>(os, elias_fano ? 1 : 0);
    if (elias_fano) {
      term->postings.save_compressed(os);
    } else {
      term->postings.save_varint(os);
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

inline void InMemoryInvertedIndexBase::load(std::istream &is,
                                            IndexFormat format) {
  auto compressed_format = format == IndexFormat::Compressed;
  documents_.clear();
  auto document_count = detail::read_scalar<uint64_t>(is);
  documents_.reserve(static_cast<size_t>(document_count));
  for (uint64_t i = 0; i < document_count; i++) {
    documents_.push_back(Document{static_cast<size_t>(
        compressed_format ? detail::read_varint(is)
                          : detail::read_scalar<uint64_t>(is))});
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
  if (compressed_format) {
    detail::TermDictionaryFst fst;
    fst.load(is);
    fst_terms = fst.terms(static_cast<size_t>(term_count));
  }

  for (uint64_t i = 0; i < term_count; i++) {
    auto str = compressed_format ? fst_terms[static_cast<size_t>(i)]
                                 : detail::read_u32string(is);
    auto count = static_cast<size_t>(compressed_format
                                         ? detail::read_varint(is)
                                         : detail::read_scalar<uint64_t>(is));
    auto &term = term_dictionary_[str];
    term.str = str;
    term.term_count = count;
    if (!compressed_format) {
      term.postings.load(is);
    } else if (detail::read_scalar<uint8_t>(is)) {
      term.postings.load_compressed(is);
    } else {
      term.postings.load_varint(is);
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

//-----------------------------------------------------------------------------
// Tokenizers and splitters
//-----------------------------------------------------------------------------

inline TextRange text_range(const TextRangeList<TextRange> &text_range_list,
                            const IPostings &positions, size_t index,
                            size_t search_hit_index) {
  auto ordinal = positions.document_ordinal(index);
  auto term_pos = positions.term_position(index, search_hit_index);
  auto term_length = positions.term_length(index, search_hit_index);
  if (term_length == 1) {
    return text_range_list.at(ordinal)[term_pos];
  } else {
    auto beg = text_range_list.at(ordinal)[term_pos];
    auto end = text_range_list.at(ordinal)[term_pos + term_length - 1];
    auto length = end.position + end.length - beg.position;
    return TextRange{beg.position, length};
  }
}

//-----------------------------------------------------------------------------

inline TextSplitter utf8_plain_text_splitter() {
  return [](std::string_view text, const auto &emit) {
    detail::for_each_word(text, nullptr, emit);
  };
}

inline TextSplitter utf8_plain_text_splitter(
    Segmenter segmenter, std::function<bool(char32_t)> claims) {
  if (!segmenter || !claims) {
    throw std::invalid_argument(
        "searchlib: utf8_plain_text_splitter needs a segmenter and what it "
        "claims");
  }
  // Held through a shared_ptr, as subword_splitter holds its stages: a
  // TextSplitter is copied into every SplitterTokenizer and every parse_query
  // call, and the two callables need not be copied with it.
  auto delegation = std::make_shared<const detail::Delegation>(
      detail::Delegation{std::move(segmenter), std::move(claims)});
  return [delegation](std::string_view text, const auto &emit) {
    detail::for_each_word(text, delegation.get(), emit);
  };
}

inline TextSplitter subword_splitter(TextSplitter base, Decomposer decompose) {
  if (!decompose) {
    throw std::invalid_argument("searchlib: subword_splitter needs a decomposer");
  }
  if (!base) {
    base = utf8_plain_text_splitter();
  }
  // Held through a shared_ptr for the same reason load_segmenting_splitter
  // does it: a TextSplitter is copied into every SplitterTokenizer and every
  // parse_query call, and the two callables need not be copied with it.
  struct Stages {
    TextSplitter base;
    Decomposer decompose;
  };
  auto stages = std::make_shared<Stages>(Stages{std::move(base), std::move(decompose)});
  return [stages](std::string_view text, const auto &emit) {
    stages->base(text, [&](const std::u32string &, TextRange range) {
      // The decomposer sees the term's own bytes, so that the pieces it hands
      // back can be located in them; what `base` emitted for the term may
      // already be normalized and is not what a range points at.
      auto term = text.substr(range.position, range.length);
      auto pieces = stages->decompose(term);
      size_t cursor = 0;
      for (const auto &piece : pieces) {
        auto at = piece.empty() ? std::string_view::npos : term.find(piece, cursor);
        if (at == std::string_view::npos) {
          throw std::invalid_argument(
              "searchlib: subword_splitter: piece '" + piece +
              "' is not part of its term '" + std::string(term) +
              "' (from the previous piece on); an analyzer whose output is "
              "not the surface text should be a TextSplitter of its own");
        }
        emit(u32(piece), TextRange{range.position + at, piece.size()});
        cursor = at + piece.size();
      }
    });
  };
}

//-----------------------------------------------------------------------------

inline UTF8PlainTextTokenizer::UTF8PlainTextTokenizer(std::string_view text)
           : text_(text) {}

// The body below is SplitterTokenizer's null-splitter branch written out
// again, which is a deliberate few lines of duplication: delegating to
// SplitterTokenizer(nullptr, text_) measures slower on this, the default
// indexing path. Tokenizing test/t_kjv.tsv (31103 documents) with no
// normalizer, clang -O2 -DNDEBUG, best of 5, three interleaved rounds,
// identical checksums both ways: 22.4-22.6 ms direct against 23.2-23.4 ms
// delegated, about 4%. The cost is per document rather than per term --
// building the temporary and moving the two std::functions into it, plus a
// call that no longer inlines -- so it does not grow with document length, but
// it is real and every existing caller of this library takes this path.
inline void UTF8PlainTextTokenizer::operator()(
           Normalizer normalizer,
           std::function<void(const std::u32string &str, size_t term_pos,
                              TextRange text_range)>
               callback) {
  size_t term_pos = 0;
  detail::for_each_word(
      text_, nullptr, [&](const std::u32string &str, TextRange range) {
        // Spelled as an if rather than `callback(normalizer ? normalizer(str)
        // : str, ...)`: one arm of that conditional is a prvalue, so the whole
        // expression is a prvalue and `str` gets copied on every term even
        // when there is no normalizer at all. See SplitterTokenizer below.
        if (normalizer) {
          callback(normalizer(str), term_pos, range);
        } else {
          callback(str, term_pos, range);
        }
        term_pos++;
      });
}

//-----------------------------------------------------------------------------

inline SplitterTokenizer::SplitterTokenizer(TextSplitter splitter,
                                            std::string_view text)
           : splitter_(std::move(splitter)), text_(text) {}

inline void SplitterTokenizer::operator()(
           Normalizer normalizer,
           std::function<void(const std::u32string &str, size_t term_pos,
                              TextRange text_range)>
               callback) {
  constexpr auto npos = std::numeric_limits<size_t>::max();
  size_t term_pos = 0;
  size_t last_start = npos;
  auto emit = [&](const std::u32string &str, TextRange range) {
    if (last_start != npos && range.position != last_start) {
      term_pos++;
    }
    last_start = range.position;
    // Not a ternary, for the reason given in UTF8PlainTextTokenizer above: it
    // would copy every term whether or not a normalizer is configured.
    if (normalizer) {
      callback(normalizer(str), term_pos, range);
    } else {
      callback(str, term_pos, range);
    }
  };
  if (splitter_) {
    splitter_(text_, emit);
  } else {
    detail::for_each_word(text_, nullptr, emit);
  }
}

//-----------------------------------------------------------------------------
// Analyzer pipeline
//-----------------------------------------------------------------------------

inline TermFilter compose(std::vector<TermFilter> filters) {
  return [filters = std::move(filters)](
             const std::u32string &str,
             std::function<void(std::u32string)> emit) {
    // Feed each surviving token of stage i into stage i+1; reaching the end
    // of the chain emits it. Drops (a stage that never calls its emit) and
    // 1->N expansions fall out naturally from how often each stage emits.
    std::function<void(size_t, const std::u32string &)> apply =
        [&](size_t i, const std::u32string &s) {
          if (i == filters.size()) {
            emit(s);
            return;
          }
          filters[i](s, [&](std::u32string out) { apply(i + 1, out); });
        };
    apply(0, str);
  };
}

inline TermFilter to_term_filter(Normalizer normalizer) {
  return [normalizer = std::move(normalizer)](
             const std::u32string &str,
             std::function<void(std::u32string)> emit) {
    emit(normalizer ? normalizer(str) : str);
  };
}

//-----------------------------------------------------------------------------
// Search
//-----------------------------------------------------------------------------

inline Expression expand_prefixes(const IInvertedIndex &inverted_index,
                                  const Expression &expr) {
  if (expr.operation == Operation::Prefix) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_prefix(
        expr.term_str, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_prefix leaves the order unspecified, and the
    // expanded Expression is handed back to the caller, so sort to keep it
    // reproducible for one prefix against one index.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_prefixes(inverted_index, node);
  }
  return expanded;
}

inline Expression expand_wildcards(const IInvertedIndex &inverted_index,
                                   const Expression &expr) {
  if (expr.operation == Operation::Wildcard) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_wildcard(
        expr.term_str, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_wildcard leaves the order unspecified, same
    // reproducibility rationale as expand_prefixes.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_wildcards(inverted_index, node);
  }
  return expanded;
}

inline Expression expand_fuzzy(const IInvertedIndex &inverted_index,
                               const Expression &expr) {
  if (expr.operation == Operation::Fuzzy) {
    std::vector<Expression> nodes;
    inverted_index.enumerate_terms_with_edit_distance(
        expr.term_str, expr.near_operation_distance, [&](const auto &str) {
          nodes.push_back(Expression{Operation::Term, str});
        });

    // enumerate_terms_with_edit_distance leaves the order unspecified, same
    // reproducibility rationale as expand_prefixes.
    std::sort(nodes.begin(), nodes.end(), [](const auto &a, const auto &b) {
      return a.term_str < b.term_str;
    });

    return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
  }

  auto expanded = expr;
  for (auto &node : expanded.nodes) {
    node = expand_fuzzy(inverted_index, node);
  }
  return expanded;
}

namespace detail {

// perform_search under a filter (see perform_search_operation): what
// bm25_top_k builds its result with, so that result stays exactly what
// perform_search would give.
inline std::shared_ptr<IPostings>
perform_filtered_search(const IInvertedIndex &inverted_index,
                        const Expression &expr, const IScopeIndex *scope_index,
                        const IPostings *filter) {
  auto result =
      perform_search_operation(inverted_index, expr, scope_index, filter);
  // Exclude logically-deleted documents from the final result. Skipped
  // entirely when the index has no tombstones, so the common path is free.
  if (result && inverted_index.has_removed_documents()) {
    return std::make_shared<FilteredPostings>(inverted_index,
                                              std::move(result));
  }
  return result;
}

} // namespace detail

inline std::shared_ptr<IPostings>
perform_search(const IInvertedIndex &inverted_index, const Expression &expr,
               const IScopeIndex *scope_index) {
  return detail::perform_filtered_search(inverted_index, expr, scope_index,
                                         nullptr);
}

inline size_t term_count_score(const IInvertedIndex &invidx,
                               const Expression &expr,
                               const IPostings &postings, size_t index) {
  auto ordinal = postings.document_ordinal(index);
  size_t score = 0;
  detail::enumerate_terms(invidx, expr, [&](const auto &term) {
    score += invidx.term_count(term, ordinal);
  });
  return score;
}

inline double tf_idf_score(const IInvertedIndex &invidx, const Expression &expr,
                           const IPostings &postings, size_t index) {
  auto ordinal = postings.document_ordinal(index);
  auto N = static_cast<double>(invidx.document_count());
  double score = 0.0;
  detail::enumerate_terms(invidx, expr, [&](const auto &term) {
    auto n = static_cast<double>(invidx.df(term));
    auto idf = std::log2((N + 0.001) / (n + 0.001));
    score += invidx.tf(term, ordinal) * idf;
  });
  return score;
}

inline double bm25_score(const IInvertedIndex &invidx, const Expression &expr,
                         const IPostings &postings, size_t index, double k1,
                         double b) {
  auto ordinal = postings.document_ordinal(index);
  auto N = static_cast<double>(invidx.document_count());
  auto dl = static_cast<double>(invidx.document_term_count(ordinal));
  auto avgdl = static_cast<double>(invidx.average_document_term_count());

  double score = 0.0;
  detail::enumerate_terms(invidx, expr, [&](const auto &term) {
    auto n = static_cast<double>(invidx.df(term));
    auto idf = std::log2((N - n + 0.5) / (n + 0.5));
    auto tf = invidx.tf(term, ordinal);

    score +=
        idf * ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl))));
  });
  return score;
}

//-----------------------------------------------------------------------------

inline BM25Scorer::BM25Scorer(const IInvertedIndex &invidx,
                              const Expression &expr, double k1, double b)
    : invidx_(invidx), avgdl_(invidx.average_document_term_count()), k1_(k1),
      b_(b) {
  // A constructor local, not a member: idf bakes the document count into each
  // term up front, so nothing per-hit ever needs N again.
  auto N = static_cast<double>(invidx.document_count());
  // The one dictionary walk. enumerate_terms expands Prefix/Wildcard/Fuzzy
  // nodes, which is exactly the work bm25_score repeats for every hit.
  detail::enumerate_terms(invidx, expr, [&](const auto &term) {
    auto postings = invidx.postings(term);
    auto size = postings->size();
    auto n = static_cast<double>(size);
    auto block_reads = postings->prefers_block_reads();
    windowed_ = windowed_ || block_reads;
    terms_.push_back(TermState{std::move(postings),
                               std::log2((N - n + 0.5) / (n + 0.5)), size, 0,
                               0, block_reads});
  });
  if (windowed_) {
    windows_.reserve(terms_.size());
    for (const auto &term : terms_) {
      windows_.emplace_back(term.postings.get());
    }
  }
}

template <typename Result, typename TermReader>
inline double BM25Scorer::score_(Result &result, size_t index,
                                 TermReader term_reader) const {
  auto ordinal = result.document_ordinal(index);
  auto dl = static_cast<double>(invidx_.document_term_count(ordinal));
  auto norm = k1_ * (1.0 - b_ + b_ * (dl / avgdl_));

  // The terms through a local pointer and count: the loop writes each
  // term's cursor, so read through the member the compiler would reload
  // the vector's bounds on every term.
  double score = 0.0;
  const auto *terms = terms_.data();
  auto term_count = terms_.size();
  for (size_t t = 0; t < term_count; t++) {
    const auto &term = terms[t];
    // A term the document does not carry contributes with tf == 0 rather
    // than being skipped, so that a degenerate index (avgdl == 0, making
    // norm NaN) produces the same value bm25_score would.
    double tf = 0.0;
    auto &reader = term_reader(t);
    auto i = detail::find_from_cursor(reader, term.size, term.block_reads,
                                      term.cursor, term.last_ordinal, ordinal);
    if (i < term.size) {
      tf = static_cast<double>(reader.search_hit_count(i)) / dl;
    }
    score += term.idf * ((tf * (k1_ + 1.0)) / (tf + norm));
  }
  return score;
}

inline double BM25Scorer::operator()(const IPostings &postings,
                                     size_t index) const {
  if (!windowed_) {
    return score_(postings, index, [&](size_t t) -> const IPostings & {
      return *terms_[t].postings;
    });
  }
  if (result_ != &postings) {
    result_ = &postings;
    result_window_ = PostingsWindow(&postings);
  }
  return score_(result_window_, index,
                [&](size_t t) -> PostingsWindow & { return windows_[t]; });
}

inline double BM25Scorer::score_ordinal_(size_t ordinal) const {
  // score_ asks its result for the ordinal at an index; this one has the
  // single document.
  struct Document {
    size_t ordinal;
    size_t document_ordinal(size_t) const { return ordinal; }
  } document{ordinal};
  if (!windowed_) {
    return score_(document, 0, [&](size_t t) -> const IPostings & {
      return *terms_[t].postings;
    });
  }
  return score_(document, 0,
                [&](size_t t) -> PostingsWindow & { return windows_[t]; });
}

namespace detail {

// Whether expr matches exactly the union of the terms BM25Scorer scores for
// it: an Or over terms, at any depth, where Prefix, Wildcard and Fuzzy count
// as the Or they expand to. That is what lets bm25_top_k enumerate the
// candidates from the scored terms' own postings.
inline bool is_term_disjunction(const Expression &expr) {
  switch (expr.operation) {
  case Operation::Term:
  case Operation::Prefix:
  case Operation::Wildcard:
  case Operation::Fuzzy:
    return true;
  case Operation::Or:
    return std::all_of(
        expr.nodes.begin(), expr.nodes.end(),
        [](const auto &node) { return is_term_disjunction(node); });
  default:
    return false;
  }
}

// MaxScore (Turtle & Flaherty 1995) over the terms a BM25Scorer scores. Each
// term gets an upper bound on what it can add to a document's score, the
// terms are ordered by it, and once k hits are held, the weakest terms whose
// bounds together cannot beat the k-th best score become non-essential:
// a document carrying only those cannot get in, so only the other terms'
// documents are enumerated, and the weak terms are merely looked up in
// them. A candidate is dropped as soon as its bound falls to the k-th best,
// and only survivors are scored, by the scorer itself.
struct MaxScore {
  // The k best hits, with each ScoredHit::index an ordinal. nullopt when the
  // scorer's parameters leave no bound to prune with (k1 <= 0 makes an
  // absent term's contribution 0/0; b outside [0, 1] or an empty corpus's
  // avgdl of 0 break the monotonicity the bounds rest on).
  static std::optional<std::vector<ScoredHit>>
  top_k(const IInvertedIndex &invidx, const BM25Scorer &scorer, size_t k) {
    auto k1 = scorer.k1_;
    auto b = scorer.b_;
    auto avgdl = scorer.avgdl_;
    if (!(k1 > 0.0 && b >= 0.0 && b <= 1.0 && avgdl > 0.0)) {
      return std::nullopt;
    }

    // BM25Scorer::score_'s arithmetic for one term a document carries.
    auto contribution = [&](double idf, size_t count, size_t length) {
      auto dl = static_cast<double>(length);
      auto tf = static_cast<double>(count) / dl;
      return idf *
             ((tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * (dl / avgdl))));
    };

    constexpr auto kEnd = std::numeric_limits<size_t>::max();
    struct List {
      PostingsWindow window;
      size_t size;
      size_t cursor;
      // The ordinal at `cursor`, or kEnd past the last: the walk compares it
      // several times per document, and each read through a vector-backed
      // window is a virtual call.
      size_t ordinal;
      double idf;
      // The most this term adds to any document, and never below 0, which
      // is what it adds to a document without it.
      double bound;
    };
    // Re-reads the ordinal at a list's cursor after the cursor moves.
    auto settle = [&](List &list) {
      list.ordinal = list.cursor < list.size
                         ? list.window.document_ordinal(list.cursor)
                         : kEnd;
    };
    std::vector<List> lists;
    lists.reserve(scorer.terms_.size());
    // At least as many as the documents the walk can reach, which is all the
    // collector needs to size itself for a k larger than that.
    size_t candidates = 0;
    // How far apart a bound and the scorer's own sum over the same terms can
    // round: each term adds at most |idf| * (k1 + 1).
    double magnitude = 1.0;
    for (const auto &term : scorer.terms_) {
      // A term with idf <= 0 (in more than half the documents) never adds
      // anything positive, so its bound is 0 with no need to look. The
      // others take one pass over their postings: a bound from idf alone,
      // idf * (k1 + 1), is what tf saturates to, but tf here is a share of
      // the document's length and saturates nowhere near it -- on KJV the
      // real maximum is 7% to 59% of that, and so loose a bound prunes
      // next to nothing.
      double bound = 0.0;
      if (scorer.terms_.size() == 1) {
        // With one term there is nothing to skip to: its bound would only
        // ever end the walk at the last document, and finding it is a whole
        // extra pass over the list. Each document's own contribution still
        // prunes it before the scorer runs.
        bound = std::numeric_limits<double>::infinity();
      } else if (term.idf > 0.0) {
        PostingsWindow scan(term.postings.get());
        for (size_t i = 0; i < term.size; i++) {
          auto count = scan.search_hit_count(i);
          auto length = invidx.document_term_count(scan.document_ordinal(i));
          bound = std::max(bound, contribution(term.idf, count, length));
        }
      }
      lists.push_back(List{PostingsWindow(term.postings.get()), term.size, 0,
                           kEnd, term.idf, bound});
      settle(lists.back());
      magnitude += std::abs(term.idf) * (k1 + 1.0);
      candidates += term.size;
    }

    // The lists by ascending bound, and what the first j of them can add
    // together: the lists before `essential` are the non-essential ones.
    auto term_count = lists.size();
    std::vector<size_t> order(term_count);
    std::iota(order.begin(), order.end(), size_t(0));
    std::stable_sort(order.begin(), order.end(), [&](size_t x, size_t y) {
      return lists[x].bound < lists[y].bound;
    });
    std::vector<double> prefix(term_count + 1, 0.0);
    for (size_t j = 0; j < term_count; j++) {
      prefix[j + 1] = prefix[j] + lists[order[j]].bound;
    }
    size_t essential = 0;

    TopKCollector collector(k, candidates);
    // Whether a document whose score is at most `bound` cannot get in. The
    // bound and the scorer sum the same terms in different orders, so it
    // must clear the threshold by more than their rounding can differ.
    auto slack = magnitude * 1e-9;
    auto cannot_enter = [&](double bound) {
      return collector.full() && bound + slack <= collector.threshold();
    };
    auto removals = invidx.has_removed_documents();

    for (;;) {
      // The next document any essential list names.
      auto ordinal = kEnd;
      for (auto j = essential; j < term_count; j++) {
        ordinal = std::min(ordinal, lists[order[j]].ordinal);
      }
      if (ordinal == kEnd) {
        break;
      }

      // Its bound: exact for the essential terms it carries, their whole
      // bound for the non-essential ones.
      auto length = invidx.document_term_count(ordinal);
      auto bound = prefix[essential];
      for (auto j = essential; j < term_count; j++) {
        auto &list = lists[order[j]];
        if (list.ordinal == ordinal) {
          bound += contribution(
              list.idf, list.window.search_hit_count(list.cursor), length);
        }
      }
      // Then the non-essential terms, strongest first, each bound traded for
      // the exact contribution, until it is out or the terms run out.
      auto out = cannot_enter(bound);
      for (auto j = essential; !out && j-- > 0;) {
        auto &list = lists[order[j]];
        bound -= list.bound;
        if (list.ordinal < ordinal) {
          list.cursor = list.window.advance(list.cursor, ordinal);
          settle(list);
        }
        if (list.ordinal == ordinal) {
          bound += contribution(
              list.idf, list.window.search_hit_count(list.cursor), length);
        }
        out = cannot_enter(bound);
      }
      if (!out && !(removals && invidx.is_document_removed(ordinal))) {
        collector.offer(ordinal, scorer.score_ordinal_(ordinal));
      }

      for (auto j = essential; j < term_count; j++) {
        auto &list = lists[order[j]];
        if (list.ordinal == ordinal) {
          list.cursor++;
          settle(list);
        }
      }
      // The threshold only rises, so the non-essential lists only grow.
      while (essential < term_count && cannot_enter(prefix[essential + 1])) {
        essential++;
      }
    }

    return collector.take_sorted();
  }
};

} // namespace detail

inline RankedResult bm25_top_k(const IInvertedIndex &invidx,
                               const Expression &expr, size_t k, double k1,
                               double b) {
  BM25Scorer scorer(invidx, expr, k1, b);
  if (k > 0 && detail::is_term_disjunction(expr)) {
    if (auto hits = detail::MaxScore::top_k(invidx, scorer, k)) {
      // The ranked documents' result entries, from perform_search's own
      // evaluation pushed down to just these documents (see the filter on
      // perform_search_operation), so they carry the same hits. A Term's
      // result is its postings whatever the filter, so it gets none.
      std::shared_ptr<IPostings> postings;
      if (expr.operation == Operation::Term) {
        postings =
            detail::perform_filtered_search(invidx, expr, nullptr, nullptr);
      } else {
        std::vector<size_t> ordinals;
        ordinals.reserve(hits->size());
        for (const auto &hit : *hits) {
          ordinals.push_back(hit.index);
        }
        std::sort(ordinals.begin(), ordinals.end());
        detail::SearchResult filter;
        std::vector<size_t> none;
        for (auto ordinal : ordinals) {
          filter.push_back(ordinal, none, none);
        }
        postings =
            detail::perform_filtered_search(invidx, expr, nullptr, &filter);
      }
      for (auto &hit : *hits) {
        hit.index =
            detail::find_postings_index_for_ordinal(*postings, hit.index);
        assert(hit.index < postings->size());
      }
      return RankedResult{std::move(postings), std::move(*hits)};
    }
  }

  auto postings = perform_search(invidx, expr);
  auto hits =
      top_k(*postings, k, [&](size_t i) { return scorer(*postings, i); });
  return RankedResult{std::move(postings), std::move(hits)};
}

//-----------------------------------------------------------------------------
// Query parsing
//-----------------------------------------------------------------------------

inline std::optional<Expression> parse_query(Normalizer normalizer,
                                             std::string_view query) {
  return parse_query(to_term_filter(std::move(normalizer)), query);
}

inline std::optional<Expression> parse_query(TermFilter filter,
                                             std::string_view query) {
  return parse_query(nullptr, std::move(filter), query);
}

inline std::optional<Expression>
parse_query(TextSplitter splitter, TermFilter filter, std::string_view query) {
  // A null splitter means "the default", so the rest of this function never
  // has to test for one. This is what makes the two shorter overloads exactly
  // this one with utf8_plain_text_splitter().
  if (!splitter) {
    splitter = utf8_plain_text_splitter();
  }

  // Split the raw query token with the same splitter the documents were
  // indexed with (index side and query side always agree on term
  // boundaries), then run each split piece through the same TermFilter
  // chain an index-side Analyzer<T> would use.
  auto term_handler = [&](std::string_view token) -> Expression {
    // A trailing `*` turns the token into a prefix query, provided it is the
    // token's only `*` -- that keeps the common `foo*` case on the cheaper
    // literal-prefix path (see Operation::Prefix) instead of the general
    // wildcard automaton below. This is resolved here rather than in the
    // grammar so that the star binds tightly to its token (`foo*` is a
    // prefix, `foo *` is not) without having to fight %whitespace skipping.
    // A bare `*` is left alone: it stays an ordinary term that no index can
    // contain, rather than becoming a match-all.
    auto is_prefix = token.size() > 1 && token.back() == '*' &&
                     token.find('*') == token.size() - 1;
    if (is_prefix) {
      token.remove_suffix(1);
    }

    auto run_filter = [&](const std::u32string &str) {
      std::vector<std::u32string> emitted;
      if (filter) {
        filter(str, [&](std::u32string out) { emitted.push_back(std::move(out)); });
      } else {
        emitted.push_back(str);
      }
      return emitted;
    };

    // Any other placement of `*` (leading, interior, or more than one) makes
    // the whole token a wildcard pattern instead of a prefix. Handled
    // separately from build() below, because the splitter treats `*` as a
    // separator and would otherwise fragment the token into unrelated words
    // -- the same way it fragments `well-known` -- which would lose the
    // pattern structure. Each `*`-delimited piece is filtered as one opaque
    // chunk rather than re-split into its own words, so a
    // wildcard segment that itself contains punctuation (`well-kno*n`) is not
    // decomposed the way a plain phrase term would be; out of scope for v1.
    //
    // The splitter is not applied here for the same reason: a `*`-delimited
    // piece is a pattern fragment, not a term, and segmenting it would insert
    // boundaries the pattern never asked for (東京タワ* must stay one prefix
    // fragment, not become 東京 followed by a pattern starting at タワ).
    auto build_wildcard = [&]() -> Expression {
      std::u32string pattern;
      size_t start = 0;
      while (true) {
        auto star_pos = token.find('*', start);
        auto segment = token.substr(
            start, star_pos == std::string_view::npos
                       ? std::string_view::npos
                       : star_pos - start);
        if (!segment.empty()) {
          auto emitted = run_filter(u32(segment));
          // A filter that expands one chunk into several (synonyms) has no
          // sensible meaning inside a wildcard pattern, so only the first
          // survives; one that drops the chunk (stop word) just closes the
          // gap, the same treatment build() gives a dropped word.
          if (!emitted.empty()) {
            pattern += emitted[0];
          }
        }
        if (star_pos == std::string_view::npos) {
          break;
        }
        // Collapses consecutive/duplicate stars (`a**b`) to one, which is
        // semantically identical and keeps the automaton's state count down.
        if (pattern.empty() || pattern.back() != U'*') {
          pattern += U'*';
        }
        start = star_pos + 1;
      }
      return Expression{Operation::Wildcard, pattern};
    };

    if (!is_prefix && token.size() > 1 &&
        token.find('*') != std::string_view::npos) {
      return build_wildcard();
    }

    auto to_expression = [](std::vector<std::u32string> emitted) {
      // A single emit is a plain Term; a filter that expanded one token into
      // several (e.g. synonyms) maps to an Or, never an implicit Adjacent
      // phrase (that would build the wrong query, e.g. "usa united states").
      if (emitted.size() == 1) {
        return Expression{Operation::Term, emitted[0]};
      }
      std::vector<Expression> nodes;
      for (auto &term : emitted) {
        nodes.push_back(Expression{Operation::Term, term});
      }
      return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
    };

    auto build = [&]() -> Expression {
      // One node per term position. Terms the splitter starts at the same
      // byte are alternatives at one position (TextSplitter's stacking
      // rule), so they and their filter expansions pool into one Or -- the
      // shape a filter's 1->N already takes -- and a new start opens the
      // next node. A position every term was dropped from (stop words)
      // leaves no node, closing the gap as Analyzer<T> does on the index
      // side.
      std::vector<Expression> nodes;
      std::vector<std::u32string> at_position;
      constexpr auto npos = std::numeric_limits<size_t>::max();
      size_t last_start = npos;
      auto close_position = [&]() {
        if (!at_position.empty()) {
          nodes.push_back(to_expression(std::move(at_position)));
          at_position.clear();
        }
      };
      bool split_any = false;
      splitter(token, [&](const std::u32string &str, TextRange range) {
        split_any = true;
        if (range.position != last_start) {
          close_position();
          last_start = range.position;
        }
        for (auto &term : run_filter(str)) {
          if (std::find(at_position.begin(), at_position.end(), term) ==
              at_position.end()) {
            at_position.push_back(std::move(term));
          }
        }
      });
      close_position();

      if (!split_any) {
        // No letter sequence in the token (e.g. digits only); such a term can
        // never exist in the index. Run it through the filter as-is so a
        // configured chain (e.g. lowercasing) still applies, but an empty
        // result still matches nothing.
        auto emitted = run_filter(u32(token));
        if (emitted.empty()) {
          return Expression{Operation::Term, u32(token)};
        }
        return to_expression(std::move(emitted));
      }

      if (nodes.empty()) {
        // Every split piece was dropped by the filter; matches nothing.
        return Expression{Operation::Term, std::u32string()};
      }
      if (nodes.size() == 1) {
        return nodes[0];
      }
      // A token split into multiple pieces (e.g. `well-known`) is an implicit
      // phrase.
      return Expression{Operation::Adjacent, std::u32string(),
                        detail::DEFAULT_NEAR_SIZE, std::move(nodes)};
    };

    auto expr = build();
    if (is_prefix) {
      return detail::as_prefix(std::move(expr));
    }
    return expr;
  };

  return detail::parse_query_impl(term_handler, query);
}

//-----------------------------------------------------------------------------
// Compressed index loading
//-----------------------------------------------------------------------------

template <typename Key>
std::shared_ptr<IInvertedIndexWithTextRange<TextRange, Key>>
load_compressed_index(std::istream &is) {
  auto index = std::make_shared<detail::CompressedInvertedIndex<Key>>();
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

} // namespace searchlib

#undef SEARCHLIB_ALWAYS_INLINE
