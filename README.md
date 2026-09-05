# cpp-searchlib

C++17 full-text search engine library (WIP. Far from release...)

TODO:
- [x] Save/load index to/from storage
- [ ] Posting list compression
- [ ] Search scope (document, section, paragraph)

## Usage

```cpp
using namespace searchlib;

std::vector<std::string> documents = {
  "This is the first document.",
  "This is the second document.",
  "This is the third document. This is the second sentence in the third document.",
  "This is not the first document.",
};

auto normalizer = [](const auto &str) { return unicode::to_lowercase(str); };

// Indexing...
InMemoryInvertedIndex<TextRange> invidx;
InMemoryIndexer<TextRange> indexer(invidx, normalizer);

// The key is yours: any size_t, in any order. The index numbers documents
// itself underneath (see IPostings::document_ordinal in searchlib.h).
size_t document_key = 0;
for (const auto &doc : documents) {
  indexer.index_document(document_key, UTF8PlainTextTokenizer(doc));
  document_key++;
}

// Search...
auto expr = parse_query(normalizer, R"( document -third )");
auto result = perform_search(invidx, *expr);
BM25Scorer scorer(invidx, *expr);

for (size_t i = 0; i < result->size(); i++) {
  // A result speaks ordinals; the index maps them back to your keys.
  auto document_key = invidx.document_key(result->document_ordinal(i));
  auto score = scorer(*result, i);

  for (size_t hit = 0; hit < result->search_hit_count(i); hit++) {
    // Text range for highlighting (UTF-8 byte position and length)
    auto rng = invidx.text_range(*result, i, hit);
    // documents[document_key].substr(rng.position, rng.length)
  }
}
```

## Query syntax

| Syntax | Description |
|---|---|
| `apple banana` | AND - documents containing all terms |
| `apple \| banana` | OR |
| `apple -banana` | NOT - exclude documents containing the term |
| `"apple tree"` | Phrase - adjacent terms |
| `apple ~ tree` | NEAR - terms within 4 term positions |
| `app*` | Prefix - every term starting with `app` |
| `a*e`, `*ana`, `app*ion` | Wildcard - `*` matches zero or more characters anywhere in the term |
| `apple~2` | Fuzzy - terms within 2 edits of `apple` |
| `( ... )` | Grouping |

Terms are tokenized and normalized in the same way as documents, so Unicode
terms work as long as the tokenizer indexed them. Languages written without
spaces (Japanese, Chinese) need a segmenting splitter on both sides to be
searchable at all -- see [Japanese word segmentation](#japanese-word-segmentation).
`NOT` is only valid along with at least one positive term.

A trailing `*` expands against the index's dictionary at search time, so
`app*` is equivalent to an `OR` over every indexed term starting with `app`,
scoring included. The star has to touch its term (`app*`, not `app *`), and a
bare `*` is not a match-all: it stays an ordinary term, which no index can
contain. The prefix is enumerated with
`IInvertedIndex::enumerate_terms_with_prefix`, which is also usable directly:

```cpp
index.enumerate_terms_with_prefix(U"app", [](const auto &term) {
  std::cout << u8(term) << std::endl;
});
```

The two backends have different cost shapes. The writable in-memory index
scans its whole dictionary (a hash map), so it pays for the entire vocabulary
no matter how selective the prefix is. An index loaded with
`load_compressed_index` descends the FST its dictionary is stored as, so it
pays per matching term instead.

On the KJV corpus (12,594 terms) that is a fixed ~85us for the hash map
against ~60ns per match for the FST, so the FST is far faster for the
selective prefixes real queries use (about 11x for a 148-term prefix, 100x for
a 4-term one) and only loses once the prefix matches more than roughly a tenth
of the vocabulary.

`perform_search` enumerates once per query, but `bm25_score` takes an
expression per hit, so scoring a prefix query with it repeats that lookup for
every hit. `BM25Scorer` expands once at construction instead:

```cpp
auto expr = *parse_query(normalizer, "app*");
auto result = perform_search(index, expr);
BM25Scorer scorer(index, expr);
auto hits = top_k(*result, 10, [&](size_t i) { return scorer(*result, i); });
```

A `*` anywhere else in a term -- leading, interior, or more than one -- is a
wildcard instead of a prefix: `a*e` matches `apple`, `*ana` matches `banana`,
`app*ion` matches `application`. It is backed by
`IInvertedIndex::enumerate_terms_with_wildcard` and expanded the same way
(`expand_wildcards` before scoring). Only `*` is supported, matching zero or
more characters; there is no `?` or character class.

The two backends answer it differently, same as prefix search: the in-memory
index tests every term against the pattern, while the compressed backend
walks the FST with a small automaton (`step`/`is_match`/`can_match`, the same
shape as fstlib's own `edit_distance_search`) that prunes any subtree the
pattern cannot match. It still visits more of the FST than a literal prefix
does -- a `*` can match anything, so descent can't be confined to one subtree
-- which is why the common single-trailing-`*` case stays on the cheaper
Prefix path instead of going through the wildcard automaton.

### Fuzzy search

`term~N` matches every term within `N` Levenshtein edits (insertion, deletion,
substitution) of `term`, counted in codepoints rather than bytes, so it works
on non-ASCII text. `apple~1` finds `ample` and `apply`; `sanctifed~1` finds
`sanctified`.

The `~` has to touch its term and be followed by digits, which is what keeps
it apart from the NEAR operator: `apple~2` is a fuzzy query, while `apple ~ 2`
and `apple~tree` are still NEAR. `N` is capped at 2 (as it is in Lucene): a
larger distance defeats the pruning both backends depend on and matches most
of the dictionary. The underlying
`IInvertedIndex::enumerate_terms_with_edit_distance` takes any distance, since
a caller naming one directly is not untrusted input.

```cpp
index.enumerate_terms_with_edit_distance(U"apple", 1, [](const auto &term) {
  std::cout << u8(term) << std::endl;
});
```

Like a prefix or wildcard query, this expands to an `OR` over the matching
terms, so `expand_fuzzy` is worth calling once before scoring for the same
reason `expand_prefixes` is. Every matching term contributes equally to the
score -- unlike Lucene, a closer edit distance carries no boost.

The compressed backend walks the FST with fstlib's own `LevenshteinAutomaton`,
which prunes a subtree as soon as every alignment through it already costs
more than `N`. The in-memory backend tests each term with a rolling-row DP,
skipping any term whose length alone puts it out of range.

## Scoring

`term_count_score`, `tf_idf_score` and `bm25_score` are available to score
each search result. Ranking is up to the caller.

`top_k` collects the k highest-scoring hits using a bounded min-heap
(`O(n log k)` instead of scoring and sorting every hit):

```cpp
BM25Scorer scorer(invidx, *expr);
auto hits = top_k(*result, 10, [&](size_t i) { return scorer(*result, i); });

for (const auto &hit : hits) {
  auto document_key = invidx.document_key(result->document_ordinal(hit.index));
  // hit.score
}
```

## Persistence

An index can be saved to and loaded from disk, so it does not have to be
rebuilt on every startup.

```cpp
// Save...
InMemoryInvertedIndex<TextRange> invidx;
// ... index documents ...
invidx.save("index.bin");

// Load...
InMemoryInvertedIndex<TextRange> loaded;
loaded.load("index.bin");
```

`save`/`load` also have `std::ostream`/`std::istream` overloads. The built-in
`TextRange` value type is serialized automatically; for a custom text-range
type `T`, pass a serializer/deserializer pair:

```cpp
invidx.save(os, [](std::ostream &os, const T &v) { /* write v */ });
loaded.load(is, [](std::istream &is) -> T { /* read and return a T */ });
```

The default on-disk format is a "plain" host-native dump (`format_type` 0)
tagged with a `format_type`/`schema_version` header. It is intended to be
loaded on the same platform that wrote it.

### Compressed format

`IndexFormat::Compressed` (`format_type` 2) stores the postings and text
ranges as Elias-Fano sequences and the term dictionary as an FST, which on
the KJV corpus brings the index down to about 20% of the plain size:

```cpp
invidx.save("index.bin", {}, IndexFormat::Compressed);
```

It can be read back into a normal `InMemoryInvertedIndex` with `load`, or
opened as an immutable, memory-lean index that answers queries straight off
the compressed structures without expanding them:

```cpp
auto index = load_compressed_index("index.bin");
```

Being immutable, it is safe to share across reader threads without external
locking.

The FST dictionary is what makes this backend both small and good at prefix
search: on KJV it holds the same 12,594 terms in 54KB instead of 459KB, and
answers a selective prefix 11x to 100x faster than a hash scan. The trade is
exact term lookup, which walks the FST instead of hashing once: 181ns against
20.5ns, so about 160ns more per lookup.

## Multi-field schema

Documents with several distinct text fields (title/body/tags) each get their
own `InMemoryInvertedIndex`, grouped by name in a `MultiFieldIndex`. All
fields of one document share the same key, and every hit carries it as
`document_key`, so hits from different fields for the same document are
grouped by a plain comparison on that -- the fields' own ordinals differ, and
never need to be compared:

```cpp
MultiFieldIndex<TextRange> index;

InMemoryIndexer(index.field("title"), normalizer)
    .index_document(0, UTF8PlainTextTokenizer("The Great Gatsby"));
InMemoryIndexer(index.field("body"), normalizer)
    .index_document(0, UTF8PlainTextTokenizer("A story about wealth."));

// Field-qualified search: just call perform_search on one field's index.
auto title_hits = perform_search(index.field("title"), *expr);

// Search every field and get back hits tagged with their field name;
// combining/ranking across fields is left to the caller.
auto hits = perform_multi_field_search(index, *expr);
```

There is no query-string `field:` syntax; field selection is a C++-level
choice. `MultiFieldIndex::save`/`load` persist every field.

## Japanese word segmentation

The default tokenizer cuts terms at runs of Unicode letters, which does not
work for a language written without spaces: `私は東京タワーに行った` is one
letter run, so it indexes as one enormous term and nothing inside it can be
found. `load_segmenting_splitter` returns a `TextSplitter` that adds word
boundaries inside such runs, using the vendored
[cpp-segmentlib](https://github.com/yhirose/cpp-segmentlib):

```cpp
#include <searchlib_segment.h>

// One splitter, shared by both sides. This is what keeps their term
// boundaries identical -- using it on only one side gives an index where
// 東京 can be found but 東京タワー cannot.
auto splitter = load_segmenting_splitter("ja-ud-gsd.mod");

// Indexing: wrap it in SplitterTokenizer, which adds the term positions.
indexer.index_document(0, SplitterTokenizer(splitter, "私は東京タワーに行った"));
// indexed as: 私 / は / 東京 / タワー / に / 行っ / た

// Search: pass the same splitter to parse_query.
auto expr = parse_query(splitter, nullptr, "東京タワー");
// -> Adjacent(東京, タワー), an implicit phrase, so it matches the document
//    above but not a document merely containing 東京 and タワー separately.
```

A query token the splitter cuts up becomes an implicit phrase, the same
treatment `well-known` gets. Insert a space to get an `AND` instead.

Only runs containing Han, Hiragana or Katakana go through the model, so text
with no CJK in it is split byte-for-byte the way the default splitter splits
it -- a Japanese model would otherwise shred it (`iPhone` into `i`/`Phone`).
`Analyzer<T>` chains, prefix (`東京タワ*`) and fuzzy (`東京タワー~1`) all
compose with it as usual.

Note that switching an existing index to a different splitter requires a full
re-index: the term boundaries change, and with them `document_term_count` and
every BM25 score.

The model file is supplied by the caller; upstream's 2.1 MB MLP reference
model is used by the tests as `test/models/ja-ud-gsd.mod`. **It is licensed
CC BY-SA 4.0, not MIT like this repository's code** (it derives from the
UD_Japanese-GSD treebank) -- see `test/models/NOTICE` before redistributing it.

## Cutting words into subwords

A language written with spaces is already cut into words by the default
splitter; what a morphological analyzer for it adds is cutting each word into
its morphemes. That is a `string -> list<string>` function, and
`subword_splitter` lifts one into a `TextSplitter`, giving every piece its
own position and byte range:

```cpp
auto splitter = subword_splitter(nullptr, [](std::string_view word) {
  return my_analyzer.morphemes(word);   // e.g. 한국어를 -> 한국어, 를
});

indexer.index_document(0, SplitterTokenizer(splitter, text));
auto expr = parse_query(splitter, nullptr, "한국어를");   // -> Adjacent(한국어, 를)
```

The pieces are a sequence, so a query for the whole word becomes an implicit
phrase over them, the same way a token the segmenting splitter cuts up does.
This is the difference from a `TermFilter` that emits several times: those
outputs are alternatives (a synonym set), which `parse_query` turns into an
`OR` and which the index side refuses, since alternatives have no place at
consecutive positions. The pieces must be the word's own bytes in order (an
analyzer that answers with a lemma rather than the surface form should be a
`TextSplitter` of its own, carrying its offsets); an empty answer drops the
word.

## CLI

`cli/` builds a small `searchlib-cli` executable exercising the library:

```sh
# Index every file under a directory into INDEX_PATH. Each file's path is
# its document key, so a hit comes back as the path it was read from.
searchlib-cli index SOURCE INDEX_PATH

# Search INDEX_PATH, printing the top hits ranked by BM25 with their
# matching text ranges.
searchlib-cli search INDEX_PATH QUERY [-n N]
```
