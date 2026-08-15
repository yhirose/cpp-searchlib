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

size_t document_id = 0;
for (const auto &doc : documents) {
  indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
  document_id++;
}

// Search...
auto expr = parse_query(normalizer, R"( document -third )");
auto result = perform_search(invidx, *expr);

for (size_t i = 0; i < result->size(); i++) {
  auto document_id = result->document_id(i);
  auto score = bm25_score(invidx, *expr, *result, i);

  for (size_t hit = 0; hit < result->search_hit_count(i); hit++) {
    // Text range for highlighting (UTF-8 byte position and length)
    auto rng = invidx.text_range(*result, i, hit);
    // documents[document_id].substr(rng.position, rng.length)
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
| `( ... )` | Grouping |

Terms are tokenized and normalized in the same way as documents, so Unicode
terms (e.g. Japanese) work as long as the tokenizer indexed them. `NOT` is
only valid along with at least one positive term.

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

`perform_search` enumerates once per query, but the scoring functions take an
expression per hit, so scoring a prefix query directly repeats that lookup for
every hit. Expand once up front instead:

```cpp
auto expr = expand_prefixes(index, *parse_query(normalizer, "app*"));
auto result = perform_search(index, expr);
auto hits = top_k(*result, 10, [&](size_t i) {
  return bm25_score(index, expr, *result, i);
});
```

## Scoring

`term_count_score`, `tf_idf_score` and `bm25_score` are available to score
each search result. Ranking is up to the caller.

`top_k` collects the k highest-scoring hits using a bounded min-heap
(`O(n log k)` instead of scoring and sorting every hit):

```cpp
auto hits = top_k(*result, 10, [&](size_t i) {
  return bm25_score(invidx, *expr, *result, i);
});

for (const auto &hit : hits) {
  auto document_id = result->document_id(hit.index);
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
own `InMemoryInvertedIndex`, grouped by name in a `MultiFieldIndex`. Because
all fields of one document share the same `document_id`, hits from different
fields for the same document are grouped by a plain `document_id` comparison
-- no per-field id remapping needed:

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

## CLI

`cli/` builds a small `searchlib-cli` executable exercising the library:

```sh
# Index every file under a directory into INDEX_PATH (plus an
# INDEX_PATH.manifest sidecar mapping document ids back to file paths).
searchlib-cli index SOURCE INDEX_PATH

# Search INDEX_PATH, printing the top hits ranked by BM25 with their
# matching text ranges.
searchlib-cli search INDEX_PATH QUERY [-n N]
```
