# cpp-searchlib

C++17 full-text search engine library (WIP. Far from release...)

TODO:
- [ ] Save/load index to/from storage
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
| `( ... )` | Grouping |

Terms are tokenized and normalized in the same way as documents, so Unicode
terms (e.g. Japanese) work as long as the tokenizer indexed them. `NOT` is
only valid along with at least one positive term.

## Scoring

`term_count_score`, `tf_idf_score` and `bm25_score` are available to score
each search result. Ranking is up to the caller.
