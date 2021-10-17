# cpp-searchlib

C++17 full-text search engine library (WIP. Far from release...)

TODO:
- [ ] Save/load index to/from storage
- [ ] Posting list compression
- [ ] Search scope (document, section, paragraph)

```cpp
using namespace searchlib;

std::vector<std::string> documents = {
  "This is the first document.",
  "This is the second document.",
  "This is the third document. This is the second sentence in the third document.",
  "This is not the first document."
};

// Indexing...
auto normalizer = [](auto str) { return unicode::to_lowercase(str); };

auto index = make_in_memory_index<TextRange>(normalizer, [&](auto &indexer) {
  size_t document_id = 0;
  for (const auto &doc : documents) {
    indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
    document_id++;
  }
};

// Search...
auto expr = parse_query(*index, normalizer, R"( first not | "the second sentence" )");

auto result = perform_search(*index, *expr);
result->size(); // 2

result->document_id(0); // 2
result->search_hit_count(0); // 1

  // 'the second sentence'
  result->term_position(0, 1); // 7
  result->term_length(0, 1); // 3
  auto [pos, len] = index->text_range(*result, 0, 1); // 36, 19

result->document_id(1); // 3
result->search_hit_count(1); // 2

  // 'not'
  result->term_position(1, 0); // 2
  result->term_length(1, 0); // 1
  auto [pos, len] = index->text_range(*result, 1, 0); // 8, 3

  // 'first'
  result->term_position(1, 1); // 4
  result->term_length(1, 1); // 1
  auto [pos, len] = index->text_range(*result, 1, 1); // 16, 5
```
