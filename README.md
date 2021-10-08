# cpp-searchlib

C++17 full-text search engine library (WIP. Far from release...)

TODO:
- [ ] Save/load index to/from storage
- [ ] Posting list compression
- [ ] Search scope (document, section, paragraph)

```cpp
std::vector<std::string> documents = {
  "This is the first document.",
  "This is the second document.",
  "This is the third document. This is the second sentence in the third document.",
  "This is not the first document."
};

// Indexing...
searchlib::InvertedIndex index;
searchlib::TextRangeList text_range_list;

{
  searchlib::Indexer::set_normalizer(
    index, [](auto str) { return unicode::to_lowercase(str); });

  size_t document_id = 0;
  for (const auto &doc : documents) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(doc, text_ranges);

    searchlib::Indexer::indexing(index, tokenizer, document_id);

    text_range_list.emplace(document_id, std::move(text_ranges));
    document_id++;
  }
}

// Search...
auto expr = searchlib::parse_query(index, R"( first not | "the second sentence" )");

auto result = searchlib::perform_search(index, *expr);
result->size(); // 2

result->document_id(0); // 2
result->search_hit_count(0); // 1

  // 'the second sentence'
  result->term_position(0, 1); // 7
  result->term_length(0, 1); // 3
  auto rng = searchlib::text_range(text_range_list, *result, 0, 1);
  rng.position; // 36
  rng.length; // 19

result->document_id(1); // 3
result->search_hit_count(1); // 2

  // 'not'
  result->term_position(1, 0); // 2
  result->term_length(1, 0); // 1
  auto rng = searchlib::text_range(text_range_list, *result, 1, 0);
  rng.position; // 8
  rng.length; // 3

  // 'first'
  result->term_position(1, 1); // 4
  result->term_length(1, 1); // 1
  auto rng = searchlib::text_range(text_range_list, *result, 1, 1);
  rng.position; // 16
  rng.length; // 5
```
