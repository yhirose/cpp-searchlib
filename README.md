# cpp-searchlib

Full-text search engine (WIP)

```cpp
std::vector<std::string> sample_data = {
  "This is the first document.",
  "This is the second document.",
  "This is the third document. This is the second sentence in the third document."
};

searchlib::Index index;
index.normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

searchlib::TextRangeList text_range_list;

// Indexing...
size_t document_id = 0;
for (const auto &s : sample_data) {
  std::vector<searchlib::TextRange> text_ranges;
  searchlib::UTF8PlainTextTokenizer tokenizer(s, index.normalizer, text_ranges);

  searchlib::indexing(index, tokenizer, document_id);

  text_range_list.emplace(document_id, std::move(text_ranges));
  document_id++;
}

// Search...
auto expr = searchlib::parse_query(index, R"( first | "the second sentence" )");

auto result = searchlib::perform_search(index, *expr);
result->size();
result->document_id(index);
result->search_hit_count(index);
result->term_position(index, search_hit_index);
result->term_count(index, search_hit_index);

auto rng = searchlib::text_range(text_range_list, *result, index, search_hit_index);
rng.position;
rng.length;
```
