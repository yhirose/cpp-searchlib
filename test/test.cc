#include <searchlib.h>

#include "lib/unicodelib.h"
#include "utils.h"

#define CATCH_CONFIG_MAIN
#include "catch.hpp"

using namespace std;

std::vector<std::string> sample_data = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third "
    "document.",
};

auto sample_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

void sample_index(searchlib::Index &index,
                  searchlib::TextRangeList &text_range_list) {
  index.normalizer = sample_normalizer;

  size_t document_id = 0;
  for (const auto &s : sample_data) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::PlainUTF8Tokenizer tokenizer(s, index.normalizer, text_ranges);

    searchlib::indexing(index, tokenizer, document_id);

    text_range_list.emplace(document_id, std::move(text_ranges));
    document_id++;
  }
}

TEST_CASE("PlainUTF8Tokenizer Test", "[tokenizer]") {
  std::vector<std::vector<std::string>> expected = {
      {"this", "is", "the", "first", "document"},
      {"this", "is", "the", "second", "document"},
      {"this", "is", "the", "third", "document", "this", "is", "the", "second",
       "sentence", "in", "the", "third", "document"},
  };

  size_t document_id = 0;
  for (const auto &s : sample_data) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::PlainUTF8Tokenizer tokenizer(s, sample_normalizer, text_ranges);
    std::vector<std::string> actual;
    tokenizer.tokenize(
        [&](auto &str, auto) { actual.emplace_back(searchlib::u8(str)); });
    REQUIRE(actual == expected[document_id]);
    document_id++;
  }
}

TEST_CASE("Parsing query Test", "[query]") {
  searchlib::Index index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    REQUIRE(expr != std::nullopt);
    REQUIRE((*expr).operation == searchlib::Operation::Term);
    REQUIRE((*expr).term_id == 2);
  }

  {
    auto expr = parse_query(index, " HELLO ");
    REQUIRE(expr == std::nullopt);
  }
}

TEST_CASE("Term search Test", "[search]") {
  searchlib::Index index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    auto result = perform_search(index, *expr);

    auto document_count = result->document_count();
    REQUIRE(document_count == 3);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 0);
      REQUIRE(result->search_hit_count(index) == 1);

      REQUIRE(result->term_position(index, 0) == 2);
      REQUIRE(result->term_count(index, 0) == 1);

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      REQUIRE(rng.position == 8);
      REQUIRE(rng.length == 3);
    }

    {
      auto index = 2;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 3);

      REQUIRE(result->term_position(index, 0) == 2);
      REQUIRE(result->term_count(index, 0) == 1);

      REQUIRE(result->term_position(index, 1) == 7);
      REQUIRE(result->term_count(index, 1) == 1);

      REQUIRE(result->term_position(index, 2) == 11);
      REQUIRE(result->term_count(index, 2) == 1);

      auto rng = searchlib::text_range(text_range_list, *result, index, 2);
      REQUIRE(rng.position == 59);
      REQUIRE(rng.length == 3);
    }
  }

  {
    auto expr = parse_query(index, " second ");
    auto result = perform_search(index, *expr);

    auto document_count = result->document_count();
    REQUIRE(document_count == 2);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 1);
      REQUIRE(result->search_hit_count(index) == 1);

      REQUIRE(result->term_position(index, 0) == 3);
      REQUIRE(result->term_count(index, 0) == 1);

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      REQUIRE(rng.position == 12);
      REQUIRE(rng.length == 6);
    }

    {
      auto index = 1;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 1);

      REQUIRE(result->term_position(index, 0) == 8);
      REQUIRE(result->term_count(index, 0) == 1);

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      REQUIRE(rng.position == 40);
      REQUIRE(rng.length == 6);
    }
  }
}
