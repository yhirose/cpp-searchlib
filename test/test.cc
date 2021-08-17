﻿#include <searchlib.h>
#include <fstream>
#include <sstream>
#include <string>
#include <catch2/catch_test_macros.hpp>

#include "lib/unicodelib.h"
#include "utils.h"

#ifdef _MSC_BUILD
const auto KJV_PATH = "../../test/t_kjv.tsv";
#else
const auto KJV_PATH = "../test/t_kjv.tsv";
#endif

std::vector<std::string> split(const std::string &input, char delimiter) {
  std::istringstream ss(input);
  std::string field;
  std::vector<std::string> result;
  while (std::getline(ss, field, delimiter)) {
    result.push_back(field);
  }
  return result;
}

std::u32string to_lowercase(std::u32string str) {
  std::transform(str.begin(), str.end(), str.begin(),
                 [](auto c) { return std::tolower(c); });
  return str;
}

std::vector<std::string> sample_data = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third.",
    "Fourth document",
    "Hello World!",
};

void sample_index(searchlib::InvertedIndex &index,
                  searchlib::TextRangeList &text_range_list) {
  index.normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

  size_t document_id = 0;
  for (const auto &s : sample_data) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(s, index.normalizer,
                                                text_ranges);

    searchlib::indexing(index, tokenizer, document_id);

    text_range_list.emplace(document_id, std::move(text_ranges));
    document_id++;
  }
}

void kjv_index(searchlib::InvertedIndex &index,
               searchlib::TextRangeList &text_range_list) {
  index.normalizer = [](auto sv) { return unicode::to_lowercase(sv); };
  std::ifstream fs(KJV_PATH);
  std::string line;
  while (std::getline(fs, line)) {
    auto fields = split(line, '\t');
    auto document_id = std::stoi(fields[0]);
    const auto &s = fields[4];

    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(s, index.normalizer,
                                                text_ranges);

    searchlib::indexing(index, tokenizer, document_id);

    text_range_list.emplace(document_id, std::move(text_ranges));
  }
}

TEST_CASE("UTF8PlainTextTokenizer Test", "[tokenizer]") {
  std::vector<std::vector<std::string>> expected = {
      {"this", "is", "the", "first", "document"},
      {"this", "is", "the", "second", "document"},
      {"this", "is", "the", "third", "document", "this", "is", "the", "second",
       "sentence", "in", "the", "third"},
      {"fourth", "document"},
      {"hello", "world"},
  };

  size_t document_id = 0;
  for (const auto &s : sample_data) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(
        s, [](auto sv) { return unicode::to_lowercase(sv); }, text_ranges);
    std::vector<std::string> actual;
    tokenizer.tokenize(
        [&](auto &str, auto) { actual.emplace_back(searchlib::u8(str)); });
    REQUIRE(actual == expected[document_id]);
    document_id++;
  }
}

TEST_CASE("Parsing query Test", "[query]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    REQUIRE(expr != std::nullopt);
    REQUIRE((*expr).operation == searchlib::Operation::Term);
    REQUIRE((*expr).term_id == 2);
  }

  {
    auto expr = parse_query(index, " nothing ");
    REQUIRE(expr == std::nullopt);
  }
}

TEST_CASE("Term search Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 3);

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

    REQUIRE(result->size() == 2);

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

TEST_CASE("And search Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " the second third ");

    REQUIRE(expr->operation == searchlib::Operation::And);
    REQUIRE(expr->nodes.size() == 3);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 1);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 6);

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 3);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 12);
        REQUIRE(rng.length == 5);
      }

      {
        auto hit_index = 3;
        REQUIRE(result->term_position(index, hit_index) == 8);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 40);
        REQUIRE(rng.length == 6);
      }

      {
        auto hit_index = 5;
        REQUIRE(result->term_position(index, hit_index) == 12);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 63);
        REQUIRE(rng.length == 5);
      }
    }
  }
}

TEST_CASE("Or search Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " third | HELLO | second ");

    REQUIRE(expr->operation == searchlib::Operation::Or);
    REQUIRE(expr->nodes.size() == 3);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 3);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 1);
      REQUIRE(result->search_hit_count(index) == 1);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 3);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 12);
        REQUIRE(rng.length == 6);
      }
    }

    {
      auto index = 1;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 3);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 3);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 12);
        REQUIRE(rng.length == 5);
      }

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 8);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 40);
        REQUIRE(rng.length == 6);
      }

      {
        auto hit_index = 2;
        REQUIRE(result->term_position(index, hit_index) == 12);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 63);
        REQUIRE(rng.length == 5);
      }
    }

    {
      auto index = 2;
      REQUIRE(result->document_id(index) == 4);
      REQUIRE(result->search_hit_count(index) == 1);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 0);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 0);
        REQUIRE(rng.length == 5);
      }
    }
  }
}

TEST_CASE("Adjacent search Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( "is the" )");

    REQUIRE(expr->operation == searchlib::Operation::Adjacent);
    REQUIRE(expr->nodes.size() == 2);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 3);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 0);
      REQUIRE(result->search_hit_count(index) == 1);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 1);
        REQUIRE(result->term_count(index, hit_index) == 2);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 5);
        REQUIRE(rng.length == 6);
      }
    }

    {
      auto index = 1;
      REQUIRE(result->document_id(index) == 1);
      REQUIRE(result->search_hit_count(index) == 1);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 1);
        REQUIRE(result->term_count(index, hit_index) == 2);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 5);
        REQUIRE(rng.length == 6);
      }
    }

    {
      auto index = 2;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 2);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 1);
        REQUIRE(result->term_count(index, hit_index) == 2);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 5);
        REQUIRE(rng.length == 6);
      }

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 6);
        REQUIRE(result->term_count(index, hit_index) == 2);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 33);
        REQUIRE(rng.length == 6);
      }
    }
  }
}

TEST_CASE("Adjacent search with 3 words Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( "the second sentence" )");

    REQUIRE(expr->operation == searchlib::Operation::Adjacent);
    REQUIRE(expr->nodes.size() == 3);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 1);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 1);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 7);
        REQUIRE(result->term_count(index, hit_index) == 3);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 36);
        REQUIRE(rng.length == 19);
      }
    }
  }
}

TEST_CASE("Near search Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( second ~ document )");

    REQUIRE(expr->operation == searchlib::Operation::Near);
    REQUIRE(expr->nodes.size() == 2);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 2);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 1);
      REQUIRE(result->search_hit_count(index) == 2);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 3);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 12);
        REQUIRE(rng.length == 6);
      }

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 4);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 19);
        REQUIRE(rng.length == 8);
      }
    }

    {
      auto index = 1;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 2);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 4);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 18);
        REQUIRE(rng.length == 8);
      }

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 8);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 40);
        REQUIRE(rng.length == 6);
      }
    }
  }
}

TEST_CASE("Near search with phrase Test", "[memory]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( sentence ~ "is the" )");

    REQUIRE(expr->operation == searchlib::Operation::Near);
    REQUIRE(expr->nodes.size() == 2);

    auto result = perform_search(index, *expr);

    REQUIRE(result->size() == 1);

    {
      auto index = 0;
      REQUIRE(result->document_id(index) == 2);
      REQUIRE(result->search_hit_count(index) == 2);

      {
        auto hit_index = 0;
        REQUIRE(result->term_position(index, hit_index) == 6);
        REQUIRE(result->term_count(index, hit_index) == 2);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 33);
        REQUIRE(rng.length == 6);
      }

      {
        auto hit_index = 1;
        REQUIRE(result->term_position(index, hit_index) == 9);
        REQUIRE(result->term_count(index, hit_index) == 1);

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        REQUIRE(rng.position == 47);
        REQUIRE(rng.length == 8);
      }
    }
  }
}

TEST_CASE("KJB Test", "[kjv]") {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;

  kjv_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( apple )");
    auto result = perform_search(index, *expr);
    REQUIRE(result->size() == 8);
  }

  {
    auto expr = parse_query(index, R"( "apple tree" )");
    auto result = perform_search(index, *expr);
    REQUIRE(result->size() == 3);
  }
}

TEST_CASE("UTF8 decode performance Test", "[kjv]") {
  // auto normalizer = [](const auto &str) {
  //   return unicode::to_lowercase(str);
  // };
  auto normalizer = to_lowercase;

  std::ifstream fs(KJV_PATH);

  std::string s;
  while (std::getline(fs, s)) {
    std::vector<searchlib::TextRange> text_ranges;

    searchlib::UTF8PlainTextTokenizer tokenizer(s, normalizer, text_ranges);

    tokenizer.tokenize([&](auto &str, auto) {});
  }
}

