﻿#include <gtest/gtest.h>
#include <searchlib.h>

#include "test_utils.h"

std::vector<std::string> sample_documents = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third.",
    "Fourth document",
    "Hello World!",
};

void sample_index(searchlib::InvertedIndex &index,
                  searchlib::TextRangeList &text_range_list) {
  searchlib::Indexer::set_normalizer(
      index, [](auto sv) { return unicode::to_lowercase(sv); });

  size_t document_id = 0;
  for (const auto &doc : sample_documents) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(doc, &text_ranges);

    searchlib::Indexer::indexing(index, document_id, tokenizer);

    text_range_list.emplace(document_id, std::move(text_ranges));
    document_id++;
  }

  EXPECT_EQ(sample_documents.size(), index.document_count());

  auto term = U"the";
  EXPECT_EQ(5, index.term_occurrences(term));
}

TEST(TokenizerTest, UTF8PlainTextTokenizer) {
  std::vector<std::vector<std::string>> expected = {
      {"this", "is", "the", "first", "document"},
      {"this", "is", "the", "second", "document"},
      {"this", "is", "the", "third", "document", "this", "is", "the", "second",
       "sentence", "in", "the", "third"},
      {"fourth", "document"},
      {"hello", "world"},
  };

  size_t document_id = 0;
  for (const auto &doc : sample_documents) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(doc, &text_ranges);
    std::vector<std::string> actual;
    tokenizer.tokenize(
        [](auto sv) { return unicode::to_lowercase(sv); },
        [&](auto &str, auto) { actual.emplace_back(searchlib::u8(str)); });
    EXPECT_EQ(expected[document_id], actual);
    document_id++;
  }
}

TEST(QueryTest, ParsingQuery) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    EXPECT_NE(std::nullopt, expr);
    EXPECT_EQ(searchlib::Operation::Term, (*expr).operation);
    EXPECT_EQ(U"the", (*expr).term_str);
  }

  {
    auto expr = parse_query(index, " nothing ");
    EXPECT_EQ(std::nullopt, expr);
  }
}

TEST(TermTest, TermSearch) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " The ");
    auto result = perform_search(index, *expr);

    EXPECT_EQ(3, result->size());

    {
      auto index = 0;
      EXPECT_EQ(0, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      EXPECT_EQ(2, result->term_position(index, 0));
      EXPECT_EQ(1, result->term_count(index, 0));

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      EXPECT_EQ(8, rng.position);
      EXPECT_EQ(3, rng.length);
    }

    {
      auto index = 2;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(3, result->search_hit_count(index));

      EXPECT_EQ(2, result->term_position(index, 0));
      EXPECT_EQ(1, result->term_count(index, 0));

      EXPECT_EQ(7, result->term_position(index, 1));
      EXPECT_EQ(1, result->term_count(index, 1));

      EXPECT_EQ(11, result->term_position(index, 2));
      EXPECT_EQ(1, result->term_count(index, 2));

      auto rng = searchlib::text_range(text_range_list, *result, index, 2);
      EXPECT_EQ(59, rng.position);
      EXPECT_EQ(3, rng.length);
    }
  }

  {
    auto expr = parse_query(index, " second ");
    auto result = perform_search(index, *expr);

    EXPECT_EQ(2, result->size());

    {
      auto index = 0;
      EXPECT_EQ(1, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      EXPECT_EQ(3, result->term_position(index, 0));
      EXPECT_EQ(1, result->term_count(index, 0));

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      EXPECT_EQ(12, rng.position);
      EXPECT_EQ(6, rng.length);
    }

    {
      auto index = 1;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      EXPECT_EQ(8, result->term_position(index, 0));
      EXPECT_EQ(1, result->term_count(index, 0));

      auto rng = searchlib::text_range(text_range_list, *result, index, 0);
      EXPECT_EQ(40, rng.position);
      EXPECT_EQ(6, rng.length);
    }
  }
}

TEST(AndTest, AndSearch) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " the second third ");

    EXPECT_EQ(searchlib::Operation::And, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(1, result->size());

    {
      auto index = 0;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(6, result->search_hit_count(index));

      {
        auto hit_index = 1;
        EXPECT_EQ(3, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 3;
        EXPECT_EQ(8, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 5;
        EXPECT_EQ(12, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(OrTest, OrSearch) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, " third | HELLO | second ");

    EXPECT_EQ(searchlib::Operation::Or, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(3, result->size());

    {
      auto index = 0;
      EXPECT_EQ(1, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(3, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 2;
        EXPECT_EQ(12, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }

    {
      auto index = 2;
      EXPECT_EQ(4, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(0, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(0, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearch) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( "is the" )");

    EXPECT_EQ(searchlib::Operation::Adjacent, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(3, result->size());

    {
      auto index = 0;
      EXPECT_EQ(0, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, result->term_position(index, hit_index));
        EXPECT_EQ(2, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(1, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, result->term_position(index, hit_index));
        EXPECT_EQ(2, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 2;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(2, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, result->term_position(index, hit_index));
        EXPECT_EQ(2, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(6, result->term_position(index, hit_index));
        EXPECT_EQ(2, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearchWith3Words) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( "the second sentence" )");

    EXPECT_EQ(searchlib::Operation::Adjacent, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(1, result->size());

    {
      auto index = 0;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(1, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(7, result->term_position(index, hit_index));
        EXPECT_EQ(3, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(36, rng.position);
        EXPECT_EQ(19, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearch) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( second ~ document )");

    EXPECT_EQ(searchlib::Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(2, result->size());

    {
      auto index = 0;
      EXPECT_EQ(1, result->document_id(index));
      EXPECT_EQ(2, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(4, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(19, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(2, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(4, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(18, rng.position);
        EXPECT_EQ(8, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearchWithPhrase) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;
  sample_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( sentence ~ "is the" )");

    EXPECT_EQ(searchlib::Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto result = perform_search(index, *expr);

    EXPECT_EQ(1, result->size());

    {
      auto index = 0;
      EXPECT_EQ(2, result->document_id(index));
      EXPECT_EQ(2, result->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(6, result->term_position(index, hit_index));
        EXPECT_EQ(2, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(9, result->term_position(index, hit_index));
        EXPECT_EQ(1, result->term_count(index, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *result, index, hit_index);
        EXPECT_EQ(47, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }
  }
}

TEST(TF_IDF_Test, TF_IDF) {
  searchlib::InvertedIndex index;

  std::vector<std::string> documents = {
      "apple orange orange banana",
      "banana orange strawberry strawberry grape",
  };

  size_t document_id = 0;
  for (const auto &doc : documents) {
    searchlib::UTF8PlainTextTokenizer tokenizer(doc);
    searchlib::Indexer::indexing(index, document_id, tokenizer);
    document_id++;
  }

  {
    auto term = U"apple";

    EXPECT_EQ(1, index.df(term));
    EXPECT_AP(1, index.idf(term));

    EXPECT_EQ(0.25, index.tf(term, 0));
    EXPECT_AP(0.25, index.tf_idf(term, 0));

    EXPECT_EQ(0, index.tf(term, 1));
    EXPECT_AP(0, index.tf_idf(term, 1));
  }

  {
    auto term = U"orange";

    EXPECT_EQ(2, index.df(term));
    EXPECT_EQ(0, index.idf(term));

    EXPECT_EQ(0.5, index.tf(term, 0));
    EXPECT_AP(0, index.tf_idf(term, 0));

    EXPECT_EQ(0.2, index.tf(term, 1));
    EXPECT_AP(0, index.tf_idf(term, 1));
  }

  {
    auto term = U"banana";

    EXPECT_EQ(2, index.df(term));
    EXPECT_EQ(0, index.idf(term));

    EXPECT_EQ(0.25, index.tf(term, 0));
    EXPECT_AP(0, index.tf_idf(term, 0));

    EXPECT_EQ(0.2, index.tf(term, 1));
    EXPECT_AP(0, index.tf_idf(term, 1));
  }

  {
    auto term = U"strawberry";

    EXPECT_EQ(1, index.df(term));
    EXPECT_AP(1, index.idf(term));

    EXPECT_EQ(0, index.tf(term, 0));
    EXPECT_AP(0, index.tf_idf(term, 0));

    EXPECT_EQ(0.4, index.tf(term, 1));
    EXPECT_AP(0.4, index.tf_idf(term, 1));
  }

  {
    auto term = U"grape";

    EXPECT_EQ(1, index.df(term));
    EXPECT_AP(1, index.idf(term));

    EXPECT_EQ(0, index.tf(term, 0));
    EXPECT_AP(0, index.tf_idf(term, 0));

    EXPECT_EQ(0.2, index.tf(term, 1));
    EXPECT_AP(0.2, index.tf_idf(term, 1));
  }
}
