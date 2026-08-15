#include <gtest/gtest.h>
#include <searchlib.h>

#include <atomic>
#include <filesystem>
#include <sstream>
#include <thread>

#include "test_utils.h"

using namespace searchlib;

std::vector<std::string> sample_documents = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third.",
    "Fourth document",
    "Hello World!",
    "東京 タワー 港区",
    "A well-known example.",
};

auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

auto sample_index() {
  InMemoryInvertedIndex<TextRange> invidx;

  InMemoryIndexer indexer(invidx, normalizer);
  size_t document_id = 0;
  for (const auto &doc : sample_documents) {
    indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
    document_id++;
  }

  EXPECT_EQ(sample_documents.size(), invidx.document_count());

  auto term = U"the";
  EXPECT_EQ(5, invidx.term_count(term));

  return invidx;
}

TEST(TokenizerTest, UTF8PlainTextTokenizer) {
  std::vector<std::vector<std::string>> expected = {
      {"this", "is", "the", "first", "document"},
      {"this", "is", "the", "second", "document"},
      {"this", "is", "the", "third", "document", "this", "is", "the", "second",
       "sentence", "in", "the", "third"},
      {"fourth", "document"},
      {"hello", "world"},
      {"東京", "タワー", "港区"},
      {"a", "well", "known", "example"},
  };

  size_t document_id = 0;
  for (const auto &doc : sample_documents) {
    UTF8PlainTextTokenizer tokenizer(doc);
    std::vector<std::string> actual;
    tokenizer([](auto sv) { return unicode::to_lowercase(sv); },
              [&](auto &str, auto, auto) { actual.emplace_back(u8(str)); });
    EXPECT_EQ(expected[document_id], actual);
    document_id++;
  }
}

TEST(QueryTest, ParsingQuery) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " The ");
    EXPECT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Term, (*expr).operation);
    EXPECT_EQ(U"the", (*expr).term_str);
  }

  {
    auto expr = parse_query(normalizer, " nothing ");
    EXPECT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Term, (*expr).operation);
    EXPECT_EQ(U"nothing", (*expr).term_str);
  }
}

TEST(QueryTest, UnknownTerm) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " nothing ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }

  {
    auto expr = parse_query(normalizer, " second | nothing ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(2, postings->size());
  }

  {
    auto expr = parse_query(normalizer, " second nothing ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }

  {
    auto expr = parse_query(normalizer, R"( "second nothing" )");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }

  {
    auto expr = parse_query(normalizer, " second ~ nothing ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }
}

TEST(TermTest, TermSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " The ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(0, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(2, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(8, rng.position);
      EXPECT_EQ(3, rng.length);
    }

    {
      auto index = 2;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(3, postings->search_hit_count(index));

      EXPECT_EQ(2, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      EXPECT_EQ(7, postings->term_position(index, 1));
      EXPECT_EQ(1, postings->term_length(index, 1));

      EXPECT_EQ(11, postings->term_position(index, 2));
      EXPECT_EQ(1, postings->term_length(index, 2));

      auto rng = invidx.text_range(*postings, index, 2);
      EXPECT_EQ(59, rng.position);
      EXPECT_EQ(3, rng.length);
    }
  }

  {
    auto expr = parse_query(normalizer, " second ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(2, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(1, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(3, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(12, rng.position);
      EXPECT_EQ(6, rng.length);
    }

    {
      auto index = 1;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(8, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(40, rng.position);
      EXPECT_EQ(6, rng.length);
    }
  }
}

TEST(TermTest, OutOfOrderDocumentIds) {
  // Postings keeps entries sorted by document_id internally; this indexes
  // documents in descending order to verify that add_term_position's sorted
  // insertion (not just append) keeps lookups and result ordering correct.
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  std::vector<std::string> documents = {
      "third document here",
      "second document here",
      "first document here",
  };
  for (size_t i = 0; i < documents.size(); i++) {
    size_t document_id = documents.size() - 1 - i;
    indexer.index_document(document_id, UTF8PlainTextTokenizer(documents[i]));
  }

  EXPECT_EQ(3, invidx.document_count());
  EXPECT_EQ(3, invidx.df(U"document"));
  EXPECT_EQ(1, invidx.term_count(U"document", 0));
  EXPECT_EQ(1, invidx.term_count(U"document", 1));
  EXPECT_EQ(1, invidx.term_count(U"document", 2));

  auto expr = parse_query(normalizer, "document");
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(3, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
  EXPECT_EQ(1, postings->document_id(1));
  EXPECT_EQ(2, postings->document_id(2));
}

TEST(AndTest, AndSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " the second third ");

    EXPECT_EQ(Operation::And, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(6, postings->search_hit_count(index));

      {
        auto hit_index = 1;
        EXPECT_EQ(3, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 3;
        EXPECT_EQ(8, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 5;
        EXPECT_EQ(12, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(OrTest, OrSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " third | HELLO | second ");

    EXPECT_EQ(Operation::Or, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(1, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(3, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 2;
        EXPECT_EQ(12, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }

    {
      auto index = 2;
      EXPECT_EQ(4, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(0, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(0, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, R"( "is the" )");

    EXPECT_EQ(Operation::Adjacent, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(0, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(index, hit_index));
        EXPECT_EQ(2, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(1, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(index, hit_index));
        EXPECT_EQ(2, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto index = 2;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(2, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(index, hit_index));
        EXPECT_EQ(2, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(6, postings->term_position(index, hit_index));
        EXPECT_EQ(2, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearchWith3Words) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, R"( "the second sentence" )");

    EXPECT_EQ(Operation::Adjacent, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(7, postings->term_position(index, hit_index));
        EXPECT_EQ(3, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(36, rng.position);
        EXPECT_EQ(19, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, R"( second ~ document )");

    EXPECT_EQ(Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(2, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(1, postings->document_id(index));
      EXPECT_EQ(2, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(4, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(19, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }

    {
      auto index = 1;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(2, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(4, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(18, rng.position);
        EXPECT_EQ(8, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearchWithPhrase) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, R"( sentence ~ "is the" )");

    EXPECT_EQ(Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto index = 0;
      EXPECT_EQ(2, postings->document_id(index));
      EXPECT_EQ(2, postings->search_hit_count(index));

      {
        auto hit_index = 0;
        EXPECT_EQ(6, postings->term_position(index, hit_index));
        EXPECT_EQ(2, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(9, postings->term_position(index, hit_index));
        EXPECT_EQ(1, postings->term_length(index, hit_index));

        auto rng = invidx.text_range(*postings, index, hit_index);
        EXPECT_EQ(47, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }
  }
}

TEST(QueryTest, UnicodeTerm) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " 東京 ");
    ASSERT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Term, expr->operation);
    EXPECT_EQ(U"東京", expr->term_str);

    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(5, postings->document_id(0));
  }

  {
    auto expr = parse_query(normalizer, " 東京 港区 ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(2, postings->search_hit_count(0));
  }

  {
    auto expr = parse_query(normalizer, R"( "東京 タワー" )");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(0, postings->term_position(0, 0));
    EXPECT_EQ(2, postings->term_length(0, 0));
  }

  {
    auto expr = parse_query(normalizer, " 東京 -港区 ");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }
}

TEST(QueryTest, ImplicitPhrase) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " well-known ");
    ASSERT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Adjacent, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(6, postings->document_id(0));
    EXPECT_EQ(1, postings->term_position(0, 0));
    EXPECT_EQ(2, postings->term_length(0, 0));
  }

  {
    // An implicit phrase is flattened into the surrounding quoted phrase.
    auto expr = parse_query(normalizer, R"( "a well-known example" )");
    ASSERT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Adjacent, expr->operation);
    EXPECT_EQ(4, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(6, postings->document_id(0));
    EXPECT_EQ(4, postings->term_length(0, 0));
  }

  {
    // A token without letters can never match.
    auto expr = parse_query(normalizer, " 2021 ");
    ASSERT_NE(std::nullopt, expr);
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(0, postings->size());
  }
}

TEST(NotTest, NotSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(normalizer, " document -second ");

    EXPECT_EQ(Operation::And, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());
    EXPECT_EQ(Operation::Not, expr->nodes[1].operation);

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(2, postings->size());

    EXPECT_EQ(0, postings->document_id(0));
    EXPECT_EQ(1, postings->search_hit_count(0));

    EXPECT_EQ(3, postings->document_id(1));
    EXPECT_EQ(1, postings->search_hit_count(1));
  }

  {
    auto expr = parse_query(normalizer, R"( document -"the second sentence" )");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());
    EXPECT_EQ(0, postings->document_id(0));
    EXPECT_EQ(1, postings->document_id(1));
    EXPECT_EQ(3, postings->document_id(2));
  }

  {
    auto expr = parse_query(normalizer, " the -(second | fourth) ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(0, postings->document_id(0));
  }

  {
    // Excluding an unknown term excludes nothing.
    auto expr = parse_query(normalizer, " document -nothing ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(4, postings->size());
  }

  {
    auto expr = parse_query(normalizer, " second -second ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(0, postings->size());
  }
}

TEST(NotTest, InvalidNotQuery) {
  // NOT requires at least one positive operand within the same AND.
  EXPECT_EQ(std::nullopt, parse_query(normalizer, " -second "));
  EXPECT_EQ(std::nullopt, parse_query(normalizer, " -first -second "));
  EXPECT_EQ(std::nullopt, parse_query(normalizer, " first | -second "));
  EXPECT_EQ(std::nullopt, parse_query(normalizer, " first ~ -second "));
}

TEST(TF_IDF_Test, TF_IDF) {
  const std::vector<std::string> documents = {
      "apple orange orange banana",
      "banana orange strawberry strawberry grape",
  };

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);

    size_t document_id = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_id, tokenizer);
      document_id++;
    }
  }

  {
    auto term = U"apple";

    EXPECT_EQ(1, invidx.df(term));

    EXPECT_EQ(0.25, invidx.tf(term, 0));
    EXPECT_EQ(0, invidx.tf(term, 1));
  }

  {
    auto term = U"orange";

    EXPECT_EQ(2, invidx.df(term));

    EXPECT_EQ(0.5, invidx.tf(term, 0));
    EXPECT_EQ(0.2, invidx.tf(term, 1));
  }

  {
    auto term = U"banana";

    EXPECT_EQ(2, invidx.df(term));

    EXPECT_EQ(0.25, invidx.tf(term, 0));
    EXPECT_EQ(0.2, invidx.tf(term, 1));
  }

  {
    auto term = U"strawberry";

    EXPECT_EQ(1, invidx.df(term));

    EXPECT_EQ(0, invidx.tf(term, 0));
    EXPECT_EQ(0.4, invidx.tf(term, 1));
  }

  {
    auto term = U"grape";

    EXPECT_EQ(1, invidx.df(term));

    EXPECT_EQ(0, invidx.tf(term, 0));
    EXPECT_EQ(0.2, invidx.tf(term, 1));
  }
}

TEST(TopKTest, ReturnsHighestScoresDescending) {
  const std::vector<std::string> documents = {
      "apple apple apple",
      "apple",
      "banana",
      "apple apple",
  };

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    size_t document_id = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_id, tokenizer);
      document_id++;
    }
  }

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);
  ASSERT_EQ(3, result->size()); // docs 0, 1, 3

  auto hits = top_k(*result, 2, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  ASSERT_EQ(2, hits.size());
  EXPECT_EQ(0, result->document_id(hits[0].index));
  EXPECT_EQ(3, hits[0].score);
  EXPECT_EQ(3, result->document_id(hits[1].index));
  EXPECT_EQ(2, hits[1].score);
}

TEST(TopKTest, KGreaterThanCountReturnsAllSorted) {
  const std::vector<std::string> documents = {
      "apple",
      "apple apple apple",
      "apple apple",
  };

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    size_t document_id = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_id, tokenizer);
      document_id++;
    }
  }

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);

  auto hits = top_k(*result, 10, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  ASSERT_EQ(3, hits.size());
  EXPECT_EQ(1, result->document_id(hits[0].index));
  EXPECT_EQ(2, result->document_id(hits[1].index));
  EXPECT_EQ(0, result->document_id(hits[2].index));
}

TEST(TopKTest, ZeroKReturnsEmpty) {
  const std::vector<std::string> documents = {"apple"};

  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  UTF8PlainTextTokenizer tokenizer(documents[0]);
  indexer.index_document(0, tokenizer);

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);

  auto hits = top_k(*result, 0, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  EXPECT_TRUE(hits.empty());
}

TEST(TopKTest, TiesBrokenByAscendingIndex) {
  const std::vector<std::string> documents = {
      "apple",
      "apple",
      "apple",
  };

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    size_t document_id = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_id, tokenizer);
      document_id++;
    }
  }

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);

  auto hits = top_k(*result, 3, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  ASSERT_EQ(3, hits.size());
  EXPECT_EQ(0, hits[0].index);
  EXPECT_EQ(1, hits[1].index);
  EXPECT_EQ(2, hits[2].index);
}

TEST(PersistenceTest, RoundTrip) {
  auto invidx = sample_index();

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  // Index-level metadata.
  EXPECT_EQ(invidx.document_count(), loaded.document_count());
  EXPECT_EQ(invidx.average_document_term_count(),
            loaded.average_document_term_count());

  // Term-level statistics.
  for (auto term : {U"the", U"document", U"second"}) {
    EXPECT_EQ(invidx.term_count(term), loaded.term_count(term));
    EXPECT_EQ(invidx.df(term), loaded.df(term));
  }

  // Searching the loaded index yields the same hits and text ranges.
  auto expr = parse_query(normalizer, "the");
  auto expected = perform_search(invidx, *expr);
  auto actual = perform_search(loaded, *expr);
  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_id(i), actual->document_id(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      auto a = invidx.text_range(*expected, i, h);
      auto b = loaded.text_range(*actual, i, h);
      EXPECT_EQ(a.position, b.position);
      EXPECT_EQ(a.length, b.length);
    }
  }
}

TEST(PersistenceTest, UnicodeTermSurvives) {
  auto invidx = sample_index();

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  auto expr = parse_query(normalizer, "東京");
  auto postings = perform_search(loaded, *expr);
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(5, postings->document_id(0));
}

TEST(PersistenceTest, FileRoundTrip) {
  auto invidx = sample_index();

  auto path =
      (std::filesystem::temp_directory_path() / "searchlib_test.idx").string();
  invidx.save(path);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(path);
  std::filesystem::remove(path);

  EXPECT_EQ(invidx.document_count(), loaded.document_count());
  EXPECT_EQ(invidx.term_count(U"the"), loaded.term_count(U"the"));
}

TEST(PersistenceTest, RejectsInvalidData) {
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  ss << "this is not a searchlib index file";

  InMemoryInvertedIndex<TextRange> loaded;
  EXPECT_THROW(loaded.load(ss), std::runtime_error);
}

TEST(RemoveDocumentTest, ExcludedFromTermSearch) {
  auto invidx = sample_index();

  auto expr = parse_query(normalizer, "the");
  EXPECT_EQ(3, perform_search(invidx, *expr)->size()); // docs 0, 1, 2

  EXPECT_FALSE(invidx.has_removed_documents());
  invidx.remove_document(1);
  EXPECT_TRUE(invidx.has_removed_documents());
  EXPECT_TRUE(invidx.is_document_removed(1));

  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
  EXPECT_EQ(2, postings->document_id(1));

  // The surviving entries' hits and text ranges stay addressable after the
  // index positions have been renumbered by the filter.
  auto rng = invidx.text_range(*postings, 1, 0);
  EXPECT_LT(0u, rng.length);
}

TEST(RemoveDocumentTest, ExcludedFromOrSearch) {
  auto invidx = sample_index();

  auto expr = parse_query(normalizer, " third | HELLO | second ");
  EXPECT_EQ(3, perform_search(invidx, *expr)->size()); // docs 1, 2, 4

  invidx.remove_document(2);

  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(1, postings->document_id(0));
  EXPECT_EQ(4, postings->document_id(1));
}

TEST(RemoveDocumentTest, ReindexClearsTombstone) {
  auto invidx = sample_index();
  InMemoryIndexer indexer(invidx, normalizer);

  auto expr = parse_query(normalizer, "first"); // only doc 0
  ASSERT_EQ(1, perform_search(invidx, *expr)->size());

  invidx.remove_document(0);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());

  // Re-indexing the same document_id clears the tombstone (update = remove +
  // re-index), so the document becomes searchable again.
  indexer.index_document(0, UTF8PlainTextTokenizer(sample_documents[0]));
  EXPECT_FALSE(invidx.is_document_removed(0));
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
}

TEST(RemoveDocumentTest, MutableInterface) {
  auto invidx = sample_index();

  // Removal is reachable through the IMutableInvertedIndex interface, as a
  // FederationMember's mutable_index would be.
  IMutableInvertedIndex &mutable_index = invidx;
  mutable_index.remove_document(0);

  auto expr = parse_query(normalizer, "the");
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(1, postings->document_id(0));
  EXPECT_EQ(2, postings->document_id(1));
}

TEST(PersistenceTest, RemovedDocumentsSurvive) {
  auto invidx = sample_index();
  invidx.remove_document(1);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_TRUE(loaded.has_removed_documents());
  EXPECT_TRUE(loaded.is_document_removed(1));

  auto expr = parse_query(normalizer, "the");
  auto postings = perform_search(loaded, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
  EXPECT_EQ(2, postings->document_id(1));
}

// Builds an Operation::SameScope Expression by hand: parse_query does not
// support a query-string syntax for it (v1, C++ API only; see
// docs/missing_features.ja.md 3.4.1).
static Expression same_scope_expr(const std::string &scope_name,
                                  std::vector<std::u32string> terms) {
  Expression expr;
  expr.operation = Operation::SameScope;
  expr.scope_name = scope_name;
  for (auto &t : terms) {
    Expression node;
    node.operation = Operation::Term;
    node.term_str = t;
    expr.nodes.push_back(node);
  }
  return expr;
}

// Fixture for SameScopeTest: 5 documents, each with a hand-picked
// term_pos -> scope-ordinal assignment registered under scope_name
// "paragraph". Document 2 deliberately has no scope data registered at all.
//
//   0: "alpha beta gamma delta"     paragraphs: {alpha,beta} | {gamma,delta}
//   1: "alpha gamma alpha delta"    paragraphs: {alpha,gamma} | {alpha,delta}
//   2: "gamma delta"                (no scope registered)
//   3: "epsilon zeta epsilon"       paragraphs: {epsilon,zeta,epsilon}
//   4: "one two three"              paragraphs: {one,two,three}
static InMemoryInvertedIndex<TextRange> same_scope_index() {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  indexer.index_document(0, UTF8PlainTextTokenizer("alpha beta gamma delta"));
  invidx.set_scope_ids("paragraph", 0, {0, 0, 1, 1});

  indexer.index_document(1, UTF8PlainTextTokenizer("alpha gamma alpha delta"));
  invidx.set_scope_ids("paragraph", 1, {0, 0, 1, 1});

  indexer.index_document(2, UTF8PlainTextTokenizer("gamma delta"));
  // No set_scope_ids call for document 2.

  indexer.index_document(3, UTF8PlainTextTokenizer("epsilon zeta epsilon"));
  invidx.set_scope_ids("paragraph", 3, {7, 7, 7});

  indexer.index_document(4, UTF8PlainTextTokenizer("one two three"));
  invidx.set_scope_ids("paragraph", 4, {9, 9, 9});

  return invidx;
}

TEST(SameScopeTest, HitsInSameScopeSurvive) {
  auto invidx = same_scope_index();

  // alpha@0 and beta@1 share paragraph 0 in document 0; document 1 has no
  // "beta" at all, so it is excluded by the And-style document intersection
  // before scope filtering even runs.
  auto expr = same_scope_expr("paragraph", {U"alpha", U"beta"});
  auto postings = perform_search(invidx, expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
  ASSERT_EQ(2, postings->search_hit_count(0));
  EXPECT_EQ(0, postings->term_position(0, 0));
  EXPECT_EQ(1, postings->term_position(0, 1));
}

TEST(SameScopeTest, HitsAcrossScopesAreDropped) {
  auto invidx = same_scope_index();

  // In document 0, alpha@0 is paragraph 0 while gamma@2 is paragraph 1, so
  // that document contributes no match. Document 1 has alpha@0/gamma@1 in
  // paragraph 0 (a match) and alpha@2 in paragraph 1 (no partner), so only
  // the first combination survives.
  auto expr = same_scope_expr("paragraph", {U"alpha", U"gamma"});
  auto postings = perform_search(invidx, expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(1, postings->document_id(0));
  ASSERT_EQ(2, postings->search_hit_count(0));
  EXPECT_EQ(0, postings->term_position(0, 0)); // alpha@0
  EXPECT_EQ(1, postings->term_position(0, 1)); // gamma@1
}

TEST(SameScopeTest, DocumentsWithoutScopeDataAreExcluded) {
  auto invidx = same_scope_index();

  // Document 2 has both "gamma" and "delta" as plain terms but never had
  // set_scope_ids called for it, so has_scope() is false and it must not
  // appear in the result even though document 0 matches.
  auto expr = same_scope_expr("paragraph", {U"gamma", U"delta"});
  auto postings = perform_search(invidx, expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
  ASSERT_EQ(2, postings->search_hit_count(0));
  EXPECT_EQ(2, postings->term_position(0, 0)); // gamma@2
  EXPECT_EQ(3, postings->term_position(0, 1)); // delta@3
}

TEST(SameScopeTest, ThreeOrMoreNodes) {
  auto invidx = same_scope_index();

  auto expr = same_scope_expr("paragraph", {U"one", U"two", U"three"});
  auto postings = perform_search(invidx, expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(4, postings->document_id(0));
  ASSERT_EQ(3, postings->search_hit_count(0));
  EXPECT_EQ(0, postings->term_position(0, 0));
  EXPECT_EQ(1, postings->term_position(0, 1));
  EXPECT_EQ(2, postings->term_position(0, 2));
}

TEST(SameScopeTest, NullScopeIndexYieldsNoMatches) {
  auto invidx = same_scope_index();

  auto expr = same_scope_expr("paragraph", {U"alpha", U"beta"});
  auto postings = perform_search(invidx, expr); // scope_index defaults to null

  EXPECT_EQ(0, postings->size());
}

TEST(SameScopeTest, ComposesInsideAnd) {
  auto invidx = same_scope_index();

  // And(Term("delta"), SameScope(gamma, delta)) — SameScope nested as an And
  // operand recurses through the same perform_search_operation dispatch, so
  // it composes with the existing algebra with no special-casing.
  Expression and_expr;
  and_expr.operation = Operation::And;
  Expression delta_term;
  delta_term.operation = Operation::Term;
  delta_term.term_str = U"delta";
  and_expr.nodes.push_back(delta_term);
  and_expr.nodes.push_back(same_scope_expr("paragraph", {U"gamma", U"delta"}));

  auto postings = perform_search(invidx, and_expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(0, postings->document_id(0));
}

TEST(SameScopeTest, SetScopeIdsRejectsSizeMismatch) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("alpha beta"));

  EXPECT_THROW(invidx.set_scope_ids("paragraph", 0, {0}),
              std::invalid_argument);
}

TEST(SameScopeTest, SetScopeIdsRejectsNonMonotone) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("alpha beta gamma"));

  EXPECT_THROW(invidx.set_scope_ids("paragraph", 0, {0, 1, 0}),
              std::invalid_argument);
}

TEST(SameScopeTest, PersistedScopeSurvivesRoundtrip) {
  auto invidx = same_scope_index();

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_TRUE(loaded.has_scope("paragraph", 0));
  EXPECT_FALSE(loaded.has_scope("paragraph", 2));

  auto expr = same_scope_expr("paragraph", {U"alpha", U"gamma"});
  auto expected = perform_search(invidx, expr, &invidx);
  auto actual = perform_search(loaded, expr, &loaded);

  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_id(i), actual->document_id(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      EXPECT_EQ(expected->term_position(i, h), actual->term_position(i, h));
    }
  }
}

// "common" appears twice in every document, taking its postings across the
// Elias-Fano threshold with multiple positions per document, while each
// "termN" stays tiny and keeps the plain per-term representation — so one
// index exercises both encodings.
static InMemoryInvertedIndex<TextRange> wide_term_index(size_t document_count) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  for (size_t i = 0; i < document_count; i++) {
    auto text = "common term" + std::to_string(i) + " common";
    indexer.index_document(i, UTF8PlainTextTokenizer(text));
  }
  return invidx;
}

TEST(CompressedPersistenceTest, RoundTrip) {
  auto invidx = wide_term_index(100);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss); // the format is auto-detected from the header

  EXPECT_EQ(invidx.document_count(), loaded.document_count());
  for (auto term : {U"common" /* EF */, U"term42" /* plain */}) {
    EXPECT_EQ(invidx.term_count(term), loaded.term_count(term));
    EXPECT_EQ(invidx.df(term), loaded.df(term));
  }

  auto expr = parse_query(normalizer, "common term42");
  auto expected = perform_search(invidx, *expr);
  auto actual = perform_search(loaded, *expr);
  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_id(i), actual->document_id(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      auto a = invidx.text_range(*expected, i, h);
      auto b = loaded.text_range(*actual, i, h);
      EXPECT_EQ(a.position, b.position);
      EXPECT_EQ(a.length, b.length);
    }
  }
}

TEST(CompressedPersistenceTest, AllTermsBelowThreshold) {
  // sample_index's terms all have tiny postings, so a Compressed save falls
  // back to the plain per-term representation throughout; the format must
  // still round-trip.
  auto invidx = sample_index();

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  auto expr = parse_query(normalizer, "the");
  auto expected = perform_search(invidx, *expr);
  auto actual = perform_search(loaded, *expr);
  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_id(i), actual->document_id(i));
  }
}

TEST(CompressedPersistenceTest, TombstoneSurvives) {
  auto invidx = wide_term_index(100);
  invidx.remove_document(42);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_TRUE(loaded.is_document_removed(42));
  auto expr = parse_query(normalizer, "common");
  EXPECT_EQ(99, perform_search(loaded, *expr)->size());
}

TEST(CompressedPersistenceTest, SmallerThanPlain) {
  auto invidx = wide_term_index(500);

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(plain, {}, IndexFormat::Plain);
  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto plain_size = plain.str().size();
  auto compressed_size = compressed.str().size();
  EXPECT_LT(compressed_size, plain_size);
}

TEST(ThreadSafetyTest, ReadWriteBasics) {
  ThreadSafeInvertedIndex<TextRange> index;

  index.write([&](auto &idx) {
    InMemoryIndexer indexer(idx, normalizer);
    size_t document_id = 0;
    for (const auto &doc : sample_documents) {
      indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
      document_id++;
    }
  });

  // A read operation runs the query and fully materializes its results inside
  // the shared lock, returning only plain values.
  auto search_the = [&] {
    return index.read([&](const auto &idx) {
      auto expr = parse_query(normalizer, "the");
      auto postings = perform_search(idx, *expr);
      std::vector<size_t> ids;
      for (size_t i = 0; i < postings->size(); i++) {
        ids.push_back(postings->document_id(i));
      }
      return ids;
    });
  };

  EXPECT_EQ((std::vector<size_t>{0, 1, 2}), search_the());

  index.write([&](auto &idx) { idx.remove_document(1); });

  EXPECT_EQ((std::vector<size_t>{0, 2}), search_the());
}

// Stresses the exact hazard the locking guards against: a writer keeps
// inserting fresh terms (rehashing term_dictionary_, whose entries a bare-term
// search result aliases) and mutating postings vectors, while readers run
// queries and consume their results concurrently. Must complete without data
// races or crashes (run under -fsanitize=thread to actually catch races) and
// every result must stay internally consistent.
TEST(ThreadSafetyTest, ConcurrentReadersAndWriter) {
  ThreadSafeInvertedIndex<TextRange> index;
  index.write([&](auto &idx) {
    InMemoryIndexer indexer(idx, normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("document zero"));
  });

  constexpr int kWriterIterations = 300;
  std::atomic<bool> writer_done{false};

  // Parse once and share the read-only Expression across threads. Query parsing
  // (peglib) is not itself thread-safe and is orthogonal to index locking; the
  // realistic pattern is to parse before searching.
  auto expr = parse_query(normalizer, "document");
  ASSERT_TRUE(expr.has_value());

  std::thread writer([&] {
    for (int i = 1; i <= kWriterIterations; i++) {
      index.write([&](auto &idx) {
        InMemoryIndexer indexer(idx, normalizer);
        // Each doc introduces a unique term ("term<i>") to force rehashing,
        // plus the shared term "document".
        auto text = "document term" + std::to_string(i);
        indexer.index_document(static_cast<size_t>(i),
                               UTF8PlainTextTokenizer(text));
        if (i % 5 == 0) {
          idx.remove_document(static_cast<size_t>(i - 1));
        }
      });
    }
    writer_done.store(true);
  });

  auto reader_body = [&] {
    while (!writer_done.load()) {
      auto ok = index.read([&](const auto &idx) {
        auto postings = perform_search(idx, *expr);
        // Consume every entry (document_id, hits, text_range) under the lock.
        for (size_t i = 0; i < postings->size(); i++) {
          if (postings->search_hit_count(i) == 0) {
            return false;
          }
          (void)idx.text_range(*postings, i, 0);
        }
        return true;
      });
      EXPECT_TRUE(ok);
    }
  };

  std::thread reader1(reader_body);
  std::thread reader2(reader_body);

  writer.join();
  reader1.join();
  reader2.join();

  // The writer removed every 5th prior document; the rest plus doc 0 remain
  // searchable under "document".
  auto count = index.read(
      [&](const auto &idx) { return perform_search(idx, *expr)->size(); });
  EXPECT_GT(count, 0u);
}

//-----------------------------------------------------------------------------
// Prefix search (roadmap item 14)
//-----------------------------------------------------------------------------

const std::vector<std::string> prefix_documents = {
    "apple apricot",
    "apple banana",
    "application",
    "banana",
};

auto prefix_index() {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  size_t document_id = 0;
  for (const auto &doc : prefix_documents) {
    indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
    document_id++;
  }
  return invidx;
}

// enumerate_terms_with_prefix leaves the order unspecified, so every
// assertion here compares sorted vectors.
static std::vector<std::string> terms_with_prefix(const IInvertedIndex &invidx,
                                                  const std::u32string &prefix) {
  std::vector<std::string> terms;
  invidx.enumerate_terms_with_prefix(
      prefix, [&](const auto &str) { terms.push_back(u8(str)); });
  std::sort(terms.begin(), terms.end());
  return terms;
}

static std::vector<size_t> hit_document_ids(const IPostings &postings) {
  std::vector<size_t> ids;
  for (size_t i = 0; i < postings.size(); i++) {
    ids.push_back(postings.document_id(i));
  }
  return ids;
}

TEST(PrefixSearchTest, EnumerateTermsWithPrefix) {
  auto invidx = prefix_index();

  EXPECT_EQ((std::vector<std::string>{"apple", "application", "apricot"}),
            terms_with_prefix(invidx, U"ap"));
  EXPECT_EQ((std::vector<std::string>{"apple", "application"}),
            terms_with_prefix(invidx, U"app"));
  EXPECT_EQ((std::vector<std::string>{"banana"}),
            terms_with_prefix(invidx, U"banana"));
  EXPECT_TRUE(terms_with_prefix(invidx, U"zzz").empty());
}

TEST(PrefixSearchTest, EmptyPrefixEnumeratesWholeDictionary) {
  auto invidx = prefix_index();
  EXPECT_EQ((std::vector<std::string>{"apple", "application", "apricot",
                                      "banana"}),
            terms_with_prefix(invidx, U""));
}

TEST(PrefixSearchTest, QueryMatchesEveryTermWithThePrefix) {
  auto invidx = prefix_index();

  auto expr = parse_query(normalizer, "app*");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Prefix, expr->operation);
  EXPECT_EQ(U"app", expr->term_str);

  // apple (docs 0, 1) + application (doc 2)
  auto result = perform_search(invidx, *expr);
  EXPECT_EQ((std::vector<size_t>{0, 1, 2}), hit_document_ids(*result));
}

TEST(PrefixSearchTest, QueryIsEquivalentToTheExplicitOr) {
  auto invidx = prefix_index();

  auto prefix_expr = parse_query(normalizer, "ap*");
  auto or_expr = parse_query(normalizer, "apple|application|apricot");
  ASSERT_TRUE(prefix_expr);
  ASSERT_TRUE(or_expr);

  auto prefix_result = perform_search(invidx, *prefix_expr);
  auto or_result = perform_search(invidx, *or_expr);

  ASSERT_EQ(or_result->size(), prefix_result->size());
  EXPECT_EQ(hit_document_ids(*or_result), hit_document_ids(*prefix_result));

  // Scoring has to agree too: enumerate_terms expands a Prefix node into the
  // same term set the Or spells out by hand.
  for (size_t i = 0; i < prefix_result->size(); i++) {
    EXPECT_AP(bm25_score(invidx, *or_expr, *or_result, i),
              bm25_score(invidx, *prefix_expr, *prefix_result, i));
  }
}

TEST(PrefixSearchTest, NoMatchingTermYieldsNoHits) {
  auto invidx = prefix_index();

  auto expr = parse_query(normalizer, "zzz*");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Prefix, expr->operation);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(PrefixSearchTest, BareStarStaysAnOrdinaryTerm) {
  auto invidx = prefix_index();

  auto expr = parse_query(normalizer, "*");
  ASSERT_TRUE(expr);
  // Not a match-all: `*` alone is just a term no tokenizer can produce.
  EXPECT_EQ(Operation::Term, expr->operation);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(PrefixSearchTest, CombinesWithOtherOperators) {
  auto invidx = prefix_index();

  // `app* banana` == documents holding both an "app"-prefixed term and
  // "banana"; only doc 1 (apple banana) qualifies.
  auto and_expr = parse_query(normalizer, "app* banana");
  ASSERT_TRUE(and_expr);
  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, *and_expr)));

  // Negation over a prefix: "banana" but nothing starting with "app".
  auto not_expr = parse_query(normalizer, "banana -app*");
  ASSERT_TRUE(not_expr);
  EXPECT_EQ((std::vector<size_t>{3}),
            hit_document_ids(*perform_search(invidx, *not_expr)));
}

TEST(PrefixSearchTest, AppliesToTheLastTermOfASplitToken) {
  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("a well-known example"));
    indexer.index_document(1, UTF8PlainTextTokenizer("well done"));
  }

  // `well-kno*` is an implicit phrase whose last term is a prefix, so it
  // matches doc 0 but not doc 1.
  auto expr = parse_query(normalizer, "well-kno*");
  ASSERT_TRUE(expr);
  ASSERT_EQ(Operation::Adjacent, expr->operation);
  ASSERT_EQ(2, expr->nodes.size());
  EXPECT_EQ(Operation::Term, expr->nodes[0].operation);
  EXPECT_EQ(Operation::Prefix, expr->nodes[1].operation);

  EXPECT_EQ((std::vector<size_t>{0}),
            hit_document_ids(*perform_search(invidx, *expr)));
}

TEST(PrefixSearchTest, ExcludesLogicallyDeletedDocuments) {
  auto invidx = prefix_index();
  invidx.remove_document(1);

  auto expr = parse_query(normalizer, "app*");
  ASSERT_TRUE(expr);
  EXPECT_EQ((std::vector<size_t>{0, 2}),
            hit_document_ids(*perform_search(invidx, *expr)));
}

TEST(PrefixSearchTest, DroppedTokenDoesNotBecomeMatchAll) {
  auto invidx = prefix_index();

  // A stop-word filter drops "the" entirely, so `build()` yields a Term with
  // an empty string. Turning that into a Prefix would enumerate the whole
  // dictionary, i.e. `the*` would quietly match every document.
  TermFilter drop_the = [](const std::u32string &str, auto emit) {
    if (str != U"the") {
      emit(to_lowercase(str));
    }
  };

  auto expr = parse_query(drop_the, "the*");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Term, expr->operation);
  EXPECT_TRUE(expr->term_str.empty());
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(PrefixSearchTest, ExpandPrefixesRewritesToAnEquivalentOr) {
  auto invidx = prefix_index();

  auto parsed = parse_query(normalizer, "ap*");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::Prefix, parsed->operation);

  auto expanded = expand_prefixes(invidx, *parsed);
  ASSERT_EQ(Operation::Or, expanded.operation);
  ASSERT_EQ(3, expanded.nodes.size());
  // Sorted, so the expansion is reproducible for one prefix and one index.
  EXPECT_EQ(U"apple", expanded.nodes[0].term_str);
  EXPECT_EQ(U"application", expanded.nodes[1].term_str);
  EXPECT_EQ(U"apricot", expanded.nodes[2].term_str);

  // Same hits and same scores as searching the unexpanded expression, which
  // is what makes expanding-before-scoring a pure optimization.
  auto result = perform_search(invidx, expanded);
  auto direct = perform_search(invidx, *parsed);
  ASSERT_EQ(direct->size(), result->size());
  for (size_t i = 0; i < result->size(); i++) {
    EXPECT_EQ(direct->document_id(i), result->document_id(i));
    EXPECT_AP(bm25_score(invidx, *parsed, *direct, i),
              bm25_score(invidx, expanded, *result, i));
  }
}

TEST(PrefixSearchTest, ExpandPrefixesRewritesNestedNodes) {
  auto invidx = prefix_index();

  auto parsed = parse_query(normalizer, "app* banana");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::And, parsed->operation);

  auto expanded = expand_prefixes(invidx, *parsed);
  ASSERT_EQ(Operation::And, expanded.operation);
  ASSERT_EQ(2, expanded.nodes.size());
  // The Prefix child became an Or; the plain Term child is untouched.
  EXPECT_EQ(Operation::Or, expanded.nodes[0].operation);
  EXPECT_EQ(Operation::Term, expanded.nodes[1].operation);
  EXPECT_EQ(U"banana", expanded.nodes[1].term_str);

  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, expanded)));
}

//-----------------------------------------------------------------------------
// FST term dictionary (Compressed format, roadmap item 14 step 2)
//-----------------------------------------------------------------------------

TEST(FstTermDictionaryTest, RoundTripsUnicodeTerms) {
  // The FST keys are UTF-8 encoded, so non-ASCII terms are the interesting
  // case: they have to come back as the same u32strings.
  auto invidx = sample_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(compressed);

  for (const auto *term : {U"東京", U"タワー", U"港区", U"document", U"the"}) {
    EXPECT_TRUE(loaded.term_exists(term)) << u8(term);
    EXPECT_EQ(invidx.term_count(term), loaded.term_count(term)) << u8(term);
    EXPECT_EQ(invidx.df(term), loaded.df(term)) << u8(term);
  }
  EXPECT_FALSE(loaded.term_exists(U"zzzzz"));
}

TEST(FstTermDictionaryTest, CompressedBackendLooksUpUnicodeTerms) {
  auto invidx = sample_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  EXPECT_TRUE(loaded->term_exists(U"東京"));
  EXPECT_EQ(invidx.df(U"東京"), loaded->df(U"東京"));
  EXPECT_FALSE(loaded->term_exists(U"京"));  // a suffix is not a term
  EXPECT_FALSE(loaded->term_exists(U"東"));  // nor is a prefix

  // predictive_search over UTF-8 must not split a multi-byte codepoint.
  EXPECT_EQ((std::vector<std::string>{"東京"}),
            terms_with_prefix(*loaded, U"東"));
}

TEST(FstTermDictionaryTest, EmptyIndexRoundTrips) {
  // Zero terms means no FST is built at all; the reader has to cope.
  InMemoryInvertedIndex<TextRange> invidx;

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto position = compressed.tellg();
  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(compressed);
  EXPECT_EQ(0, loaded.document_count());
  EXPECT_FALSE(loaded.term_exists(U"anything"));

  compressed.clear();
  compressed.seekg(position);
  auto read_only = load_compressed_index(compressed);
  EXPECT_EQ(0, read_only->document_count());
  EXPECT_FALSE(read_only->term_exists(U"anything"));

  EXPECT_TRUE(terms_with_prefix(*read_only, U"").empty());
}

TEST(FstTermDictionaryTest, CompressedPrefixEnumerationMatchesInMemory) {
  auto invidx = prefix_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  // The compressed backend descends the FST while the in-memory one scans
  // its hash map, so this pins the two implementations to the same answer.
  for (const auto *prefix : {U"", U"a", U"ap", U"app", U"b", U"zzz"}) {
    EXPECT_EQ(terms_with_prefix(invidx, prefix),
              terms_with_prefix(*loaded, prefix))
        << u8(prefix);
  }
}

TEST(FstTermDictionaryTest, CompressedEnumerationReusesItsBuffer) {
  auto invidx = prefix_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  // The compressed backend hands the callback a buffer it refills per term,
  // as IInvertedIndex::enumerate_terms_with_prefix allows. Copying in the
  // callback (what every caller does) must still see every distinct term.
  std::vector<std::string> copied;
  loaded->enumerate_terms_with_prefix(
      U"ap", [&](const auto &str) { copied.push_back(u8(str)); });
  std::sort(copied.begin(), copied.end());
  EXPECT_EQ((std::vector<std::string>{"apple", "application", "apricot"}),
            copied);
}

TEST(FstTermDictionaryTest, RejectsEmptyTermWithADiagnosis) {
  // A Normalizer may map a token to the empty string, which the Plain format
  // tolerates but an FST cannot represent. The error has to say so.
  InMemoryInvertedIndex<TextRange> invidx;
  {
    Normalizer erase_apple = [](const std::u32string &str) {
      return str == U"apple" ? U"" : str;
    };
    InMemoryIndexer indexer(invidx, erase_apple);
    indexer.index_document(0, UTF8PlainTextTokenizer("apple banana"));
  }
  ASSERT_TRUE(invidx.term_exists(U""));

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  EXPECT_NO_THROW(invidx.save(plain));

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  try {
    invidx.save(compressed, {}, IndexFormat::Compressed);
    FAIL() << "expected an exception";
  } catch (const std::runtime_error &e) {
    EXPECT_NE(std::string(e.what()).find("empty term"), std::string::npos)
        << e.what();
  }
}

//-----------------------------------------------------------------------------
// average_document_term_count is maintained incrementally
//-----------------------------------------------------------------------------

// The running total behind it has to survive every path that writes
// documents_, or bm25 scores silently drift.
TEST(AverageDocumentTermCountTest, TracksIndexing) {
  InMemoryInvertedIndex<TextRange> invidx;
  EXPECT_DOUBLE_EQ(0.0, invidx.average_document_term_count());

  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("one two three"));
  EXPECT_DOUBLE_EQ(3.0, invidx.average_document_term_count());

  indexer.index_document(1, UTF8PlainTextTokenizer("one"));
  EXPECT_DOUBLE_EQ(2.0, invidx.average_document_term_count()); // (3+1)/2
}

TEST(AverageDocumentTermCountTest, ReindexingReplacesTheOldCount) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  indexer.index_document(0, UTF8PlainTextTokenizer("one two three four"));
  indexer.index_document(1, UTF8PlainTextTokenizer("one two"));
  EXPECT_DOUBLE_EQ(3.0, invidx.average_document_term_count()); // (4+2)/2

  // Re-indexing the same document_id must subtract the previous count, not
  // add to it.
  indexer.index_document(0, UTF8PlainTextTokenizer("one"));
  EXPECT_EQ(2, invidx.document_count());
  EXPECT_DOUBLE_EQ(1.5, invidx.average_document_term_count()); // (1+2)/2
}

TEST(AverageDocumentTermCountTest, SurvivesSaveLoadOnBothFormats) {
  auto invidx = sample_index();
  auto expected = invidx.average_document_term_count();
  EXPECT_GT(expected, 0.0);

  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    invidx.save(ss, {}, format);

    InMemoryInvertedIndex<TextRange> loaded;
    loaded.load(ss);
    EXPECT_DOUBLE_EQ(expected, loaded.average_document_term_count());

    // Loading twice must not double-count.
    std::stringstream again(std::ios::in | std::ios::out | std::ios::binary);
    invidx.save(again, {}, format);
    loaded.load(again);
    EXPECT_DOUBLE_EQ(expected, loaded.average_document_term_count());
  }
}

TEST(AverageDocumentTermCountTest, MatchesTheCompressedBackend) {
  auto invidx = sample_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  EXPECT_DOUBLE_EQ(invidx.average_document_term_count(),
                   loaded->average_document_term_count());
}
