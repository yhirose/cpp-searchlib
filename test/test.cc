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
