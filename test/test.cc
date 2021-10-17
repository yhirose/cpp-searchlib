#include <gtest/gtest.h>
#include <searchlib.h>

#include "test_utils.h"

using namespace searchlib;

std::vector<std::string> sample_documents = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third.",
    "Fourth document",
    "Hello World!",
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
    auto expr = parse_query(invidx, normalizer, " The ");
    EXPECT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Term, (*expr).operation);
    EXPECT_EQ(U"the", (*expr).term_str);
  }

  {
    auto expr = parse_query(invidx, normalizer, " nothing ");
    EXPECT_EQ(std::nullopt, expr);
  }
}

TEST(TermTest, TermSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(invidx, normalizer, " The ");
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
    auto expr = parse_query(invidx, normalizer, " second ");
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

TEST(AndTest, AndSearch) {
  const auto &invidx = sample_index();

  {
    auto expr = parse_query(invidx, normalizer, " the second third ");

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
    auto expr = parse_query(invidx, normalizer, " third | HELLO | second ");

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
    auto expr = parse_query(invidx, normalizer, R"( "is the" )");

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
    auto expr = parse_query(invidx, normalizer, R"( "the second sentence" )");

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
    auto expr = parse_query(invidx, normalizer, R"( second ~ document )");

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
    auto expr = parse_query(invidx, normalizer, R"( sentence ~ "is the" )");

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
