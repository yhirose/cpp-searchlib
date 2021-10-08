#include <gtest/gtest.h>
#include <searchlib.h>

#include "test_utils.h"

std::vector<std::string> sample_documents = {
    "This is the first document.",
    "This is the second document.",
    "This is the third document. This is the second sentence in the third.",
    "Fourth document",
    "Hello World!",
};

void sample_index(searchlib::InvertedIndex &invidx,
                  searchlib::TextRangeList &text_range_list) {
  searchlib::Indexer::set_normalizer(
      invidx, [](auto sv) { return unicode::to_lowercase(sv); });

  size_t document_id = 0;
  for (const auto &doc : sample_documents) {
    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(doc, &text_ranges);

    searchlib::Indexer::indexing(invidx, document_id, tokenizer);

    text_range_list.emplace(document_id, std::move(text_ranges));
    document_id++;
  }

  EXPECT_EQ(sample_documents.size(), invidx.document_count());

  auto term = U"the";
  EXPECT_EQ(5, invidx.term_count(term));
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
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, " The ");
    EXPECT_NE(std::nullopt, expr);
    EXPECT_EQ(searchlib::Operation::Term, (*expr).operation);
    EXPECT_EQ(U"the", (*expr).term_str);
  }

  {
    auto expr = parse_query(invidx, " nothing ");
    EXPECT_EQ(std::nullopt, expr);
  }
}

TEST(TermTest, TermSearch) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, " The ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(0, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      EXPECT_EQ(2, postings->term_position(invidx, 0));
      EXPECT_EQ(1, postings->term_length(invidx, 0));

      auto rng = searchlib::text_range(text_range_list, *postings, invidx, 0);
      EXPECT_EQ(8, rng.position);
      EXPECT_EQ(3, rng.length);
    }

    {
      auto invidx = 2;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(3, postings->search_hit_count(invidx));

      EXPECT_EQ(2, postings->term_position(invidx, 0));
      EXPECT_EQ(1, postings->term_length(invidx, 0));

      EXPECT_EQ(7, postings->term_position(invidx, 1));
      EXPECT_EQ(1, postings->term_length(invidx, 1));

      EXPECT_EQ(11, postings->term_position(invidx, 2));
      EXPECT_EQ(1, postings->term_length(invidx, 2));

      auto rng = searchlib::text_range(text_range_list, *postings, invidx, 2);
      EXPECT_EQ(59, rng.position);
      EXPECT_EQ(3, rng.length);
    }
  }

  {
    auto expr = parse_query(invidx, " second ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(2, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(1, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      EXPECT_EQ(3, postings->term_position(invidx, 0));
      EXPECT_EQ(1, postings->term_length(invidx, 0));

      auto rng = searchlib::text_range(text_range_list, *postings, invidx, 0);
      EXPECT_EQ(12, rng.position);
      EXPECT_EQ(6, rng.length);
    }

    {
      auto invidx = 1;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      EXPECT_EQ(8, postings->term_position(invidx, 0));
      EXPECT_EQ(1, postings->term_length(invidx, 0));

      auto rng = searchlib::text_range(text_range_list, *postings, invidx, 0);
      EXPECT_EQ(40, rng.position);
      EXPECT_EQ(6, rng.length);
    }
  }
}

TEST(AndTest, AndSearch) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, " the second third ");

    EXPECT_EQ(searchlib::Operation::And, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(6, postings->search_hit_count(invidx));

      {
        auto hit_index = 1;
        EXPECT_EQ(3, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 3;
        EXPECT_EQ(8, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 5;
        EXPECT_EQ(12, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(OrTest, OrSearch) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, " third | HELLO | second ");

    EXPECT_EQ(searchlib::Operation::Or, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(1, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto invidx = 1;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(3, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(5, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 2;
        EXPECT_EQ(12, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(63, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }

    {
      auto invidx = 2;
      EXPECT_EQ(4, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(0, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(0, rng.position);
        EXPECT_EQ(5, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearch) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, R"( "is the" )");

    EXPECT_EQ(searchlib::Operation::Adjacent, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(0, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(invidx, hit_index));
        EXPECT_EQ(2, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto invidx = 1;
      EXPECT_EQ(1, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(invidx, hit_index));
        EXPECT_EQ(2, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }

    {
      auto invidx = 2;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(2, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(1, postings->term_position(invidx, hit_index));
        EXPECT_EQ(2, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(5, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(6, postings->term_position(invidx, hit_index));
        EXPECT_EQ(2, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(AdjacentTest, AdjacentSearchWith3Words) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, R"( "the second sentence" )");

    EXPECT_EQ(searchlib::Operation::Adjacent, expr->operation);
    EXPECT_EQ(3, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(1, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(7, postings->term_position(invidx, hit_index));
        EXPECT_EQ(3, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(36, rng.position);
        EXPECT_EQ(19, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearch) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, R"( second ~ document )");

    EXPECT_EQ(searchlib::Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(2, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(1, postings->document_id(invidx));
      EXPECT_EQ(2, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(3, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(12, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(4, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(19, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }

    {
      auto invidx = 1;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(2, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(4, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(18, rng.position);
        EXPECT_EQ(8, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(8, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(40, rng.position);
        EXPECT_EQ(6, rng.length);
      }
    }
  }
}

TEST(NearTest, NearSearchWithPhrase) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;
  sample_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, R"( sentence ~ "is the" )");

    EXPECT_EQ(searchlib::Operation::Near, expr->operation);
    EXPECT_EQ(2, expr->nodes.size());

    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());

    {
      auto invidx = 0;
      EXPECT_EQ(2, postings->document_id(invidx));
      EXPECT_EQ(2, postings->search_hit_count(invidx));

      {
        auto hit_index = 0;
        EXPECT_EQ(6, postings->term_position(invidx, hit_index));
        EXPECT_EQ(2, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(33, rng.position);
        EXPECT_EQ(6, rng.length);
      }

      {
        auto hit_index = 1;
        EXPECT_EQ(9, postings->term_position(invidx, hit_index));
        EXPECT_EQ(1, postings->term_length(invidx, hit_index));

        auto rng =
            searchlib::text_range(text_range_list, *postings, invidx, hit_index);
        EXPECT_EQ(47, rng.position);
        EXPECT_EQ(8, rng.length);
      }
    }
  }
}

TEST(TF_IDF_Test, TF_IDF) {
  searchlib::InvertedIndex invidx;

  std::vector<std::string> documents = {
      "apple orange orange banana",
      "banana orange strawberry strawberry grape",
  };

  size_t document_id = 0;
  for (const auto &doc : documents) {
    searchlib::UTF8PlainTextTokenizer tokenizer(doc);
    searchlib::Indexer::indexing(invidx, document_id, tokenizer);
    document_id++;
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
