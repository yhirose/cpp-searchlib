#include <gtest/gtest.h>
#include <searchlib.h>

#include <atomic>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <map>
#include <random>
#include <set>
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

static auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

auto sample_index() {
  InMemoryInvertedIndex<TextRange> invidx;

  InMemoryIndexer indexer(invidx, normalizer);
  size_t document_key = 0;
  for (const auto &doc : sample_documents) {
    indexer.index_document(document_key, UTF8PlainTextTokenizer(doc));
    document_key++;
  }

  EXPECT_EQ(sample_documents.size(), invidx.document_count());

  auto term = U"the";
  EXPECT_EQ(5, invidx.term_count(term));

  return invidx;
}

// Indexes `text` as one term, whatever the default splitter would make of it.
// For the tests below that are about the term dictionary or the automata
// over it, not about splitting: they want 日本語 and 東京都 as terms, which the
// UAX #29 default cuts one scalar at a time.
Tokenizer<TextRange> whole_term(std::string text) {
  return [text = std::move(text)](Normalizer normalizer, auto callback) {
    auto str = u32(text);
    callback(normalizer ? normalizer(str) : str, 0,
             TextRange{0, text.size()});
  };
}

TEST(TokenizerTest, UTF8PlainTextTokenizer) {
  std::vector<std::vector<std::string>> expected = {
      {"this", "is", "the", "first", "document"},
      {"this", "is", "the", "second", "document"},
      {"this", "is", "the", "third", "document", "this", "is", "the", "second",
       "sentence", "in", "the", "third"},
      {"fourth", "document"},
      {"hello", "world"},
      // UAX #29: Han is one scalar per word, a Katakana run is one word.
      {"東", "京", "タワー", "港", "区"},
      {"a", "well", "known", "example"},
  };

  size_t document_key = 0;
  for (const auto &doc : sample_documents) {
    UTF8PlainTextTokenizer tokenizer(doc);
    std::vector<std::string> actual;
    tokenizer([](auto sv) { return unicode::to_lowercase(sv); },
              [&](auto &str, auto, auto) { actual.emplace_back(u8(str)); });
    EXPECT_EQ(expected[document_key], actual);
    document_key++;
  }
}

TEST(TokenizerTest, IllFormedUtf8IsSkipped) {
  // The vendored unicodelib validates UTF-8 as of the 2026-07 revision; the
  // one before it only checked that enough bytes were left, so each of these
  // decoded to *something*. Two of the results are visible here: "ab\xE3" plus
  // "cd" came back as the single term "ab㣤", silently swallowing the
  // "cd", and the bad continuation byte became a Hiragana term.
  //
  // "\xF7\xBF\xBF\xBF" is the sharp one. It decoded to U+1FFFFF, past the end
  // of Unicode, and general_category() indexed its block table with no range
  // check -- `_blocks[cp / _block_size]` off the end, then a dereference of
  // whatever pointer that read. Four bytes any client can put in a query
  // string. It faulted under some builds and quietly returned garbage under
  // others, which is why this test pins the terms rather than just asking the
  // call to return.
  const std::vector<std::pair<const char *, std::string>> ill_formed = {
      {"truncated", "\xE3"},
      {"bad continuation", "\xE3\x81\x21"},
      {"overlong U+0000", "\xC0\x80"},
      {"surrogate U+D800", "\xED\xA0\x80"},
      {"past U+10FFFF", "\xF7\xBF\xBF\xBF"},
      {"invalid lead bytes", "\xFF\xFE"},
  };

  for (const auto &[what, bytes] : ill_formed) {
    // Ill-formed bytes are skipped, never turned into terms, and the letters
    // around them are still found. None of these carries a letter of its own,
    // so whether the surrounding text comes back as one run or two is left
    // open; only the letters themselves are pinned.
    std::vector<std::string> actual;
    auto text = "ab" + bytes + "cd";
    UTF8PlainTextTokenizer tokenizer(text);
    tokenizer(nullptr,
              [&](auto &str, auto, auto) { actual.emplace_back(u8(str)); });
    std::string joined;
    for (const auto &term : actual) {
      joined += term;
    }
    EXPECT_EQ("abcd", joined) << what;

    // And the query side, which is the one an untrusted string reaches.
    EXPECT_NO_THROW(parse_query(normalizer, bytes)) << what;
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
      EXPECT_EQ(0, postings->document_ordinal(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(2, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(8, rng.position);
      EXPECT_EQ(3, rng.length);
    }

    {
      auto index = 2;
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(1, postings->document_ordinal(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(3, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(12, rng.position);
      EXPECT_EQ(6, rng.length);
    }

    {
      auto index = 1;
      EXPECT_EQ(2, postings->document_ordinal(index));
      EXPECT_EQ(1, postings->search_hit_count(index));

      EXPECT_EQ(8, postings->term_position(index, 0));
      EXPECT_EQ(1, postings->term_length(index, 0));

      auto rng = invidx.text_range(*postings, index, 0);
      EXPECT_EQ(40, rng.position);
      EXPECT_EQ(6, rng.length);
    }
  }
}

TEST(TermTest, OutOfOrderDocumentKeys) {
  // Keys are the caller's business and arrive in any order; the ordinals the
  // index assigns underneath follow indexing order regardless, which is what
  // keeps every postings list append-only (add_term_position asserts it).
  // This indexes keys in descending order and checks that nothing about the
  // result depends on it except the keys themselves.
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  std::vector<std::string> documents = {
      "third document here",
      "second document here",
      "first document here",
  };
  for (size_t i = 0; i < documents.size(); i++) {
    size_t document_key = documents.size() - 1 - i;
    indexer.index_document(document_key, UTF8PlainTextTokenizer(documents[i]));
  }

  // Ordinals 0, 1, 2 were handed out in that order, to keys 2, 1, 0.
  EXPECT_EQ(2, invidx.document_key(0));
  EXPECT_EQ(1, invidx.document_key(1));
  EXPECT_EQ(0, invidx.document_key(2));
  EXPECT_EQ(0, *invidx.document_ordinal(2));
  EXPECT_EQ(2, *invidx.document_ordinal(0));
  EXPECT_FALSE(invidx.document_ordinal(3));

  EXPECT_EQ(3, invidx.document_count());
  EXPECT_EQ(3, invidx.df(U"document"));
  for (size_t key = 0; key < 3; key++) {
    EXPECT_EQ(1, invidx.term_count(U"document", *invidx.document_ordinal(key)));
  }

  auto expr = parse_query(normalizer, "document");
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(3, postings->size());
  // Ascending in ordinal, which is indexing order -- so descending in key.
  EXPECT_EQ(2, invidx.document_key(postings->document_ordinal(0)));
  EXPECT_EQ(1, invidx.document_key(postings->document_ordinal(1)));
  EXPECT_EQ(0, invidx.document_key(postings->document_ordinal(2)));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(1, postings->document_ordinal(index));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(4, postings->document_ordinal(index));
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
      EXPECT_EQ(0, postings->document_ordinal(index));
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
      EXPECT_EQ(1, postings->document_ordinal(index));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(1, postings->document_ordinal(index));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
      EXPECT_EQ(2, postings->document_ordinal(index));
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
    // The default splitter cuts Han one scalar at a time, on both sides, so
    // the query token becomes the implicit phrase 東 + 京 and still finds the
    // document that was indexed the same way.
    auto expr = parse_query(normalizer, " 東京 ");
    ASSERT_NE(std::nullopt, expr);
    EXPECT_EQ(Operation::Adjacent, expr->operation);
    ASSERT_EQ(2, expr->nodes.size());
    EXPECT_EQ(U"東", expr->nodes[0].term_str);
    EXPECT_EQ(U"京", expr->nodes[1].term_str);

    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(5, postings->document_ordinal(0));
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
    EXPECT_EQ(3, postings->term_length(0, 0)); // 東 京 タワー
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
    EXPECT_EQ(6, postings->document_ordinal(0));
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
    EXPECT_EQ(6, postings->document_ordinal(0));
    EXPECT_EQ(4, postings->term_length(0, 0));
  }

  {
    // A number is a term like any other -- UAX #29 keeps `2021` whole, and
    // `2.0` and `1,234.56` too (WB11/12 join digits across MidNum) -- so a
    // query for one finds it, and only it.
    InMemoryInvertedIndex<TextRange> numbered;
    InMemoryIndexer indexer(numbered, normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("released in 2021"));
    indexer.index_document(1, UTF8PlainTextTokenizer("version 2.0 shipped"));
    indexer.index_document(2, UTF8PlainTextTokenizer("costs 1,234.56 today"));

    for (auto [query, hits] : {std::pair{" 2021 ", 1}, {"2.0", 1},
                               {"1,234.56", 1}, {"2", 0}, {"234", 0}}) {
      auto expr = parse_query(normalizer, query);
      ASSERT_NE(std::nullopt, expr) << query;
      auto postings = perform_search(numbered, *expr);
      EXPECT_EQ(hits, postings->size()) << query;
    }
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

    EXPECT_EQ(0, postings->document_ordinal(0));
    EXPECT_EQ(1, postings->search_hit_count(0));

    EXPECT_EQ(3, postings->document_ordinal(1));
    EXPECT_EQ(1, postings->search_hit_count(1));
  }

  {
    auto expr = parse_query(normalizer, R"( document -"the second sentence" )");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(3, postings->size());
    EXPECT_EQ(0, postings->document_ordinal(0));
    EXPECT_EQ(1, postings->document_ordinal(1));
    EXPECT_EQ(3, postings->document_ordinal(2));
  }

  {
    auto expr = parse_query(normalizer, " the -(second | fourth) ");
    auto postings = perform_search(invidx, *expr);

    EXPECT_EQ(1, postings->size());
    EXPECT_EQ(0, postings->document_ordinal(0));
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

    size_t document_key = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_key, tokenizer);
      document_key++;
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

// One scorer scoring one result after another, the first freed before the
// second is built (which can put it at the same address): on the compressed
// backend, where the scorer reads in blocks, every score is the one
// bm25_score gives.
TEST(BM25Test, AScorerScoresSuccessiveResultsOnTheCompressedBackend) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  for (int i = 0; i < 100; i++) {
    indexer.index_document(i, UTF8PlainTextTokenizer(std::string("common ") +
                                                     (i % 2 ? "odd" : "even") +
                                                     " filler words"));
  }
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);
  auto compressed = load_compressed_index(ss);

  auto expr = *parse_query(normalizer, "even | odd | common");
  BM25Scorer scorer(*compressed, expr);
  for (auto query : {"common even", "common odd", "common even"}) {
    auto result = perform_search(*compressed, *parse_query(normalizer, query));
    ASSERT_EQ(50, result->size()) << query;
    for (size_t i = 0; i < result->size(); i++) {
      EXPECT_DOUBLE_EQ(bm25_score(*compressed, expr, *result, i),
                       scorer(*result, i))
          << query << " hit " << i;
    }
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
    size_t document_key = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_key, tokenizer);
      document_key++;
    }
  }

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);
  ASSERT_EQ(3, result->size()); // docs 0, 1, 3

  auto hits = top_k(*result, 2, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  ASSERT_EQ(2, hits.size());
  EXPECT_EQ(0, result->document_ordinal(hits[0].index));
  EXPECT_EQ(3, hits[0].score);
  EXPECT_EQ(3, result->document_ordinal(hits[1].index));
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
    size_t document_key = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_key, tokenizer);
      document_key++;
    }
  }

  auto expr = parse_query(normalizer, "apple");
  auto result = perform_search(invidx, *expr);

  auto hits = top_k(*result, 10, [&](size_t i) {
    return term_count_score(invidx, *expr, *result, i);
  });

  ASSERT_EQ(3, hits.size());
  EXPECT_EQ(1, result->document_ordinal(hits[0].index));
  EXPECT_EQ(2, result->document_ordinal(hits[1].index));
  EXPECT_EQ(0, result->document_ordinal(hits[2].index));
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
    size_t document_key = 0;
    for (const auto &doc : documents) {
      UTF8PlainTextTokenizer tokenizer(doc);
      indexer.index_document(document_key, tokenizer);
      document_key++;
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
    EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      auto a = invidx.text_range(*expected, i, h);
      auto b = loaded.text_range(*actual, i, h);
      EXPECT_EQ(a.position, b.position);
      EXPECT_EQ(a.length, b.length);
    }
  }
}

// An index built with TextRangeStorage::Skip has to be indistinguishable
// from one built with Store on every axis except text_range(): the whole
// point is that a caller who never highlights pays nothing for the ranges
// and gives up nothing else.
TEST(TextRangeStorageTest, SkippingRangesChangesNothingButTextRange) {
  auto stored = sample_index();

  InMemoryInvertedIndex<TextRange> skipped;
  {
    InMemoryIndexer indexer(skipped, normalizer, TextRangeStorage::Skip);
    size_t document_key = 0;
    for (const auto &doc : sample_documents) {
      indexer.index_document(document_key++, UTF8PlainTextTokenizer(doc));
    }
  }

  EXPECT_EQ(stored.document_count(), skipped.document_count());
  EXPECT_EQ(stored.average_document_term_count(),
            skipped.average_document_term_count());

  for (const auto *query : {"the", "document", "the | second",
                            "the document", R"("the second")", "doc*"}) {
    auto expr = parse_query(normalizer, query);
    ASSERT_TRUE(expr) << query;
    auto a = perform_search(stored, *expr);
    auto b = perform_search(skipped, *expr);
    ASSERT_EQ(a->size(), b->size()) << query;
    for (size_t i = 0; i < a->size(); i++) {
      EXPECT_EQ(a->document_ordinal(i), b->document_ordinal(i)) << query;
      EXPECT_EQ(a->search_hit_count(i), b->search_hit_count(i)) << query;
      // Term positions come from the postings, not from the ranges, so they
      // survive too -- only the byte offsets are gone.
      for (size_t h = 0; h < a->search_hit_count(i); h++) {
        EXPECT_EQ(a->term_position(i, h), b->term_position(i, h)) << query;
      }
      EXPECT_AP(bm25_score(stored, *expr, *a, i),
                bm25_score(skipped, *expr, *b, i));
    }
  }
}

TEST(TextRangeStorageTest, SkippedIndexSaysSoInsteadOfMisreporting) {
  InMemoryInvertedIndex<TextRange> skipped;
  {
    InMemoryIndexer indexer(skipped, normalizer, TextRangeStorage::Skip);
    indexer.index_document(0, UTF8PlainTextTokenizer(sample_documents[0]));
  }

  auto expr = parse_query(normalizer, "the");
  auto postings = perform_search(skipped, *expr);
  ASSERT_EQ(1, postings->size());
  EXPECT_THROW(skipped.text_range(*postings, 0, 0), std::runtime_error);
}

// Both on-disk formats have a text-range section; with Skip it is simply
// empty, which has to survive a round trip rather than tripping the
// structural checks the compressed section makes.
TEST(TextRangeStorageTest, EmptyTextRangeSectionRoundTrips) {
  InMemoryInvertedIndex<TextRange> skipped;
  {
    InMemoryIndexer indexer(skipped, normalizer, TextRangeStorage::Skip);
    size_t document_key = 0;
    for (const auto &doc : sample_documents) {
      indexer.index_document(document_key++, UTF8PlainTextTokenizer(doc));
    }
  }

  auto expr = parse_query(normalizer, "the | document");
  auto expected = perform_search(skipped, *expr);

  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    skipped.save(ss, {}, format);

    InMemoryInvertedIndex<TextRange> loaded;
    loaded.load(ss);
    EXPECT_EQ(skipped.document_count(), loaded.document_count());

    auto actual = perform_search(loaded, *expr);
    ASSERT_EQ(expected->size(), actual->size());
    for (size_t i = 0; i < expected->size(); i++) {
      EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
    }
    EXPECT_THROW(loaded.text_range(*actual, 0, 0), std::runtime_error);
  }

  // And through the read-only backend, which parses the same bytes with its
  // own reader (see the note on load_compressed_index).
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  skipped.save(ss, {}, IndexFormat::Compressed);
  auto read_only = load_compressed_index(ss);
  EXPECT_EQ(skipped.document_count(), read_only->document_count());
  auto actual = perform_search(*read_only, *expr);
  EXPECT_EQ(expected->size(), actual->size());
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
  EXPECT_EQ(5, postings->document_ordinal(0));
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

// A load that fails -- the stream ends early, or says something impossible --
// leaves the index it was called on as it was, in either format: nothing of
// the half-read file is kept and nothing of the old index is lost.
TEST(PersistenceTest, FailedLoadLeavesTheIndexAsItWas) {
  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    auto source = sample_index();
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    source.save(ss, {}, format);
    const std::string bytes = ss.str();

    // Not a sample document: the index being loaded into has to be told apart
    // from the one in the file.
    InMemoryInvertedIndex<TextRange> index;
    InMemoryIndexer indexer(index, normalizer);
    indexer.index_document(100, UTF8PlainTextTokenizer("kept document here"));
    indexer.index_document(101, UTF8PlainTextTokenizer("another kept one"));
    index.remove_document(101);
    index.set_scope_ids("paragraph", 100, {0, 0, 1}); // "kept document here"

    size_t tried = 0;
    for (size_t length = 0; length < bytes.size(); length += 23) {
      std::stringstream in(bytes.substr(0, length),
                           std::ios::in | std::ios::binary);
      EXPECT_THROW(index.load(in), std::runtime_error) << length;
      tried++;

      ASSERT_EQ(1, index.document_count()) << length;
      EXPECT_TRUE(index.is_document_removed(1)) << length;
      EXPECT_EQ(1, index.df(U"kept")) << length;
      EXPECT_EQ(0, index.df(U"first")) << length;
      EXPECT_TRUE(index.has_scope("paragraph", 0)) << length;
      EXPECT_EQ(100, index.document_key(0)) << length;
      auto postings = perform_search(index, *parse_query(normalizer, "kept"));
      ASSERT_EQ(1, postings->size()) << length;
      EXPECT_EQ(0u, index.text_range(*postings, 0, 0).position) << length;
    }
    EXPECT_GT(tried, 10u);

    // And the whole file still loads, replacing it.
    std::stringstream whole(bytes, std::ios::in | std::ios::binary);
    index.load(whole);
    EXPECT_EQ(source.document_count(), index.document_count());
    EXPECT_EQ(source.df(U"first"), index.df(U"first"));
    EXPECT_EQ(0, index.df(U"kept"));
  }
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
  EXPECT_TRUE(invidx.is_document_removed(*invidx.document_ordinal(1)));

  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(0, postings->document_ordinal(0));
  EXPECT_EQ(2, postings->document_ordinal(1));

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
  EXPECT_EQ(1, postings->document_ordinal(0));
  EXPECT_EQ(4, postings->document_ordinal(1));
}

TEST(RemoveDocumentTest, ReindexRevivesTheKey) {
  auto invidx = sample_index();
  InMemoryIndexer indexer(invidx, normalizer);
  auto live = invidx.document_count();

  auto expr = parse_query(normalizer, "first"); // only doc 0
  ASSERT_EQ(1, perform_search(invidx, *expr)->size());

  auto old_ordinal = *invidx.document_ordinal(0);
  invidx.remove_document(0);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
  EXPECT_EQ(live - 1, invidx.document_count());

  // Re-indexing the key (update = remove + re-index) makes it searchable
  // again -- under a fresh ordinal. The old ordinal stays tombstoned; the
  // key now names the new one.
  indexer.index_document(0, UTF8PlainTextTokenizer(sample_documents[0]));
  auto new_ordinal = *invidx.document_ordinal(0);
  EXPECT_NE(old_ordinal, new_ordinal);
  EXPECT_TRUE(invidx.is_document_removed(old_ordinal));
  EXPECT_FALSE(invidx.is_document_removed(new_ordinal));
  EXPECT_EQ(live, invidx.document_count());

  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(new_ordinal, postings->document_ordinal(0));
  EXPECT_EQ(0, invidx.document_key(postings->document_ordinal(0)));
}

TEST(RemoveDocumentTest, ReindexReplacesTheText) {
  // The point of a fresh ordinal per re-index: terms of the old text must
  // not linger. Under the old same-slot scheme add_term_position appended to
  // the existing entry, so "old" would still have found this document.
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  indexer.index_document(7, UTF8PlainTextTokenizer("old words here"));
  indexer.index_document(8, UTF8PlainTextTokenizer("other document"));
  indexer.index_document(7, UTF8PlainTextTokenizer("new words here"));

  EXPECT_EQ(2, invidx.document_count());
  EXPECT_EQ(0, perform_search(invidx, *parse_query(normalizer, "old"))->size());

  auto postings = perform_search(invidx, *parse_query(normalizer, "new"));
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(7, invidx.document_key(postings->document_ordinal(0)));

  // Both texts share "words here"; only the new one may answer for key 7.
  auto shared = perform_search(invidx, *parse_query(normalizer, "words"));
  ASSERT_EQ(1, shared->size());
  EXPECT_EQ(1, shared->search_hit_count(0));
  EXPECT_EQ(7, invidx.document_key(shared->document_ordinal(0)));

  // The text ranges answer for the new text, not the old one.
  auto rng = invidx.text_range(*postings, 0, 0);
  EXPECT_EQ(0u, rng.position);
  EXPECT_EQ(3u, rng.length); // "new"
}

TEST(RemoveDocumentTest, MutableInterface) {
  auto invidx = sample_index();

  // Removal is reachable through the IMutableInvertedIndex interface, as a
  // FederationMember's mutable_index would be.
  IMutableInvertedIndex<size_t> &mutable_index = invidx;
  mutable_index.remove_document(0);

  auto expr = parse_query(normalizer, "the");
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(1, postings->document_ordinal(0));
  EXPECT_EQ(2, postings->document_ordinal(1));
}

TEST(PersistenceTest, RemovedDocumentsSurvive) {
  auto invidx = sample_index();
  invidx.remove_document(1);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_TRUE(loaded.has_removed_documents());
  EXPECT_TRUE(loaded.is_document_removed(*loaded.document_ordinal(1)));

  auto expr = parse_query(normalizer, "the");
  auto postings = perform_search(loaded, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ(0, loaded.document_key(postings->document_ordinal(0)));
  EXPECT_EQ(2, loaded.document_key(postings->document_ordinal(1)));
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
  EXPECT_EQ(0, postings->document_ordinal(0));
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
  EXPECT_EQ(1, postings->document_ordinal(0));
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
  EXPECT_EQ(0, postings->document_ordinal(0));
  ASSERT_EQ(2, postings->search_hit_count(0));
  EXPECT_EQ(2, postings->term_position(0, 0)); // gamma@2
  EXPECT_EQ(3, postings->term_position(0, 1)); // delta@3
}

TEST(SameScopeTest, ThreeOrMoreNodes) {
  auto invidx = same_scope_index();

  auto expr = same_scope_expr("paragraph", {U"one", U"two", U"three"});
  auto postings = perform_search(invidx, expr, &invidx);

  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(4, postings->document_ordinal(0));
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
  EXPECT_EQ(0, postings->document_ordinal(0));
}

// A copy of an index owns its scope data: registering scopes on either one
// afterwards must not show up on the other.
TEST(SameScopeTest, ACopyHasItsOwnScopeData) {
  auto a = same_scope_index();
  auto b = a;

  b.set_scope_ids("paragraph", 2, {0, 0}); // "gamma delta", none before
  EXPECT_TRUE(b.has_scope("paragraph", 2));
  EXPECT_FALSE(a.has_scope("paragraph", 2));

  a.set_scope_ids("section", 0, {0, 0, 0, 0});
  EXPECT_TRUE(a.has_scope("section", 0));
  EXPECT_FALSE(b.has_scope("section", 0));

  // What was there before the copy is in both.
  EXPECT_TRUE(a.has_scope("paragraph", 0));
  EXPECT_TRUE(b.has_scope("paragraph", 0));
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

  EXPECT_TRUE(loaded.has_scope("paragraph", *loaded.document_ordinal(0)));
  EXPECT_FALSE(loaded.has_scope("paragraph", *loaded.document_ordinal(2)));

  auto expr = same_scope_expr("paragraph", {U"alpha", U"gamma"});
  auto expected = perform_search(invidx, expr, &invidx);
  auto actual = perform_search(loaded, expr, &loaded);

  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
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
    EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      auto a = invidx.text_range(*expected, i, h);
      auto b = loaded.text_range(*actual, i, h);
      EXPECT_EQ(a.position, b.position);
      EXPECT_EQ(a.length, b.length);
    }
  }
}

TEST(CompressedPersistenceTest, BlockReadsAgreeWithElementReads) {
  // The block primitives -- read_ordinals, read_hit_counts,
  // read_term_positions, advance -- against the per-element calls they
  // stand in for, on the Elias-Fano postings a compressed save produces and
  // on the in-memory ones. Irregular hit counts and gaps between documents,
  // so a block never lines up with the data by accident.
  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    for (size_t i = 0; i < 300; i++) {
      std::string text;
      switch (i % 5) {
      case 0: text = "common x common y common"; break;
      case 1: text = "x common"; break;
      case 2: text = "x y"; break;
      case 3: text = "common"; break;
      case 4: text = "x y z common w common"; break;
      }
      indexer.index_document(i, UTF8PlainTextTokenizer(text));
    }
  }
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(ss);

  auto plain = invidx.postings(U"common");
  auto ef = loaded->postings(U"common");
  ASSERT_TRUE(ef->prefers_block_reads());
  ASSERT_EQ(plain->size(), ef->size());
  auto n = ef->size();

  for (const auto &p : {plain, ef}) {
    for (size_t index = 0; index < n; index += 3) {
      size_t ordinals[7], counts[7];
      auto got = p->read_ordinals(index, ordinals, 7);
      EXPECT_EQ(got, p->read_hit_counts(index, counts, 7));
      EXPECT_EQ(std::min<size_t>(7, n - index), got);
      for (size_t i = 0; i < got; i++) {
        EXPECT_EQ(p->document_ordinal(index + i), ordinals[i]);
        EXPECT_EQ(p->search_hit_count(index + i), counts[i]);
      }

      std::vector<size_t> positions(p->search_hit_count(index));
      p->read_term_positions(index, positions.data());
      for (size_t h = 0; h < positions.size(); h++) {
        EXPECT_EQ(p->term_position(index, h), positions[h]);
      }
    }

    // advance(from, ordinal) is the first index past `from` at or beyond
    // `ordinal`; the scan is the definition.
    for (size_t from = 0; from + 1 < n; from += 7) {
      for (auto gap : {1, 2, 5, 40, 1000}) {
        auto ordinal = p->document_ordinal(from) + gap;
        auto expected = from + 1;
        while (expected < n && p->document_ordinal(expected) < ordinal) {
          expected++;
        }
        EXPECT_EQ(expected, p->advance(from, ordinal)) << from << " " << gap;
      }
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
    EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
  }
}

TEST(CompressedPersistenceTest, TombstoneSurvives) {
  auto invidx = wide_term_index(100);
  invidx.remove_document(42);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_TRUE(loaded.is_document_removed(*loaded.document_ordinal(42)));
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
    size_t document_key = 0;
    for (const auto &doc : sample_documents) {
      indexer.index_document(document_key, UTF8PlainTextTokenizer(doc));
      document_key++;
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
        ids.push_back(postings->document_ordinal(i));
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

  // Parse once and share the read-only Expression across threads. Query
  // parsing is orthogonal to index locking; the realistic pattern is to
  // parse before searching.
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
        // Consume every entry (ordinal, hits, text_range) under the lock.
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
  size_t document_key = 0;
  for (const auto &doc : prefix_documents) {
    indexer.index_document(document_key, UTF8PlainTextTokenizer(doc));
    document_key++;
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
    ids.push_back(postings.document_ordinal(i));
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

// A token the splitter finds no word in (only punctuation) is run through
// the filter as it is; one the filter then drops is the empty term, like a
// token whose every piece it drops, rather than the unfiltered text.
TEST(QueryTest, ATokenWithNoWordThatTheFilterDropsIsTheEmptyTerm) {
  TermFilter drop_everything = [](const std::u32string &, auto) {};
  TermFilter keep = [](const std::u32string &str, auto emit) { emit(str); };

  for (auto token : {"...", "!!", "@#$"}) {
    auto dropped = parse_query(drop_everything, token);
    ASSERT_TRUE(dropped) << token;
    EXPECT_EQ(Operation::Term, dropped->operation) << token;
    EXPECT_TRUE(dropped->term_str.empty()) << token;

    // Kept, it stays what was typed.
    auto kept = parse_query(keep, token);
    ASSERT_TRUE(kept) << token;
    EXPECT_EQ(Operation::Term, kept->operation) << token;
    EXPECT_EQ(u32(token), kept->term_str) << token;
  }

  // The same for a word the splitter does find.
  auto dropped = parse_query(drop_everything, "apple");
  ASSERT_TRUE(dropped);
  EXPECT_TRUE(dropped->term_str.empty());
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
    EXPECT_EQ(direct->document_ordinal(i), result->document_ordinal(i));
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
// Wildcard search (roadmap item 14 step 3)
//-----------------------------------------------------------------------------

// enumerate_terms_with_wildcard leaves the order unspecified, so every
// assertion here compares sorted vectors.
static std::vector<std::string>
terms_with_wildcard(const IInvertedIndex &invidx,
                    const std::u32string &pattern) {
  std::vector<std::string> terms;
  invidx.enumerate_terms_with_wildcard(
      pattern, [&](const auto &str) { terms.push_back(u8(str)); });
  std::sort(terms.begin(), terms.end());
  return terms;
}

TEST(WildcardSearchTest, EnumerateTermsWithWildcard) {
  auto invidx = prefix_index();

  EXPECT_EQ((std::vector<std::string>{"apple"}),
            terms_with_wildcard(invidx, U"a*e"));
  EXPECT_EQ((std::vector<std::string>{"banana"}),
            terms_with_wildcard(invidx, U"*ana"));
  EXPECT_EQ((std::vector<std::string>{"application"}),
            terms_with_wildcard(invidx, U"app*ion"));
  EXPECT_EQ((std::vector<std::string>{"apple", "application", "apricot",
                                      "banana"}),
            terms_with_wildcard(invidx, U"*a*"));
  EXPECT_TRUE(terms_with_wildcard(invidx, U"zzz*zzz").empty());
}

TEST(WildcardSearchTest, EmptyPatternOnlyMatchesTheEmptyTerm) {
  // No `*` in an empty pattern means it matches nothing but the empty
  // string, unlike enumerate_terms_with_prefix's empty-prefix-matches-all.
  auto invidx = prefix_index();
  EXPECT_TRUE(terms_with_wildcard(invidx, U"").empty());
}

TEST(WildcardSearchTest, TrailingStarOnlyStillParsesAsPrefix) {
  // Regression guard: the is_prefix check has to keep routing the common
  // single-trailing-star case to the cheaper Operation::Prefix path instead
  // of the general wildcard automaton.
  auto expr = parse_query(normalizer, "app*");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Prefix, expr->operation);
  EXPECT_EQ(U"app", expr->term_str);
}

TEST(WildcardSearchTest, LeadingAndInteriorStarParseAsWildcard) {
  auto leading = parse_query(normalizer, "*ana");
  ASSERT_TRUE(leading);
  EXPECT_EQ(Operation::Wildcard, leading->operation);
  EXPECT_EQ(U"*ana", leading->term_str);

  auto interior = parse_query(normalizer, "a*e");
  ASSERT_TRUE(interior);
  EXPECT_EQ(Operation::Wildcard, interior->operation);
  EXPECT_EQ(U"a*e", interior->term_str);
}

TEST(WildcardSearchTest, MultipleStarsCollapseToOne) {
  // "app**" has more than one star, so it takes the wildcard path rather
  // than Prefix, but the pattern still collapses to "app*" and matches the
  // same terms.
  auto expr = parse_query(normalizer, "app**");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Wildcard, expr->operation);
  EXPECT_EQ(U"app*", expr->term_str);
}

TEST(WildcardSearchTest, FilterAppliesToEachLiteralSegment) {
  // The normalizer (lowercasing) has to run on "AP" and "E" independently,
  // the same way it would on a plain term.
  auto expr = parse_query(normalizer, "AP*E");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Wildcard, expr->operation);
  EXPECT_EQ(U"ap*e", expr->term_str);
}

TEST(WildcardSearchTest, QueryMatchesEveryTermSatisfyingThePattern) {
  auto invidx = prefix_index();

  auto expr = parse_query(normalizer, "app*ion");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Wildcard, expr->operation);

  // application (doc 2) only.
  auto result = perform_search(invidx, *expr);
  EXPECT_EQ((std::vector<size_t>{2}), hit_document_ids(*result));
}

TEST(WildcardSearchTest, QueryIsEquivalentToTheExplicitOr) {
  auto invidx = prefix_index();

  auto wildcard_expr = parse_query(normalizer, "*a*");
  auto or_expr =
      parse_query(normalizer, "apple|application|apricot|banana");
  ASSERT_TRUE(wildcard_expr);
  ASSERT_TRUE(or_expr);

  auto wildcard_result = perform_search(invidx, *wildcard_expr);
  auto or_result = perform_search(invidx, *or_expr);

  ASSERT_EQ(or_result->size(), wildcard_result->size());
  EXPECT_EQ(hit_document_ids(*or_result), hit_document_ids(*wildcard_result));

  // Scoring has to agree too: enumerate_terms expands a Wildcard node into
  // the same term set the Or spells out by hand.
  for (size_t i = 0; i < wildcard_result->size(); i++) {
    EXPECT_AP(bm25_score(invidx, *or_expr, *or_result, i),
              bm25_score(invidx, *wildcard_expr, *wildcard_result, i));
  }
}

TEST(WildcardSearchTest, NoMatchingTermYieldsNoHits) {
  auto invidx = prefix_index();

  auto expr = parse_query(normalizer, "zzz*zzz");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Wildcard, expr->operation);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(WildcardSearchTest, CombinesWithOtherOperators) {
  auto invidx = prefix_index();

  // `a*e banana` == documents holding both an "a...e" term and "banana";
  // only doc 1 (apple banana) qualifies.
  auto and_expr = parse_query(normalizer, "a*e banana");
  ASSERT_TRUE(and_expr);
  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, *and_expr)));

  // Negation over a wildcard: "banana" but nothing matching "a*e".
  auto not_expr = parse_query(normalizer, "banana -a*e");
  ASSERT_TRUE(not_expr);
  EXPECT_EQ((std::vector<size_t>{3}),
            hit_document_ids(*perform_search(invidx, *not_expr)));
}

TEST(WildcardSearchTest, ExcludesLogicallyDeletedDocuments) {
  auto invidx = prefix_index();
  invidx.remove_document(0); // apple apricot

  auto expr = parse_query(normalizer, "a*e");
  ASSERT_TRUE(expr);
  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, *expr)));
}

TEST(WildcardSearchTest, ExpandWildcardsRewritesToAnEquivalentOr) {
  auto invidx = prefix_index();

  auto parsed = parse_query(normalizer, "*a*");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::Wildcard, parsed->operation);

  auto expanded = expand_wildcards(invidx, *parsed);
  ASSERT_EQ(Operation::Or, expanded.operation);
  ASSERT_EQ(4, expanded.nodes.size());
  // Sorted, so the expansion is reproducible for one pattern and one index.
  EXPECT_EQ(U"apple", expanded.nodes[0].term_str);
  EXPECT_EQ(U"application", expanded.nodes[1].term_str);
  EXPECT_EQ(U"apricot", expanded.nodes[2].term_str);
  EXPECT_EQ(U"banana", expanded.nodes[3].term_str);

  // Same hits and same scores as searching the unexpanded expression, which
  // is what makes expanding-before-scoring a pure optimization.
  auto result = perform_search(invidx, expanded);
  auto direct = perform_search(invidx, *parsed);
  ASSERT_EQ(direct->size(), result->size());
  for (size_t i = 0; i < result->size(); i++) {
    EXPECT_EQ(direct->document_ordinal(i), result->document_ordinal(i));
    EXPECT_AP(bm25_score(invidx, *parsed, *direct, i),
              bm25_score(invidx, expanded, *result, i));
  }
}

TEST(WildcardSearchTest, ExpandWildcardsRewritesNestedNodes) {
  auto invidx = prefix_index();

  auto parsed = parse_query(normalizer, "a*e banana");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::And, parsed->operation);

  auto expanded = expand_wildcards(invidx, *parsed);
  ASSERT_EQ(Operation::And, expanded.operation);
  ASSERT_EQ(2, expanded.nodes.size());
  // The Wildcard child became an Or; the plain Term child is untouched.
  EXPECT_EQ(Operation::Or, expanded.nodes[0].operation);
  EXPECT_EQ(Operation::Term, expanded.nodes[1].operation);
  EXPECT_EQ(U"banana", expanded.nodes[1].term_str);

  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, expanded)));
}

TEST(WildcardSearchTest, CompressedBackendMatchesInMemory) {
  auto invidx = prefix_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  // The compressed backend walks the FST with a custom automaton while the
  // in-memory one scans its hash map and tests each term directly, so this
  // pins the two implementations to the same answer.
  for (const auto *pattern :
       {U"*", U"a*e", U"*ana", U"app*ion", U"*a*", U"zzz*zzz"}) {
    EXPECT_EQ(terms_with_wildcard(invidx, pattern),
              terms_with_wildcard(*loaded, pattern))
        << u8(pattern);
  }
}

TEST(WildcardSearchTest, CompressedBackendMatchesInMemoryOffTheEasyPath) {
  // The compressed backend's automaton keeps its DP row as a bitmask, which
  // buys three special cases the patterns above never reach: a run of stars
  // is collapsed to one (the row's star closure is a single shift, which is
  // only enough when no two star positions are adjacent), a pattern past 63
  // codepoints spills the row from one inline word onto the heap, and a
  // non-ASCII literal misses the direct-indexed table and takes the
  // binary-searched fallback. Each is pinned here against the in-memory
  // backend, which does none of that.
  const std::u32string a62(62, U'a');
  const std::u32string a63(63, U'a');
  const std::u32string a64(64, U'a');

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    size_t document_key = 0;
    for (const auto &term :
         {a62, a63, a64, std::u32string(65, U'a'), std::u32string(100, U'a'),
          a63 + U"b", a64 + U"b", std::u32string(U"apple"),
          std::u32string(U"banana"), std::u32string(U"café"),
          std::u32string(U"日本語"), std::u32string(U"あいうえお")}) {
      indexer.index_document(document_key++, whole_term(u8(term)));
    }
  }

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  std::vector<std::u32string> patterns = {
      // collapsed star runs
      U"**", U"***", U"a**e", U"**a**", U"a***b",
      // the row's word boundary, with the star on either side of it
      a62 + U"*", a63 + U"*", a64 + U"*", U"*" + a63, U"*" + a64,
      a62 + U"*b", a63 + U"*b", a64 + U"*b",
      // the same boundary with no star at all, so is_match() has to pick the
      // accepting bit out of the right word
      a63, a64,
      // literals outside the ASCII table
      U"caf*", U"*é", U"日*語", U"*本*", U"あ*お", U"*う*",
  };
  for (const auto &pattern : patterns) {
    EXPECT_EQ(terms_with_wildcard(invidx, pattern),
              terms_with_wildcard(*loaded, pattern))
        << u8(pattern);
  }

  // Not a tautology only if the patterns actually match something.
  EXPECT_EQ((std::vector<std::string>{"日本語"}),
            terms_with_wildcard(*loaded, U"日*語"));
  EXPECT_EQ((std::vector<std::string>{"apple"}),
            terms_with_wildcard(*loaded, U"a**e"));
  EXPECT_EQ((std::vector<std::string>{u8(a64 + U"b"), u8(a63 + U"b")}),
            terms_with_wildcard(*loaded, a62 + U"*b"));
}

TEST(WildcardSearchTest, CompressedEnumerationReusesItsBuffer) {
  auto invidx = prefix_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  std::vector<std::string> copied;
  loaded->enumerate_terms_with_wildcard(
      U"*a*", [&](const auto &str) { copied.push_back(u8(str)); });
  std::sort(copied.begin(), copied.end());
  EXPECT_EQ((std::vector<std::string>{"apple", "application", "apricot",
                                      "banana"}),
            copied);
}

//-----------------------------------------------------------------------------
// Fuzzy search (edit distance)
//-----------------------------------------------------------------------------

// A vocabulary with terms one and two edits apart from "apple", so that the
// distance threshold is what decides each case rather than the corpus.
const std::vector<std::string> fuzzy_documents = {
    "apple ample",
    "maple banana",
    "apply",
};

auto fuzzy_index() {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  size_t document_key = 0;
  for (const auto &doc : fuzzy_documents) {
    indexer.index_document(document_key++, UTF8PlainTextTokenizer(doc));
  }
  // Two Han terms, for the codepoints-not-bytes distance tests; the default
  // splitter would cut them one character at a time.
  indexer.index_document(document_key++, whole_term("東京"));
  indexer.index_document(document_key++, whole_term("東京都"));
  return invidx;
}

// enumerate_terms_with_edit_distance leaves the order unspecified, so every
// assertion here compares sorted vectors.
static std::vector<std::string>
terms_within_edits(const IInvertedIndex &invidx, const std::u32string &str,
                   size_t max_edits) {
  std::vector<std::string> terms;
  invidx.enumerate_terms_with_edit_distance(
      str, max_edits, [&](const auto &s) { terms.push_back(u8(s)); });
  std::sort(terms.begin(), terms.end());
  return terms;
}

TEST(FuzzySearchTest, EnumerateTermsWithEditDistance) {
  auto invidx = fuzzy_index();

  // Distance 0 is an exact match, and each step out admits the next ring of
  // terms: "ample"/"apply" are one edit from "apple", "maple" is two.
  EXPECT_EQ((std::vector<std::string>{"apple"}),
            terms_within_edits(invidx, U"apple", 0));
  EXPECT_EQ((std::vector<std::string>{"ample", "apple", "apply"}),
            terms_within_edits(invidx, U"apple", 1));
  EXPECT_EQ((std::vector<std::string>{"ample", "apple", "apply", "maple"}),
            terms_within_edits(invidx, U"apple", 2));
  EXPECT_TRUE(terms_within_edits(invidx, U"zzzzz", 2).empty());
}

TEST(FuzzySearchTest, CountsEachEditKind) {
  auto invidx = fuzzy_index();

  // All three edits count the same, and each of these needles is one edit of
  // exactly one kind away from "apple" and two or more from every other term.
  EXPECT_EQ((std::vector<std::string>{"apple"}), // substitute i -> a
            terms_within_edits(invidx, U"ipple", 1));
  EXPECT_EQ((std::vector<std::string>{"apple"}), // insert the missing a
            terms_within_edits(invidx, U"pple", 1));
  EXPECT_EQ((std::vector<std::string>{"apple"}), // delete the extra p
            terms_within_edits(invidx, U"appple", 1));
}

TEST(FuzzySearchTest, MeasuresDistanceInCodepointsNotBytes) {
  auto invidx = fuzzy_index();

  // One CJK codepoint is three UTF-8 bytes, so a byte-wise distance would
  // call this three edits and find nothing at 1.
  EXPECT_EQ((std::vector<std::string>{"東京"}),
            terms_within_edits(invidx, U"東今", 1));
  EXPECT_EQ((std::vector<std::string>{"東京", "東京都"}),
            terms_within_edits(invidx, U"東京", 1));
}

TEST(FuzzySearchTest, EmptyTermMatchesEveryTermWithinTheDistance) {
  auto invidx = fuzzy_index();

  // Every term is `its length` edits away from the empty string. The query
  // syntax never produces this (see DroppedTokenDoesNotBecomeMatchAll), but
  // the C++ API answers it per its documented contract.
  EXPECT_TRUE(terms_within_edits(invidx, U"", 1).empty());
  EXPECT_EQ((std::vector<std::string>{"東京"}),
            terms_within_edits(invidx, U"", 2));
}

TEST(FuzzySearchTest, TermTildeDigitsParsesAsFuzzy) {
  auto expr = parse_query(normalizer, "apple~2");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Fuzzy, expr->operation);
  EXPECT_EQ(U"apple", expr->term_str);
  EXPECT_EQ(2, expr->near_operation_distance);
}

// The `~` is NEAR's operator, so every established spelling of it has to keep
// parsing exactly as it did before Fuzzy existed. Each row here was captured
// from the parser before the FUZZY rule was added.
TEST(FuzzySearchTest, DoesNotDisturbTheNearOperator) {
  struct Case {
    const char *query;
    const char *description;
  };
  for (const auto &c : {
           Case{"apple ~ 2", "spaced, digits: the star has to touch its term"},
           Case{"apple ~ tree", "the ordinary spelling"},
           Case{"apple~ tree", "no digits after the tilde"},
           Case{"apple ~tree", "no digits after the tilde"},
           Case{"apple~tree", "no digits after the tilde"},
           Case{"apple~2x", "digits are not the end of the token"},
       }) {
    auto expr = parse_query(normalizer, c.query);
    ASSERT_TRUE(expr) << c.query;
    EXPECT_EQ(Operation::Near, expr->operation) << c.query << " -- " << c.description;
  }

  // A phrase keeps its own reading too: Lucene's `"..."~N` phrase slop is not
  // supported, and this stays the NEAR it always was.
  auto phrase = parse_query(normalizer, R"("apple tree"~2)");
  ASSERT_TRUE(phrase);
  EXPECT_EQ(Operation::Near, phrase->operation);
}

TEST(FuzzySearchTest, DistanceIsClampedAndNeverOverflows) {
  // A distance from a query string is end-user input: a huge one would defeat
  // both backends' pruning, and a huge enough one would overflow a naive
  // parse. It saturates at the cap instead.
  auto huge = parse_query(normalizer, "apple~99999999999999999999");
  ASSERT_TRUE(huge);
  EXPECT_EQ(Operation::Fuzzy, huge->operation);
  EXPECT_EQ(2, huge->near_operation_distance);

  auto within = parse_query(normalizer, "apple~1");
  ASSERT_TRUE(within);
  EXPECT_EQ(1, within->near_operation_distance);

  // Zero is a legitimate distance, not "unset": it means an exact match.
  auto zero = parse_query(normalizer, "apple~0");
  ASSERT_TRUE(zero);
  EXPECT_EQ(Operation::Fuzzy, zero->operation);
  EXPECT_EQ(0, zero->near_operation_distance);
}

TEST(FuzzySearchTest, AppliesToTheLastTermOfASplitToken) {
  auto expr = parse_query(normalizer, "well-known~1");
  ASSERT_TRUE(expr);
  ASSERT_EQ(Operation::Adjacent, expr->operation);
  ASSERT_EQ(2, expr->nodes.size());
  EXPECT_EQ(Operation::Term, expr->nodes[0].operation);
  EXPECT_EQ(Operation::Fuzzy, expr->nodes[1].operation);
  EXPECT_EQ(U"known", expr->nodes[1].term_str);
  EXPECT_EQ(1, expr->nodes[1].near_operation_distance);
}

TEST(FuzzySearchTest, DoesNotApplyToAPrefixNode) {
  // `app*~2` has no useful reading -- a distance around a prefix expansion is
  // meaningless -- so the `~2` is dropped and the Prefix stands, mirroring how
  // a `*` on a synonym expansion is dropped.
  auto expr = parse_query(normalizer, "app*~2");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Prefix, expr->operation);
  EXPECT_EQ(U"app", expr->term_str);
}

TEST(FuzzySearchTest, DroppedTokenDoesNotBecomeMatchAll) {
  auto invidx = fuzzy_index();

  // A stop-word filter drops "the" entirely, leaving an empty term. As a
  // Fuzzy that would match every term of length <= 2, so it has to stay a
  // Term, which matches nothing.
  TermFilter drop_the = [](const std::u32string &str, auto emit) {
    if (str != U"the") {
      emit(to_lowercase(str));
    }
  };

  auto expr = parse_query(drop_the, "the~2");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Term, expr->operation);
  EXPECT_TRUE(expr->term_str.empty());
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(FuzzySearchTest, QueryMatchesEveryTermWithinTheDistance) {
  auto invidx = fuzzy_index();

  // One edit from "apple" reaches "ample" (doc 0) and "apply" (doc 2).
  auto expr = parse_query(normalizer, "apple~1");
  ASSERT_TRUE(expr);
  EXPECT_EQ((std::vector<size_t>{0, 2}),
            hit_document_ids(*perform_search(invidx, *expr)));

  // Widening to two edits pulls in "maple" (doc 1) as well.
  auto wider = parse_query(normalizer, "apple~2");
  ASSERT_TRUE(wider);
  EXPECT_EQ((std::vector<size_t>{0, 1, 2}),
            hit_document_ids(*perform_search(invidx, *wider)));
}

TEST(FuzzySearchTest, QueryIsEquivalentToTheExplicitOr) {
  auto invidx = fuzzy_index();

  auto fuzzy_expr = parse_query(normalizer, "apple~1");
  auto or_expr = parse_query(normalizer, "ample|apple|apply");
  ASSERT_TRUE(fuzzy_expr);
  ASSERT_TRUE(or_expr);

  auto fuzzy_result = perform_search(invidx, *fuzzy_expr);
  auto or_result = perform_search(invidx, *or_expr);

  ASSERT_EQ(or_result->size(), fuzzy_result->size());
  EXPECT_EQ(hit_document_ids(*or_result), hit_document_ids(*fuzzy_result));

  // Scoring agrees too, which is also what pins down that a closer edit
  // distance carries no boost: every branch of the expansion is equal.
  for (size_t i = 0; i < fuzzy_result->size(); i++) {
    EXPECT_AP(bm25_score(invidx, *or_expr, *or_result, i),
              bm25_score(invidx, *fuzzy_expr, *fuzzy_result, i));
  }
}

TEST(FuzzySearchTest, NoMatchingTermYieldsNoHits) {
  auto invidx = fuzzy_index();

  auto expr = parse_query(normalizer, "zzzzz~2");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Fuzzy, expr->operation);
  EXPECT_EQ(0, perform_search(invidx, *expr)->size());
}

TEST(FuzzySearchTest, CombinesWithOtherOperators) {
  auto invidx = fuzzy_index();

  // Both an "apple"-ish term and "banana": only doc 1 (maple banana) has
  // both, and only once the distance reaches 2 and admits "maple".
  auto and_expr = parse_query(normalizer, "apple~2 banana");
  ASSERT_TRUE(and_expr);
  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, *and_expr)));

  // Negation over a fuzzy term: "banana" but nothing within one edit of
  // "maple", which excludes doc 1.
  auto not_expr = parse_query(normalizer, "banana -maple~1");
  ASSERT_TRUE(not_expr);
  EXPECT_EQ(0, perform_search(invidx, *not_expr)->size());
}

TEST(FuzzySearchTest, ExcludesLogicallyDeletedDocuments) {
  auto invidx = fuzzy_index();
  invidx.remove_document(0); // apple ample

  auto expr = parse_query(normalizer, "apple~1");
  ASSERT_TRUE(expr);
  EXPECT_EQ((std::vector<size_t>{2}),
            hit_document_ids(*perform_search(invidx, *expr)));
}

TEST(FuzzySearchTest, ExpandFuzzyRewritesToAnEquivalentOr) {
  auto invidx = fuzzy_index();

  auto parsed = parse_query(normalizer, "apple~1");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::Fuzzy, parsed->operation);

  auto expanded = expand_fuzzy(invidx, *parsed);
  ASSERT_EQ(Operation::Or, expanded.operation);
  ASSERT_EQ(3, expanded.nodes.size());
  // Sorted, so the expansion is reproducible for one term and one index.
  EXPECT_EQ(U"ample", expanded.nodes[0].term_str);
  EXPECT_EQ(U"apple", expanded.nodes[1].term_str);
  EXPECT_EQ(U"apply", expanded.nodes[2].term_str);

  auto result = perform_search(invidx, expanded);
  auto direct = perform_search(invidx, *parsed);
  ASSERT_EQ(direct->size(), result->size());
  for (size_t i = 0; i < result->size(); i++) {
    EXPECT_EQ(direct->document_ordinal(i), result->document_ordinal(i));
    EXPECT_AP(bm25_score(invidx, *parsed, *direct, i),
              bm25_score(invidx, expanded, *result, i));
  }
}

TEST(FuzzySearchTest, ExpandFuzzyRewritesNestedNodes) {
  auto invidx = fuzzy_index();

  auto parsed = parse_query(normalizer, "aple~2 banana");
  ASSERT_TRUE(parsed);
  ASSERT_EQ(Operation::And, parsed->operation);

  auto expanded = expand_fuzzy(invidx, *parsed);
  ASSERT_EQ(Operation::And, expanded.operation);
  ASSERT_EQ(2, expanded.nodes.size());
  EXPECT_EQ(Operation::Or, expanded.nodes[0].operation);
  EXPECT_EQ(Operation::Term, expanded.nodes[1].operation);
  EXPECT_EQ(U"banana", expanded.nodes[1].term_str);

  EXPECT_EQ((std::vector<size_t>{1}),
            hit_document_ids(*perform_search(invidx, expanded)));
}

TEST(FuzzySearchTest, CompressedBackendMatchesInMemory) {
  auto invidx = fuzzy_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  // The compressed backend drives fstlib's LevenshteinAutomaton over the FST
  // while the in-memory one runs a rolling-row DP per term, so this pins the
  // two implementations to the same answer -- including the empty term, where
  // fstlib's own edit_distance_search would have bailed out early.
  for (const auto *str : {U"apple", U"aple", U"zzzzz", U"東今", U""}) {
    for (size_t max_edits : {size_t{0}, size_t{1}, size_t{2}}) {
      EXPECT_EQ(terms_within_edits(invidx, str, max_edits),
                terms_within_edits(*loaded, str, max_edits))
          << u8(str) << "~" << max_edits;
    }
  }
}

TEST(FuzzySearchTest, CompressedEnumerationReusesItsBuffer) {
  auto invidx = fuzzy_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  std::vector<std::string> copied;
  loaded->enumerate_terms_with_edit_distance(
      U"apple", 1, [&](const auto &str) { copied.push_back(u8(str)); });
  std::sort(copied.begin(), copied.end());
  EXPECT_EQ((std::vector<std::string>{"ample", "apple", "apply"}), copied);
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

  for (const auto *term : {U"東", U"タワー", U"区", U"document", U"the"}) {
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

  EXPECT_TRUE(loaded->term_exists(U"タワー"));
  EXPECT_EQ(invidx.df(U"タワー"), loaded->df(U"タワー"));
  EXPECT_FALSE(loaded->term_exists(U"ワー"));  // a suffix is not a term
  EXPECT_FALSE(loaded->term_exists(U"タ"));    // nor is a prefix

  // predictive_search over UTF-8 must not split a multi-byte codepoint.
  EXPECT_EQ((std::vector<std::string>{"タワー"}),
            terms_with_prefix(*loaded, U"タ"));
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

  // Re-indexing the same document_key must subtract the previous count, not
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

TEST(StringKeyTest, IndexesRemovesAndRoundTrips) {
  // The other key type KeyTable serializes without help. Paths are the
  // natural key for a file corpus, and what the CLI uses.
  InMemoryInvertedIndex<TextRange, std::string> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document("notes/a.md", UTF8PlainTextTokenizer("the first note"));
  indexer.index_document("notes/b.md", UTF8PlainTextTokenizer("the second note"));
  indexer.index_document("notes/c.md", UTF8PlainTextTokenizer("something else"));

  EXPECT_EQ(3, invidx.document_count());
  EXPECT_EQ("notes/b.md", invidx.document_key(1));
  EXPECT_EQ(1, *invidx.document_ordinal("notes/b.md"));
  EXPECT_FALSE(invidx.document_ordinal("notes/z.md"));

  auto expr = parse_query(normalizer, "note");
  auto postings = perform_search(invidx, *expr);
  ASSERT_EQ(2, postings->size());
  EXPECT_EQ("notes/a.md", invidx.document_key(postings->document_ordinal(0)));
  EXPECT_EQ("notes/b.md", invidx.document_key(postings->document_ordinal(1)));

  invidx.remove_document("notes/a.md");
  invidx.remove_document("notes/nope.md"); // names nothing: a no-op
  EXPECT_EQ(2, invidx.document_count());
  EXPECT_EQ(1, perform_search(invidx, *expr)->size());

  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    invidx.save(ss, {}, format);

    InMemoryInvertedIndex<TextRange, std::string> loaded;
    loaded.load(ss);
    EXPECT_EQ(2, loaded.document_count());
    EXPECT_TRUE(
        loaded.is_document_removed(*loaded.document_ordinal("notes/a.md")));
    auto found = perform_search(loaded, *expr);
    ASSERT_EQ(1, found->size());
    EXPECT_EQ("notes/b.md", loaded.document_key(found->document_ordinal(0)));
  }

  // And through the read-only compressed backend, instantiated for strings.
  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto ro = load_compressed_index<std::string>(compressed);
  EXPECT_EQ(2, ro->document_count());
  auto found = perform_search(*ro, *expr);
  ASSERT_EQ(1, found->size());
  EXPECT_EQ("notes/b.md", ro->document_key(found->document_ordinal(0)));
  EXPECT_EQ(2, *ro->document_ordinal("notes/c.md"));
}

TEST(KeyTableTest, IntegerKeysRoundTripDenseOrNot) {
  // 0..n-1 in order is the identity and collapses to a flag on disk; any
  // other integer keys are written out. Both must come back as they went in.
  auto dense = sample_index(); // keys 0..4, in that order
  InMemoryInvertedIndex<TextRange> sparse;
  {
    InMemoryIndexer indexer(sparse, normalizer);
    indexer.index_document(1001001, UTF8PlainTextTokenizer("first"));
    indexer.index_document(3, UTF8PlainTextTokenizer("second"));
  }

  for (auto *invidx : {&dense, &sparse}) {
    for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
      std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
      invidx->save(ss, {}, format);
      InMemoryInvertedIndex<TextRange> loaded;
      loaded.load(ss);
      ASSERT_EQ(invidx->document_count(), loaded.document_count());
      for (size_t ordinal = 0; ordinal < loaded.document_count(); ordinal++) {
        EXPECT_EQ(invidx->document_key(ordinal), loaded.document_key(ordinal));
        EXPECT_EQ(ordinal, *loaded.document_ordinal(loaded.document_key(ordinal)));
      }
    }
  }
}

//-----------------------------------------------------------------------------
// Regressions
//-----------------------------------------------------------------------------

static std::vector<size_t> hit_positions(const IPostings &postings,
                                         size_t index) {
  std::vector<size_t> positions;
  for (size_t hit = 0; hit < postings.search_hit_count(index); hit++) {
    positions.push_back(postings.term_position(index, hit));
  }
  return positions;
}

// An update tombstones the old copy, which stays in the term's postings, so
// df must discount it: counting it made df exceed the live document count
// and BM25's idf the log of a negative number -- NaN for every score.
TEST(BM25Test, UpdatesDoNotSkewScores) {
  auto build = [](size_t updates) {
    InMemoryInvertedIndex<TextRange> invidx;
    InMemoryIndexer indexer(invidx, normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("apple banana"));
    indexer.index_document(1, UTF8PlainTextTokenizer("cherry"));
    indexer.index_document(2, UTF8PlainTextTokenizer("cherry date"));
    for (size_t i = 0; i < updates; i++) {
      indexer.index_document(0, UTF8PlainTextTokenizer("apple banana"));
    }
    return invidx;
  };
  auto fresh = build(0);
  auto updated = build(3);

  ASSERT_EQ(4, updated.postings(U"apple")->size());
  EXPECT_EQ(1, updated.df(U"apple"));
  EXPECT_EQ(fresh.document_count(), updated.document_count());

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  updated.save(ss, {}, IndexFormat::Compressed);
  auto compressed = load_compressed_index(ss);
  EXPECT_EQ(1, compressed->df(U"apple"));

  auto expr = *parse_query(normalizer, "apple | cherry");

  // Every scorer, keyed by document key, must agree with the index that
  // never saw the updates.
  auto scores = [&](const auto &invidx) {
    std::map<size_t, std::vector<double>> by_key;
    auto result = perform_search(invidx, expr);
    BM25Scorer scorer(invidx, expr);
    for (size_t i = 0; i < result->size(); i++) {
      auto key = invidx.document_key(result->document_ordinal(i));
      by_key[key] = {scorer(*result, i), bm25_score(invidx, expr, *result, i),
                     tf_idf_score(invidx, expr, *result, i)};
    }
    auto ranked = bm25_top_k(invidx, expr, 10);
    for (const auto &hit : ranked.hits) {
      auto key =
          invidx.document_key(ranked.postings->document_ordinal(hit.index));
      by_key[key].push_back(hit.score);
    }
    return by_key;
  };

  auto expected = scores(fresh);
  ASSERT_EQ(3, expected.size());
  for (const auto &actual : {scores(updated), scores(*compressed)}) {
    ASSERT_EQ(expected.size(), actual.size());
    for (const auto &[key, values] : expected) {
      ASSERT_EQ(values.size(), actual.at(key).size());
      for (size_t i = 0; i < values.size(); i++) {
        EXPECT_FALSE(std::isnan(actual.at(key)[i])) << key << " " << i;
        EXPECT_AP(values[i], actual.at(key)[i]);
      }
    }
  }
}

// The parser is shared by every call; each call's own term filter has to
// reach the terms it parses, however many threads are parsing at once.
TEST(QueryTest, ConcurrentParsesKeepTheirOwnNormalizer) {
  auto parse_many = [](Normalizer normalizer, std::u32string expected) {
    for (int i = 0; i < 2000; i++) {
      auto expr = parse_query(normalizer, "Apple");
      if (!expr || expr->term_str != expected) {
        return false;
      }
    }
    return true;
  };

  bool lower_ok = false;
  bool upper_ok = false;
  std::thread lower([&] {
    lower_ok = parse_many(
        [](const auto &str) { return unicode::to_lowercase(str); }, U"apple");
  });
  std::thread upper([&] {
    upper_ok = parse_many(
        [](const auto &str) { return unicode::to_uppercase(str); }, U"APPLE");
  });
  lower.join();
  upper.join();

  EXPECT_TRUE(lower_ok);
  EXPECT_TRUE(upper_ok);
}

TEST(QueryTest, LineBreaksSeparateTerms) {
  const auto &invidx = sample_index();
  auto expected = perform_search(invidx, *parse_query(normalizer, "is first"));
  ASSERT_EQ(1, expected->size());

  for (auto query : {"is\nfirst", "is\r\nfirst", "\n is first \r\n"}) {
    auto expr = parse_query(normalizer, query);
    ASSERT_TRUE(expr) << query;
    EXPECT_EQ(Operation::And, expr->operation);
    auto postings = perform_search(invidx, *expr);
    ASSERT_EQ(1, postings->size());
    EXPECT_EQ(expected->document_ordinal(0), postings->document_ordinal(0));
  }

  auto expr = parse_query(normalizer, "first\n|\nsecond");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Or, expr->operation);
}

// Each accepted group of hits is one per operand, and a later group can
// start before an earlier one ends; the result still has to read in
// ascending position order, which is_term_position's binary search and the
// k-way merge of an enclosing Or rely on.
TEST(NearTest, HitsComeOutInPositionOrder) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("z a a z z b b"));
  indexer.index_document(1, UTF8PlainTextTokenizer("c"));

  auto postings = perform_search(invidx, *parse_query(normalizer, "a ~ b"));
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ((std::vector<size_t>{1, 2, 5, 6}), hit_positions(*postings, 0));
  for (auto position : {1, 2, 5, 6}) {
    EXPECT_TRUE(postings->is_term_position(0, position)) << position;
  }

  auto merged = perform_search(invidx, *parse_query(normalizer, "(a ~ b) | c"));
  ASSERT_EQ(2, merged->size());
  EXPECT_EQ((std::vector<size_t>{1, 2, 5, 6}), hit_positions(*merged, 0));
}

// With three or more operands the test is that the chosen hits, in order,
// each start within the distance of the furthest end so far, and setting the
// earliest hit aside when a group fails (right for two operands) loses
// groups: c@1 a@4 b@9 fails on the gap before b, but a@5 closes it, and the
// earliest hit c@1 belongs to the group that results.
TEST(NearTest, ThreeOperandsFindAGroupTheEarliestHitIsPartOf) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("c c x x a a x x a b"));

  auto postings = perform_search(invidx, *parse_query(normalizer, "a ~ b ~ c"));
  ASSERT_EQ(1, postings->size());
  // c@1 a@5 b@9, and nothing else is on a group.
  EXPECT_EQ((std::vector<size_t>{1, 5, 9}), hit_positions(*postings, 0));
}

// The reach of a group is its furthest end, not the end of the hit before:
// after the phrase and the word inside it, d is 4 past the phrase's end.
TEST(NearTest, ThreeOperandsMeasureFromTheFurthestEnd) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("a b c x x x d"));
  indexer.index_document(1, UTF8PlainTextTokenizer("a b c x x x x d"));

  auto postings =
      perform_search(invidx, *parse_query(normalizer, R"("a b c" ~ b ~ d)"));
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ(0, postings->document_ordinal(0));
  EXPECT_EQ((std::vector<size_t>{0, 1, 6}), hit_positions(*postings, 0));
  EXPECT_EQ(3, postings->term_length(0, 0));
}

// Against every choice of one hit per operand, on random documents: the
// documents that match, and the hits that are on some matching group.
TEST(NearTest, ChainsAgreeWithABruteForce) {
  struct Query {
    const char *text;
    std::vector<int> operands; // indexes into the vocabulary
  };
  const std::vector<Query> queries = {
      {"a ~ b ~ c", {0, 1, 2}},
      {"a ~ b ~ c ~ d", {0, 1, 2, 3}},
      {"a ~ a ~ b", {0, 0, 1}},
      {"c ~ a ~ b ~ a", {2, 0, 1, 0}},
  };
  const char *vocabulary[] = {"a", "b", "c", "d", "x"};
  constexpr int kDistance = 4; // DEFAULT_NEAR_SIZE

  std::mt19937 rng(11);
  for (int round = 0; round < 1500; round++) {
    std::string text;
    std::vector<std::vector<int>> positions(5);
    int length = 1 + rng() % 18;
    for (int i = 0; i < length; i++) {
      int word = rng() % 5;
      text += std::string(i ? " " : "") + vocabulary[word];
      positions[word].push_back(i);
    }
    InMemoryInvertedIndex<TextRange> invidx;
    InMemoryIndexer indexer(invidx, normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer(text));

    for (const auto &query : queries) {
      std::set<size_t> expected;
      std::vector<size_t> choice(query.operands.size(), 0);
      bool possible = true;
      for (int operand : query.operands) {
        possible = possible && !positions[operand].empty();
      }
      while (possible) {
        std::vector<int> chosen;
        for (size_t j = 0; j < choice.size(); j++) {
          chosen.push_back(positions[query.operands[j]][choice[j]]);
        }
        auto sorted = chosen;
        std::sort(sorted.begin(), sorted.end());
        bool group = true;
        for (size_t i = 1; i < sorted.size(); i++) {
          group = group && sorted[i] - sorted[i - 1] <= kDistance;
        }
        if (group) {
          expected.insert(chosen.begin(), chosen.end());
        }
        size_t j = 0;
        for (; j < choice.size(); j++) {
          if (++choice[j] < positions[query.operands[j]].size()) {
            break;
          }
          choice[j] = 0;
        }
        possible = j < choice.size();
      }

      auto postings =
          perform_search(invidx, *parse_query(normalizer, query.text));
      std::set<size_t> actual;
      if (postings->size() == 1) {
        auto hits = hit_positions(*postings, 0);
        actual.insert(hits.begin(), hits.end());
      }
      EXPECT_EQ(expected, actual) << query.text << " on '" << text << "'";
      EXPECT_EQ(expected.empty() ? 0u : 1u, postings->size())
          << query.text << " on '" << text << "'";
    }
  }
}

// The hits of an Or or an And are the union of its operands' hits as a set:
// a position two operands share is one hit, not two. It used to come out
// once per operand, doubling the count and the highlights.
TEST(OrTest, AHitTwoOperandsShareIsOneHit) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("apple x apple pie"));
  indexer.index_document(1, UTF8PlainTextTokenizer("pie"));

  const std::vector<std::pair<const char *, std::vector<size_t>>> cases = {
      {"apple", {0, 2}},
      {"apple | apple", {0, 2}},
      {"apple apple", {0, 2}},
      {"apple | app*", {0, 2}},
      {"(apple | pie) | apple", {0, 2, 3}},
  };
  for (const auto &[query, expected] : cases) {
    auto postings = perform_search(invidx, *parse_query(normalizer, query));
    ASSERT_LE(1u, postings->size()) << query;
    ASSERT_EQ(0, postings->document_ordinal(0)) << query;
    EXPECT_EQ(expected.size(), postings->search_hit_count(0)) << query;
    EXPECT_EQ(expected, hit_positions(*postings, 0)) << query;
    EXPECT_TRUE(postings->is_term_position(0, 2)) << query;
  }
}

// Two hits at one position are two when they differ in length (a phrase and
// the word it starts with), and one when they do not.
TEST(OrTest, OnlyIdenticalHitsAreMerged) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("a b c"));

  auto both = perform_search(invidx, *parse_query(normalizer, R"("a b" | a)"));
  ASSERT_EQ(1, both->size());
  ASSERT_EQ(2, both->search_hit_count(0));
  EXPECT_EQ(0, both->term_position(0, 0));
  EXPECT_EQ(0, both->term_position(0, 1));
  EXPECT_EQ(
      std::set<size_t>({1, 2}),
      std::set<size_t>({both->term_length(0, 0), both->term_length(0, 1)}));

  auto same =
      perform_search(invidx, *parse_query(normalizer, R"("a b" | "a b")"));
  ASSERT_EQ(1, same->size());
  EXPECT_EQ(1, same->search_hit_count(0));
  EXPECT_EQ(2, same->term_length(0, 0));
}

// Two operands on the same hit used to collide in a map keyed by position,
// so one of them was neither recorded nor advanced.
TEST(NearTest, OperandsSharingAPositionCountOnce) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("a x a"));
  indexer.index_document(1, UTF8PlainTextTokenizer("apple pie apple"));

  auto same = perform_search(invidx, *parse_query(normalizer, "a ~ a"));
  ASSERT_EQ(1, same->size());
  EXPECT_EQ((std::vector<size_t>{0, 2}), hit_positions(*same, 0));

  auto prefix =
      perform_search(invidx, *parse_query(normalizer, "apple ~ app*"));
  ASSERT_EQ(1, prefix->size());
  EXPECT_EQ((std::vector<size_t>{0, 2}), hit_positions(*prefix, 0));
}

// A phrase reaching past the start of the next operand is no gap at all;
// the distance used to underflow there and reject the document.
TEST(NearTest, OverlappingOperandsAreWithinDistance) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("a b c"));

  for (auto query : {R"("a b c" ~ b)", R"("a b c" ~ "b c")"}) {
    auto postings = perform_search(invidx, *parse_query(normalizer, query));
    ASSERT_EQ(1, postings->size()) << query;
    EXPECT_EQ(0, postings->term_position(0, 0)) << query;
    EXPECT_EQ(3, postings->term_length(0, 0)) << query;
  }

  // The gap after an overlap is measured from the furthest end reached:
  // "a b c" ends at 2, so x at 2 + 5 is one past a distance of 4.
  indexer.index_document(1, UTF8PlainTextTokenizer("a b c d e f g x"));
  indexer.index_document(2, UTF8PlainTextTokenizer("a b c d e f x"));
  auto far =
      perform_search(invidx, *parse_query(normalizer, R"("a b c" ~ b ~ x)"));
  ASSERT_EQ(1, far->size());
  EXPECT_EQ(2, invidx.document_key(far->document_ordinal(0)));
}

TEST(SameScopeTest, HitsComeOutInPositionOrder) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("z a a z z b b"));
  invidx.set_scope_ids("paragraph", 0, {0, 0, 0, 0, 0, 0, 0});
  indexer.index_document(1, UTF8PlainTextTokenizer("a x a"));
  invidx.set_scope_ids("paragraph", 1, {0, 0, 1});

  auto postings = perform_search(
      invidx, same_scope_expr("paragraph", {U"a", U"b"}), &invidx);
  ASSERT_EQ(1, postings->size());
  EXPECT_EQ((std::vector<size_t>{1, 2, 5, 6}), hit_positions(*postings, 0));

  auto same = perform_search(invidx, same_scope_expr("paragraph", {U"a", U"a"}),
                             &invidx);
  ASSERT_EQ(2, same->size());
  EXPECT_EQ((std::vector<size_t>{1, 2}), hit_positions(*same, 0));
  EXPECT_EQ((std::vector<size_t>{0, 2}), hit_positions(*same, 1));
}

// A tokenizer that throws -- here the indexer's own position check, on a
// tokenizer that skips from position 0 to 5 -- used to leave the old copy
// tombstoned and a half-indexed new one live under the key.
TEST(RemoveDocumentTest, FailedReindexLeavesTheIndexUnchanged) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(7, UTF8PlainTextTokenizer("old text"));
  indexer.index_document(8, UTF8PlainTextTokenizer("other"));

  Tokenizer<TextRange> skips = [](Normalizer, auto callback) {
    callback(U"new", 0, TextRange{0, 3});
    callback(U"brandnew", 5, TextRange{4, 8});
  };
  EXPECT_THROW(indexer.index_document(7, skips), std::runtime_error);

  struct Failure {};
  Tokenizer<TextRange> fails = [](Normalizer, auto callback) {
    callback(U"new", 0, TextRange{0, 3});
    throw Failure{};
  };
  EXPECT_THROW(indexer.index_document(7, fails), Failure);

  EXPECT_EQ(2, invidx.document_count());
  EXPECT_FALSE(invidx.has_removed_documents());
  // Neither the terms the failed calls introduced nor their postings stay.
  EXPECT_FALSE(invidx.term_exists(U"new"));
  EXPECT_FALSE(invidx.term_exists(U"brandnew"));

  auto old = perform_search(invidx, *parse_query(normalizer, "old"));
  ASSERT_EQ(1, old->size());
  auto ordinal = old->document_ordinal(0);
  EXPECT_EQ(7, invidx.document_key(ordinal));
  EXPECT_EQ(2, invidx.document_term_count(ordinal));
  EXPECT_EQ(0u, invidx.text_range(*old, 0, 0).position);

  // And the next call goes through as if nothing had happened.
  indexer.index_document(7, UTF8PlainTextTokenizer("new text"));
  EXPECT_EQ(2, invidx.document_count());
  EXPECT_EQ(0, perform_search(invidx, *parse_query(normalizer, "old"))->size());
  EXPECT_EQ(1, perform_search(invidx, *parse_query(normalizer, "new"))->size());
}

// The compressed format codes a document's text ranges as one ascending
// sequence; ranges out of order used to be accepted here and then written
// past the end of the Elias-Fano bit vector on a Compressed save.
TEST(RemoveDocumentTest, TextRangesOutOfOrderAreRejected) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("kept"));

  Tokenizer<TextRange> backwards = [](Normalizer, auto callback) {
    callback(U"late", 0, TextRange{1000, 5});
    callback(U"early", 1, TextRange{0, 5});
  };
  EXPECT_THROW(indexer.index_document(1, backwards), std::runtime_error);
  EXPECT_EQ(1, invidx.document_count());

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(ss);
  EXPECT_EQ(1, loaded->document_count());
}

TEST(RemoveDocumentTest, TombstoningAnUnknownOrdinalChangesNothing) {
  InMemoryInvertedIndexBase base;

  // The ordinal joined the removed set before the lookup that rejects it, so
  // the live count (documents minus removed) wrapped around.
  EXPECT_THROW(base.tombstone(5), std::out_of_range);
  EXPECT_EQ(0, base.document_count());
  EXPECT_FALSE(base.has_removed_documents());
}

// A removed ordinal past the documents a file holds would make the live
// count (documents minus removed) wrap around to 2^64 - 1.
TEST(PersistenceTest, RejectsRemovedOrdinalOutOfRange) {
  auto save = [](size_t removed_key, IndexFormat format) {
    auto invidx = sample_index();
    invidx.remove_document(removed_key);
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    invidx.save(ss, {}, format);
    return ss.str();
  };

  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    // Two files that differ only in which ordinal is removed locate the
    // removed-ordinal field; point it past every document.
    auto bytes = save(1, format);
    auto other = save(2, format);
    ASSERT_EQ(bytes.size(), other.size());
    auto at = std::mismatch(bytes.begin(), bytes.end(), other.begin()).first -
              bytes.begin();
    ASSERT_LT(static_cast<size_t>(at) + sizeof(uint64_t), bytes.size());
    uint64_t past_the_end = sample_documents.size() + 100;
    std::memcpy(&bytes[at], &past_the_end, sizeof(past_the_end));

    {
      std::stringstream ss(bytes, std::ios::in | std::ios::binary);
      InMemoryInvertedIndex<TextRange> loaded;
      EXPECT_THROW(loaded.load(ss), std::runtime_error);
    }
    if (format == IndexFormat::Compressed) {
      std::stringstream ss(bytes, std::ios::in | std::ios::binary);
      EXPECT_THROW(load_compressed_index(ss), std::runtime_error);
    }
  }
}

// parse_query never puts a Not anywhere but directly under an And, but a
// tree built by hand can, and its operand used to come back null and be
// dereferenced.
TEST(NotTest, MisplacedNotIsRejected) {
  const auto &invidx = sample_index();
  auto term = [](std::u32string str) {
    return Expression{Operation::Term, std::move(str)};
  };
  auto not_of = [](Expression operand) {
    return Expression{
        Operation::Not, std::u32string(), 0, {std::move(operand)}};
  };
  auto node = [](Operation operation, std::vector<Expression> nodes) {
    return Expression{operation, std::u32string(), 4, std::move(nodes)};
  };

  for (const auto &expr :
       {node(Operation::Or, {term(U"first"), not_of(term(U"second"))}),
        node(Operation::Near, {term(U"first"), not_of(term(U"second"))}),
        not_of(term(U"second")),
        node(Operation::And, {not_of(term(U"second"))}),
        node(Operation::And, {term(U"first"), node(Operation::Not, {})})}) {
    EXPECT_THROW(perform_search(invidx, expr), std::invalid_argument);
    EXPECT_THROW(bm25_top_k(invidx, expr, 10), std::invalid_argument);
  }

  // The placement parse_query produces still works.
  auto valid =
      node(Operation::And, {term(U"document"), not_of(term(U"third"))});
  EXPECT_EQ(3, perform_search(invidx, valid)->size());
}

TEST(WildcardSearchTest, PatternWithoutLiteralsMatchesNothing) {
  const auto &invidx = sample_index();
  TermFilter drop_the = [](const std::u32string &str, auto emit) {
    if (str != U"the") {
      emit(to_lowercase(str));
    }
  };

  for (auto query : {"**", "***", "*the*", "the**"}) {
    auto expr = parse_query(drop_the, query);
    ASSERT_TRUE(expr) << query;
    EXPECT_EQ(Operation::Term, expr->operation) << query;
    EXPECT_TRUE(expr->term_str.empty()) << query;
    EXPECT_EQ(0, perform_search(invidx, *expr)->size()) << query;
  }

  // A pattern with any literal left is still a wildcard.
  auto expr = parse_query(drop_the, "*the*ird");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Wildcard, expr->operation);
  EXPECT_EQ(U"*ird", expr->term_str);
}

// df counts live documents, once each however often the term occurs in one,
// and is kept current as documents are removed or replaced rather than worked
// out from the postings (which keep tombstoned documents) at each call.
TEST(RemoveDocumentTest, DfTracksLiveDocuments) {
  InMemoryInvertedIndex<TextRange> a;
  InMemoryIndexer indexer(a, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("apple apple banana"));
  indexer.index_document(1, UTF8PlainTextTokenizer("apple cherry"));
  indexer.index_document(2, UTF8PlainTextTokenizer("banana"));

  EXPECT_EQ(2, a.df(U"apple")); // doc 0 twice is still one document
  EXPECT_EQ(2, a.df(U"banana"));
  EXPECT_EQ(1, a.df(U"cherry"));
  EXPECT_EQ(0, a.df(U"missing"));

  a.remove_document(0);
  EXPECT_EQ(1, a.df(U"apple"));
  EXPECT_EQ(1, a.df(U"banana"));
  a.remove_document(0); // removing it again takes nothing off twice
  EXPECT_EQ(1, a.df(U"apple"));

  // An update tombstones the old copy: the postings list both, df one.
  indexer.index_document(1, UTF8PlainTextTokenizer("cherry date"));
  EXPECT_EQ(0, a.df(U"apple"));
  EXPECT_EQ(1, a.df(U"cherry"));
  EXPECT_EQ(2, a.postings(U"cherry")->size());
  EXPECT_EQ(1, a.df(U"date"));
  EXPECT_LE(a.df(U"cherry"), a.document_count());

  // A copy has its own counts.
  auto b = a;
  b.remove_document(2);
  EXPECT_EQ(0, b.df(U"banana"));
  EXPECT_EQ(1, a.df(U"banana"));
}

// What a load rebuilds must agree with the index that was saved, and keep
// counting once documents are removed from it.
TEST(RemoveDocumentTest, DfSurvivesSaveAndLoad) {
  InMemoryInvertedIndex<TextRange> a;
  InMemoryIndexer indexer(a, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("apple apple banana"));
  indexer.index_document(1, UTF8PlainTextTokenizer("apple cherry"));
  indexer.index_document(2, UTF8PlainTextTokenizer("banana"));
  a.remove_document(0);
  indexer.index_document(1, UTF8PlainTextTokenizer("cherry date"));

  const std::u32string terms[] = {U"apple", U"banana", U"cherry", U"date",
                                  U"missing"};
  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    a.save(ss, {}, format);
    std::string bytes = ss.str();

    InMemoryInvertedIndex<TextRange> loaded;
    std::stringstream in(bytes, std::ios::in | std::ios::binary);
    loaded.load(in);
    for (const auto &term : terms) {
      EXPECT_EQ(a.df(term), loaded.df(term)) << u8(term);
    }
    loaded.remove_document(2);
    EXPECT_EQ(0, loaded.df(U"banana"));
    loaded.remove_document(1);
    EXPECT_EQ(0, loaded.df(U"cherry"));
    EXPECT_EQ(0, loaded.df(U"date"));

    if (format == IndexFormat::Compressed) {
      std::stringstream again(bytes, std::ios::in | std::ios::binary);
      auto read_only = load_compressed_index(again);
      for (const auto &term : terms) {
        EXPECT_EQ(a.df(term), read_only->df(term)) << u8(term);
      }
    }
  }
}

// A re-index that fails leaves df where it was, new terms and all.
TEST(RemoveDocumentTest, FailedReindexLeavesDfUnchanged) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("apple banana"));

  Tokenizer<TextRange> fails = [](Normalizer, auto callback) {
    callback(U"apple", 0, TextRange{0, 5});
    callback(U"fresh", 1, TextRange{6, 5});
    throw std::runtime_error("tokenizer failed");
  };
  EXPECT_THROW(indexer.index_document(0, fails), std::runtime_error);
  EXPECT_EQ(1, invidx.df(U"apple"));
  EXPECT_EQ(0, invidx.df(U"fresh"));

  indexer.index_document(0, UTF8PlainTextTokenizer("fresh"));
  EXPECT_EQ(0, invidx.df(U"apple"));
  EXPECT_EQ(1, invidx.df(U"fresh"));
}

//-----------------------------------------------------------------------------
// Corrupt input
//-----------------------------------------------------------------------------

// A length read out of a file is a claim, not a fact. These are refused as a
// truncated stream is -- a runtime_error -- rather than sized into an
// allocation (bad_alloc, or length_error from a size past max_size).
TEST(CorruptIndexTest, ClaimedLengthsAreRefusedNotAllocated) {
  auto stream = [](uint64_t claimed) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    detail::write_scalar<uint64_t>(ss, claimed);
    ss.write("abc", 3);
    return ss;
  };
  for (uint64_t claimed :
       {uint64_t(1) << 40, uint64_t(1) << 62, ~uint64_t(0)}) {
    {
      auto ss = stream(claimed);
      EXPECT_THROW(detail::read_bytes(ss), std::runtime_error) << claimed;
    }
    {
      auto ss = stream(claimed);
      EXPECT_THROW(detail::read_u32string(ss), std::runtime_error) << claimed;
    }
    {
      auto ss = stream(claimed);
      detail::BitVector bits;
      EXPECT_THROW(bits.load(ss), std::runtime_error) << claimed;
    }
  }

  // Short, honest values still read back.
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  detail::write_bytes(ss, "hello");
  detail::write_u32string(ss, U"東京");
  EXPECT_EQ("hello", detail::read_bytes(ss));
  EXPECT_EQ(U"東京", detail::read_u32string(ss));
}

// Overwrite every 8 bytes of a saved index in turn with a huge count and
// load it: whatever the field was, the load either goes through or says so
// with a runtime_error. In particular it never tries to allocate what the
// count claims. (Plain format: the Compressed one carries an FST whose
// bytes are not validated by the vendored fstlib.)
TEST(CorruptIndexTest, HugeCountAnywhereOnlyEverThrowsRuntimeError) {
  auto invidx = sample_index();
  invidx.remove_document(1);
  invidx.set_scope_ids("paragraph", 0,
                       {0, 0, 1, 1, 1}); // "This is the first document."
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);
  const std::string good = ss.str();

  const uint64_t huge = uint64_t(1) << 62;
  size_t refused = 0;
  for (size_t at = 0; at + sizeof(huge) <= good.size(); at++) {
    auto bytes = good;
    std::memcpy(&bytes[at], &huge, sizeof(huge));
    std::stringstream in(bytes, std::ios::in | std::ios::binary);
    InMemoryInvertedIndex<TextRange> loaded;
    try {
      loaded.load(in);
    } catch (const std::runtime_error &) {
      refused++;
    } catch (const std::exception &e) {
      ADD_FAILURE() << "offset " << at << ": " << typeid(e).name() << ": "
                    << e.what();
    }
  }
  // Counts are scattered through the file; plenty of the windows hit one.
  EXPECT_GT(refused, 20u);
}

// A key section is one key per document; the file's own count of them must
// say so before anything is sized from it.
TEST(CorruptIndexTest, KeySectionCountMustMatchTheDocuments) {
  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    auto invidx = sample_index();
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    invidx.save(ss, {}, format);
    auto bytes = ss.str();

    // The dense key section opens with its count, then a flag of 1; find it
    // as the last "count, 1" pair before the text ranges by looking for the
    // document count where the section starts.
    uint64_t count = sample_documents.size();
    uint32_t dense = 1;
    std::string pattern(sizeof(count) + sizeof(dense), '\0');
    std::memcpy(&pattern[0], &count, sizeof(count));
    std::memcpy(&pattern[sizeof(count)], &dense, sizeof(dense));
    auto at = bytes.rfind(pattern);
    ASSERT_NE(std::string::npos, at);

    uint64_t lie = uint64_t(1) << 40;
    std::memcpy(&bytes[at], &lie, sizeof(lie));
    std::stringstream in(bytes, std::ios::in | std::ios::binary);
    InMemoryInvertedIndex<TextRange> loaded;
    EXPECT_THROW(loaded.load(in), std::runtime_error);
    if (format == IndexFormat::Compressed) {
      std::stringstream again(bytes, std::ios::in | std::ios::binary);
      EXPECT_THROW(load_compressed_index(again), std::runtime_error);
    }
  }
}

// The df bookkeeping indexes by the ordinals in the postings, so a postings
// list naming a document the index does not have is refused at load.
TEST(CorruptIndexTest, PostingsNamingAnUnknownDocumentAreRefused) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  indexer.index_document(0, UTF8PlainTextTokenizer("apple"));
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(ss);
  auto bytes = ss.str();

  // The only posting's ordinal is 0 and no other field in a one-document
  // index holds a 0 followed by the 1 position count and position 0, so
  // look for ordinal 0, count 1, position 0 as three uint64s.
  uint64_t entry[3] = {0, 1, 0};
  auto at = bytes.find(
      std::string(reinterpret_cast<const char *>(entry), sizeof(entry)));
  ASSERT_NE(std::string::npos, at);
  uint64_t past_the_end = 50;
  std::memcpy(&bytes[at], &past_the_end, sizeof(past_the_end));

  std::stringstream in(bytes, std::ios::in | std::ios::binary);
  InMemoryInvertedIndex<TextRange> loaded;
  EXPECT_THROW(loaded.load(in), std::runtime_error);
}

// A postings list a file describes can be anything. The invariants the rest
// of the library leans on -- every entry has a position, ordinals strictly
// ascend, so do the positions within an entry -- are checked on load. An
// entry without positions used to reach save_compressed and read past an
// empty vector, and a list out of order was saved as a different one.
TEST(CorruptIndexTest, PostingsOutOfOrderOrEmptyAreRefusedByEveryFormat) {
  using Postings = InMemoryInvertedIndexBase::Postings;
  using Entries = std::vector<std::pair<uint64_t, std::vector<uint64_t>>>;

  auto plain = [](const Entries &entries) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    detail::write_scalar<uint64_t>(ss, entries.size());
    for (const auto &[ordinal, positions] : entries) {
      detail::write_scalar<uint64_t>(ss, ordinal);
      detail::write_scalar<uint64_t>(ss, positions.size());
      for (auto position : positions) {
        detail::write_scalar<uint64_t>(ss, position);
      }
    }
    return ss;
  };
  auto varint = [](const Entries &entries) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    detail::write_varint(ss, entries.size());
    uint64_t previous_ordinal = 0;
    for (const auto &[ordinal, positions] : entries) {
      detail::write_varint(ss, ordinal - previous_ordinal);
      previous_ordinal = ordinal;
      detail::write_varint(ss, positions.size());
      uint64_t previous_position = 0;
      for (auto position : positions) {
        detail::write_varint(ss, position - previous_position);
        previous_position = position;
      }
    }
    return ss;
  };

  struct Case {
    const char *what;
    Entries entries;
  };
  for (const auto &c : std::vector<Case>{
           {"an entry with no positions", {{3, {}}}},
           {"no positions in the second entry", {{3, {1}}, {5, {}}}},
           {"ordinals descending", {{5, {1}}, {3, {1}}}},
           {"the same ordinal twice", {{3, {1}}, {3, {2}}}},
           {"positions descending", {{3, {9, 4}}}},
           {"the same position twice", {{3, {4, 4}}}},
       }) {
    {
      auto ss = plain(c.entries);
      Postings postings;
      EXPECT_THROW(postings.load(ss), std::runtime_error) << c.what;
    }
    // The varint form writes a descent as a difference that wraps around, and
    // reads back as the smaller value it started from.
    {
      auto ss = varint(c.entries);
      Postings postings;
      EXPECT_THROW(postings.load_varint(ss), std::runtime_error) << c.what;
    }
  }
}

// Elias-Fano allows a value twice, so a compressed list can repeat an ordinal.
TEST(CorruptIndexTest, CompressedPostingsRepeatingAnOrdinalAreRefused) {
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  detail::EliasFano({3, 3}, 4).save(ss); // ordinals
  detail::EliasFano({1, 2}, 3).save(ss); // end offsets
  detail::EliasFano({0, 1}, 2).save(ss); // bases
  detail::EliasFano({0, 1}, 2).save(ss); // monotonized positions
  InMemoryInvertedIndexBase::Postings postings;
  EXPECT_THROW(postings.load_compressed(ss), std::runtime_error);
}
