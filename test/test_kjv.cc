#include <gtest/gtest.h>
#include <searchlib.h>

#include <filesystem>
#include <fstream>
#include <sstream>

#include "test_utils.h"

using namespace searchlib;

const auto KJV_PATH = "../../test/t_kjv.tsv";

auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

static auto kjv_index() {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  std::ifstream fs(KJV_PATH);
  if (fs) {
    std::string line;
    while (std::getline(fs, line)) {
      auto fields = split(line, '\t');
      auto document_id = std::stoi(fields[0]);
      const auto &s = fields[4];

      indexer.index_document(document_id, UTF8PlainTextTokenizer(s));
    }
  }
  return invidx;
}

TEST(KJVTest, SimpleTest) {
  const auto &invidx = kjv_index();

  {
    auto expr = parse_query(normalizer, R"( apple )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(8, postings->size());

    auto term = U"apple";
    EXPECT_EQ(8, invidx.df(term));

    EXPECT_AP(0.411, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.745, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.852, tf_idf_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.351, tf_idf_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.341, tf_idf_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.341, tf_idf_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.298, tf_idf_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.385, tf_idf_score(invidx, *expr, *postings, 7));

    EXPECT_AP(0.660, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(1.753, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(2.146, bm25_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.500, bm25_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.475, bm25_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.475, bm25_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.374, bm25_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.588, bm25_score(invidx, *expr, *postings, 7));
  }

  {
    auto expr = parse_query(normalizer, R"( "apple tree" )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(3, postings->size());

    EXPECT_EQ(1, postings->search_hit_count(0));
    EXPECT_EQ(1, postings->search_hit_count(1));
    EXPECT_EQ(1, postings->search_hit_count(2));

    EXPECT_EQ(2, term_count_score(invidx, *expr, *postings, 0));
    EXPECT_EQ(2, term_count_score(invidx, *expr, *postings, 1));
    EXPECT_EQ(5, term_count_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.572, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.556, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.051, tf_idf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.817, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.776, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.285, bm25_score(invidx, *expr, *postings, 2));
  }
}

TEST(KJVTest, CompressedPersistenceRoundTrip) {
  const auto &invidx = kjv_index();

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(plain);
  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto plain_size = plain.str().size();
  auto compressed_size = compressed.str().size();
  EXPECT_LT(compressed_size, plain_size);
  std::cout << "KJV index size: plain=" << plain_size
            << " compressed=" << compressed_size << " ("
            << (compressed_size * 100.0 / plain_size) << "%)" << std::endl;

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(compressed);

  EXPECT_EQ(invidx.document_count(), loaded.document_count());
  EXPECT_EQ(invidx.df(U"apple"), loaded.df(U"apple"));
  EXPECT_EQ(invidx.df(U"the"), loaded.df(U"the")); // high-df: EF-encoded

  auto expr = parse_query(normalizer, R"( apple )");
  auto expected = perform_search(invidx, *expr);
  auto actual = perform_search(loaded, *expr);
  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_id(i), actual->document_id(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      EXPECT_EQ(expected->term_position(i, h), actual->term_position(i, h));
    }
    EXPECT_AP(bm25_score(invidx, *expr, *expected, i),
              bm25_score(loaded, *expr, *actual, i));
  }
}

TEST(KJVTest, UTF8DecodePerformance) {
  // auto normalizer = [](const auto &str) {
  //   return unicode::to_lowercase(str);
  // };
  auto normalizer = to_lowercase;

  std::ifstream fs(KJV_PATH);

  std::string s;
  while (std::getline(fs, s)) {
    UTF8PlainTextTokenizer tokenizer(s);
    tokenizer(normalizer, [&](auto &str, auto, auto) {});
  }
}

