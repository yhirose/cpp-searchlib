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

TEST(KJVTest, CompressedBackend) {
  const auto &invidx = kjv_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto loaded = load_compressed_index(compressed);

  EXPECT_EQ(invidx.document_count(), loaded->document_count());
  EXPECT_DOUBLE_EQ(invidx.average_document_term_count(),
                   loaded->average_document_term_count());
  EXPECT_EQ(invidx.df(U"apple"), loaded->df(U"apple"));
  EXPECT_EQ(invidx.df(U"the"), loaded->df(U"the")); // high-df: EF-encoded
  EXPECT_EQ(invidx.term_count(U"the"), loaded->term_count(U"the"));
  EXPECT_TRUE(loaded->term_exists(U"apple"));
  EXPECT_FALSE(loaded->term_exists(U"zzzzz"));
  EXPECT_FALSE(loaded->has_removed_documents());

  // "apple" stays plain-coded, "the" is EF-coded, and the phrase query
  // exercises is_term_position and multi-term text ranges on both paths.
  for (const auto *query : {R"( apple )", R"( the )", R"( "apple tree" )",
                            R"( "the lord" )"}) {
    auto expr = parse_query(normalizer, query);
    ASSERT_TRUE(expr);

    auto expected = perform_search(invidx, *expr);
    auto actual = perform_search(*loaded, *expr);
    ASSERT_EQ(expected->size(), actual->size()) << query;
    for (size_t i = 0; i < expected->size(); i++) {
      EXPECT_EQ(expected->document_id(i), actual->document_id(i));
      ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
      for (size_t h = 0; h < expected->search_hit_count(i); h++) {
        EXPECT_EQ(expected->term_position(i, h), actual->term_position(i, h));

        auto expected_range = invidx.text_range(*expected, i, h);
        auto actual_range = loaded->text_range(*actual, i, h);
        EXPECT_EQ(expected_range.position, actual_range.position);
        EXPECT_EQ(expected_range.length, actual_range.length);
      }
      EXPECT_AP(bm25_score(invidx, *expr, *expected, i),
                bm25_score(*loaded, *expr, *actual, i));
    }
  }
}

TEST(KJVTest, CompressedBackendTombstones) {
  auto invidx = kjv_index();
  invidx.remove_document(1001);

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto loaded = load_compressed_index(compressed);
  EXPECT_TRUE(loaded->has_removed_documents());
  EXPECT_TRUE(loaded->is_document_removed(1001));
  EXPECT_FALSE(loaded->is_document_removed(1002));
}

TEST(KJVTest, CompressedBackendRejectsPlainFormat) {
  const auto &invidx = kjv_index();

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(plain);

  EXPECT_THROW(load_compressed_index(plain), std::runtime_error);
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

