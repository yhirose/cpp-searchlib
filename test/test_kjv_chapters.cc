#include <gtest/gtest.h>
#include <searchlib.h>

#include <filesystem>
#include <fstream>

#include "test_utils.h"

using namespace searchlib;

const auto KJV_PATH = "../../test/t_kjv_chapters.tsv";

static auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

static auto kjv_index() {
  auto index = new InMemoryInvertedIndex<TextRange>();

  InMemoryIndexer<TextRange> indexer(*index, normalizer);
  {
    std::ifstream fs(KJV_PATH);
    if (fs) {
      std::string line;
      while (std::getline(fs, line)) {
        auto fields = split(line, '\t');
        auto document_key = std::stoi(fields[0]);
        const auto &s = fields[1];

        indexer.index_document(document_key, UTF8PlainTextTokenizer(s));
      }
    }
  }

  return std::shared_ptr<IInvertedIndexWithTextRange<TextRange>>(index);
}

TEST(KJVChapterTest, SimpleTest) {
  auto p = kjv_index();
  const auto &invidx = *p;

  {
    auto expr = parse_query(normalizer, R"( apple )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(8, postings->size());

    auto term = U"apple";
    EXPECT_EQ(8, invidx.df(term));

    // The TSV's ids are book/chapter numbers, so they are the caller's keys
    // and come back through document_key; the ordinals underneath are the
    // file's line numbers.
    EXPECT_EQ(532, invidx.document_key(postings->document_ordinal(0)));
    EXPECT_EQ(1917, invidx.document_key(postings->document_ordinal(1)));
    EXPECT_EQ(2007, invidx.document_key(postings->document_ordinal(2)));
    EXPECT_EQ(2202, invidx.document_key(postings->document_ordinal(3)));
    EXPECT_EQ(2208, invidx.document_key(postings->document_ordinal(4)));
    EXPECT_EQ(2502, invidx.document_key(postings->document_ordinal(5)));
    EXPECT_EQ(2901, invidx.document_key(postings->document_ordinal(6)));
    EXPECT_EQ(3802, invidx.document_key(postings->document_ordinal(7)));

    EXPECT_EQ(1, postings->search_hit_count(0));
    EXPECT_EQ(1, postings->search_hit_count(1));
    EXPECT_EQ(1, postings->search_hit_count(2));
    EXPECT_EQ(1, postings->search_hit_count(3));
    EXPECT_EQ(1, postings->search_hit_count(4));
    EXPECT_EQ(1, postings->search_hit_count(5));
    EXPECT_EQ(1, postings->search_hit_count(6));
    EXPECT_EQ(1, postings->search_hit_count(7));

    EXPECT_AP(0.00549139, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0230779, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0174205, tf_idf_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.020448, tf_idf_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.0198816, tf_idf_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.00811905, tf_idf_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.0141007, tf_idf_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.0226411, tf_idf_score(invidx, *expr, *postings, 7));

    EXPECT_AP(0.00583253, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0697716, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0443892, bm25_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.0575726, bm25_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.0550316, bm25_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.011908, bm25_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.0312082, bm25_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.0677023, bm25_score(invidx, *expr, *postings, 7));
  }

  {
    auto expr = parse_query(normalizer, R"( apple tree )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(3, postings->size());

    EXPECT_EQ(2202, invidx.document_key(postings->document_ordinal(0)));
    EXPECT_EQ(2208, invidx.document_key(postings->document_ordinal(1)));
    EXPECT_EQ(2901, invidx.document_key(postings->document_ordinal(2)));

    EXPECT_EQ(3, postings->search_hit_count(0));
    EXPECT_EQ(2, postings->search_hit_count(1));
    EXPECT_EQ(6, postings->search_hit_count(2));

    EXPECT_AP(0.0391522, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0289746, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0463462, tf_idf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.108137, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.079287, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0994374, bm25_score(invidx, *expr, *postings, 2));
  }

  {
    auto expr = parse_query(normalizer, R"( Joshua Jericho )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(18, postings->size());

    {
      size_t i = 0;
      for (auto expected : {426, 434, 534, 602, 603, 604, 605, 606, 607, 608,
                            609, 610, 612, 613, 618, 620, 624, 1116}) {
        EXPECT_EQ(expected, invidx.document_key(postings->document_ordinal(i)));
        i++;
      }
    }

    {
      size_t i = 0;
      for (auto expected :
           {3, 2, 3, 6, 7, 13, 12, 15, 13, 19, 10, 31, 3, 2, 7, 2, 15, 2}) {
        EXPECT_EQ(expected, postings->search_hit_count(i));
        i++;
      }
    }

    {
      size_t i = 0;
      for (auto expected :
           {0.00982997, 0.0149824, 0.0444499, 0.0367272, 0.057788, 0.0860655,
            0.10183, 0.077544, 0.0654762, 0.0724664, 0.0583369, 0.103775,
            0.0289974, 0.0114411, 0.0426484, 0.0306458, 0.0671168,
            0.00910212}) {
        EXPECT_AP(expected, tf_idf_score(invidx, *expr, *postings, i));
        i++;
      }
    }

    {
      size_t i = 0;
      for (auto expected :
           {0.00955058, 0.0284355, 0.131614, 0.059746, 0.117627, 0.148535,
            0.209976, 0.110549, 0.0916737, 0.0807503, 0.091753, 0.103232,
            0.066022, 0.017691, 0.0693532, 0.0930067, 0.0853508, 0.0117135}) {
        EXPECT_AP(expected, bm25_score(invidx, *expr, *postings, i));
        i++;
      }
    }
  }
}
