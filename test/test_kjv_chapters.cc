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

    EXPECT_AP(0.00552477, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0232005, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.017513, tf_idf_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.0205566, tf_idf_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.0200426, tf_idf_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.00817141, tf_idf_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.0142035, tf_idf_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.0227614, tf_idf_score(invidx, *expr, *postings, 7));

    EXPECT_AP(0.00579213, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0694968, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0441665, bm25_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.0573186, bm25_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.0550286, bm25_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.0118435, bm25_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.0311316, bm25_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.0674305, bm25_score(invidx, *expr, *postings, 7));
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

    EXPECT_AP(0.039478, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0292668, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0468876, tf_idf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.10807, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.0794798, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.0997504, bm25_score(invidx, *expr, *postings, 2));
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
           {0.00991584, 0.0151034, 0.0448072, 0.0371685, 0.0582594, 0.0867673,
            0.102847, 0.0784337, 0.0660113, 0.073059, 0.0588133, 0.104698,
            0.0292326, 0.0115335, 0.0429949, 0.0308933, 0.0676644, 0.00919336}) {
        EXPECT_AP(expected, tf_idf_score(invidx, *expr, *postings, i));
        i++;
      }
    }

    {
      size_t i = 0;
      for (auto expected :
           {0.00951014, 0.0283386, 0.131482, 0.0599203, 0.117288, 0.148009,
            0.210045, 0.110728, 0.0912804, 0.0803526, 0.0913933, 0.102833,
            0.0658579, 0.0176168, 0.0690821, 0.0929295, 0.0849572, 0.0116986}) {
        EXPECT_AP(expected, bm25_score(invidx, *expr, *postings, i));
        i++;
      }
    }
  }
}
