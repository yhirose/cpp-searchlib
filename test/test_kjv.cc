#include <gtest/gtest.h>
#include <searchlib.h>

#include <filesystem>
#include <fstream>

#include "test_utils.h"

const auto KJV_PATH = "../../test/t_kjv.tsv";

void kjv_index(searchlib::InvertedIndex &invidx,
               searchlib::TextRangeList &text_range_list) {
  searchlib::Indexer::set_normalizer(
      invidx, [](auto sv) { return unicode::to_lowercase(sv); });

  std::ifstream fs(KJV_PATH);
  std::string line;
  while (std::getline(fs, line)) {
    auto fields = split(line, '\t');
    auto document_id = std::stoi(fields[0]);
    const auto &s = fields[4];

    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(s, &text_ranges);

    searchlib::Indexer::indexing(invidx, document_id, tokenizer);

    text_range_list.emplace(document_id, std::move(text_ranges));
  }
}

TEST(KJVTest, SimpleTest) {
  searchlib::InvertedIndex invidx;
  searchlib::TextRangeList text_range_list;

  kjv_index(invidx, text_range_list);

  {
    auto expr = parse_query(invidx, R"( apple )");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(8, postings->size());

    auto term = U"apple";
    EXPECT_EQ(8, invidx.df(term));

    EXPECT_AP(0.411, searchlib::tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.745, searchlib::tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.852, searchlib::tf_idf_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.351, searchlib::tf_idf_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.341, searchlib::tf_idf_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.341, searchlib::tf_idf_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.298, searchlib::tf_idf_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.385, searchlib::tf_idf_score(invidx, *expr, *postings, 7));
  }

  {
    auto expr = parse_query(invidx, R"( "apple tree" )");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(3, postings->size());

    // for (size_t i = 0; i < postings->size(); i++) {
    //   auto score = bm25_score(invidx, *expr, *postings, i);
    //   std::cout << i << ": score: " << score << std::endl;
    // }

    EXPECT_EQ(2, searchlib::term_count_score(invidx, *expr, *postings, 0));
    EXPECT_EQ(2, searchlib::term_count_score(invidx, *expr, *postings, 1));
    EXPECT_EQ(5, searchlib::term_count_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.059, searchlib::tf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.057, searchlib::tf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.125, searchlib::tf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.572, searchlib::tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.556, searchlib::tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.051, searchlib::tf_idf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.817, searchlib::bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.776, searchlib::bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.285, searchlib::bm25_score(invidx, *expr, *postings, 2));
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
    searchlib::UTF8PlainTextTokenizer tokenizer(s);
    tokenizer.tokenize(normalizer, [&](auto &str, auto) {});
  }
}

