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
    EXPECT_AP(11.925, invidx.idf(term));

    EXPECT_AP(0.034, invidx.tf(term, postings->document_id(0)));
    EXPECT_AP(0.411, invidx.tf_idf(term, postings->document_id(0)));

    EXPECT_AP(0.063, invidx.tf(term, postings->document_id(1)));
    EXPECT_AP(0.745, invidx.tf_idf(term, postings->document_id(1)));

    EXPECT_AP(0.071, invidx.tf(term, postings->document_id(2)));
    EXPECT_AP(0.852, invidx.tf_idf(term, postings->document_id(2)));

    EXPECT_AP(0.030, invidx.tf(term, postings->document_id(3)));
    EXPECT_AP(0.351, invidx.tf_idf(term, postings->document_id(3)));

    EXPECT_AP(0.029, invidx.tf(term, postings->document_id(4)));
    EXPECT_AP(0.341, invidx.tf_idf(term, postings->document_id(4)));

    EXPECT_AP(0.029, invidx.tf(term, postings->document_id(5)));
    EXPECT_AP(0.341, invidx.tf_idf(term, postings->document_id(5)));

    EXPECT_AP(0.025, invidx.tf(term, postings->document_id(6)));
    EXPECT_AP(0.298, invidx.tf_idf(term, postings->document_id(6)));

    EXPECT_AP(0.032, invidx.tf(term, postings->document_id(7)));
    EXPECT_AP(0.385, invidx.tf_idf(term, postings->document_id(7)));
  }

  {
    auto expr = parse_query(invidx, R"( "apple tree" )");
    auto postings = perform_search(invidx, *expr);
    EXPECT_EQ(3, postings->size());
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

