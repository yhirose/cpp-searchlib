#include <gtest/gtest.h>
#include <searchlib.h>

#include <filesystem>
#include <fstream>

#include "test_utils.h"

const auto KJV_PATH = "../../test/t_kjv.tsv";

void kjv_index(searchlib::InvertedIndex &index,
               searchlib::TextRangeList &text_range_list) {
  searchlib::Indexer::set_normalizer(
      index, [](auto sv) { return unicode::to_lowercase(sv); });

  std::ifstream fs(KJV_PATH);
  std::string line;
  while (std::getline(fs, line)) {
    auto fields = split(line, '\t');
    auto document_id = std::stoi(fields[0]);
    const auto &s = fields[4];

    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(s, &text_ranges);

    searchlib::Indexer::indexing(index, document_id, tokenizer);

    text_range_list.emplace(document_id, std::move(text_ranges));
  }
}

TEST(KJVTest, SimpleTest) {
  searchlib::InvertedIndex index;
  searchlib::TextRangeList text_range_list;

  kjv_index(index, text_range_list);

  {
    auto expr = parse_query(index, R"( apple )");
    auto result = perform_search(index, *expr);

    EXPECT_EQ(8, result->size());

    auto term = U"apple";

    EXPECT_EQ(8, index.df(term));
    EXPECT_AP(11.925, index.idf(term));

    EXPECT_AP(0.034, index.tf(term, result->document_id(0)));
    EXPECT_AP(0.411, index.tf_idf(term, result->document_id(0)));

    EXPECT_AP(0.063, index.tf(term, result->document_id(1)));
    EXPECT_AP(0.745, index.tf_idf(term, result->document_id(1)));

    EXPECT_AP(0.071, index.tf(term, result->document_id(2)));
    EXPECT_AP(0.852, index.tf_idf(term, result->document_id(2)));

    EXPECT_AP(0.030, index.tf(term, result->document_id(3)));
    EXPECT_AP(0.351, index.tf_idf(term, result->document_id(3)));

    EXPECT_AP(0.029, index.tf(term, result->document_id(4)));
    EXPECT_AP(0.341, index.tf_idf(term, result->document_id(4)));

    EXPECT_AP(0.029, index.tf(term, result->document_id(5)));
    EXPECT_AP(0.341, index.tf_idf(term, result->document_id(5)));

    EXPECT_AP(0.025, index.tf(term, result->document_id(6)));
    EXPECT_AP(0.298, index.tf_idf(term, result->document_id(6)));

    EXPECT_AP(0.032, index.tf(term, result->document_id(7)));
    EXPECT_AP(0.385, index.tf_idf(term, result->document_id(7)));
  }

  {
    auto expr = parse_query(index, R"( "apple tree" )");
    auto result = perform_search(index, *expr);
    EXPECT_EQ(3, result->size());
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

