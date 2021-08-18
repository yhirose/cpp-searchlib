#include <gtest/gtest.h>
#include <searchlib.h>

#include <fstream>

#include "test_utils.h"

#ifdef _MSC_BUILD
const auto KJV_PATH = "../../test/t_kjv.tsv";
#else
const auto KJV_PATH = "../test/t_kjv.tsv";
#endif

void kjv_index(searchlib::InvertedIndex &index,
               searchlib::TextRangeList &text_range_list) {
  index.normalizer = [](auto sv) { return unicode::to_lowercase(sv); };
  std::ifstream fs(KJV_PATH);
  std::string line;
  while (std::getline(fs, line)) {
    auto fields = split(line, '\t');
    auto document_id = std::stoi(fields[0]);
    const auto &s = fields[4];

    std::vector<searchlib::TextRange> text_ranges;
    searchlib::UTF8PlainTextTokenizer tokenizer(s, index.normalizer,
                                                text_ranges);

    searchlib::indexing(index, tokenizer, document_id);

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
    std::vector<searchlib::TextRange> text_ranges;

    searchlib::UTF8PlainTextTokenizer tokenizer(s, normalizer, text_ranges);

    tokenizer.tokenize([&](auto &str, auto) {});
  }
}

