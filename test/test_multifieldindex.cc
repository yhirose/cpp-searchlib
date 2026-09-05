#include <gtest/gtest.h>
#include <searchlib.h>

#include <sstream>

#include "test_utils.h"

using namespace searchlib;

namespace {

auto mf_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

} // namespace

TEST(MultiFieldIndexTest, FieldQualifiedSearch) {
  MultiFieldIndex<TextRange> index;

  {
    InMemoryIndexer indexer(index.field("title"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("The Great Gatsby"));
    indexer.index_document(1, UTF8PlainTextTokenizer("Moby Dick"));
  }
  {
    InMemoryIndexer indexer(index.field("body"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("A story about wealth."));
    indexer.index_document(1, UTF8PlainTextTokenizer("A story about a whale."));
  }

  EXPECT_TRUE(index.has_field("title"));
  EXPECT_TRUE(index.has_field("body"));
  EXPECT_FALSE(index.has_field("tags"));
  EXPECT_EQ((std::vector<std::string>{"body", "title"}), index.field_names());

  auto expr = parse_query(mf_normalizer, "gatsby");
  ASSERT_TRUE(expr);

  auto title_hits = perform_search(index.field("title"), *expr);
  ASSERT_EQ(1, title_hits->size());
  EXPECT_EQ(0, title_hits->document_ordinal(0));

  auto body_hits = perform_search(index.field("body"), *expr);
  EXPECT_EQ(0, body_hits->size()); // "gatsby" never appears in body
}

TEST(MultiFieldIndexTest, CombinedSearchGroupsHitsByDocumentId) {
  MultiFieldIndex<TextRange> index;

  {
    InMemoryIndexer indexer(index.field("title"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Whale Stories"));
    indexer.index_document(1, UTF8PlainTextTokenizer("Desert Tales"));
  }
  {
    InMemoryIndexer indexer(index.field("body"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Long ago, a whale swam."));
    indexer.index_document(
        1, UTF8PlainTextTokenizer("A whale washed up once, long ago."));
  }

  auto expr = parse_query(mf_normalizer, "whale");
  ASSERT_TRUE(expr);

  auto hits = perform_multi_field_search(index, *expr);
  // document 0 matches in both title and body; document 1 matches only body.
  ASSERT_EQ(3, hits.size());

  // Grouped by key: each field is its own index with its own ordinals, and
  // document 1 (body only) would not even share an ordinal across fields.
  std::map<size_t, int> match_count_by_document;
  for (const auto &hit : hits) {
    match_count_by_document[hit.document_key]++;
  }
  EXPECT_EQ(2, match_count_by_document[0]);
  EXPECT_EQ(1, match_count_by_document[1]);
}

TEST(MultiFieldIndexTest, SaveLoadRoundTrip) {
  MultiFieldIndex<TextRange> index;
  {
    InMemoryIndexer indexer(index.field("title"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Hello World"));
  }
  {
    InMemoryIndexer indexer(index.field("body"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Hello there, world."));
  }

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  index.save(ss);

  MultiFieldIndex<TextRange> loaded;
  loaded.load(ss);

  EXPECT_EQ((std::vector<std::string>{"body", "title"}), loaded.field_names());

  auto expr = parse_query(mf_normalizer, "hello");
  ASSERT_TRUE(expr);

  auto title_hits = perform_search(loaded.field("title"), *expr);
  EXPECT_EQ(1, title_hits->size());

  auto body_hits = perform_search(loaded.field("body"), *expr);
  EXPECT_EQ(1, body_hits->size());
}
