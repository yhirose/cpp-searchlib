#include <gtest/gtest.h>
#include <searchlib.h>

#include <sstream>

#include "test_utils.h"

using namespace searchlib;

namespace {

auto mf_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

} // namespace

TEST(DocValuesTest, SetGetRemove) {
  DocValues<uint64_t> values;
  EXPECT_FALSE(values.has(0));
  EXPECT_EQ(std::nullopt, values.get(0));

  values.set(0, 42);
  values.set(1, 7);
  ASSERT_TRUE(values.has(0));
  EXPECT_EQ(42, *values.get(0));
  EXPECT_EQ(7, *values.get(1));
  EXPECT_EQ(2, values.size());

  values.remove(0);
  EXPECT_FALSE(values.has(0));
  EXPECT_EQ(1, values.size());
}

TEST(DocValuesTest, SaveLoadRoundTrip) {
  DocValues<double> values;
  values.set(0, 3.5);
  values.set(5, -1.25);
  values.set(2, 0.0);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  values.save(ss);

  DocValues<double> loaded;
  loaded.load(ss);

  EXPECT_EQ(3, loaded.size());
  EXPECT_EQ(3.5, *loaded.get(0));
  EXPECT_EQ(-1.25, *loaded.get(5));
  EXPECT_EQ(0.0, *loaded.get(2));
  EXPECT_FALSE(loaded.has(1));
}

TEST(StoredFieldsTest, SetGetRemove) {
  StoredFields fields;
  EXPECT_FALSE(fields.has(0));
  EXPECT_EQ(nullptr, fields.get(0));

  fields.set(0, "hello");
  fields.set(1, "world");
  ASSERT_TRUE(fields.has(0));
  ASSERT_NE(nullptr, fields.get(0));
  EXPECT_EQ("hello", *fields.get(0));
  EXPECT_EQ("world", *fields.get(1));
  EXPECT_EQ(2, fields.size());

  fields.remove(0);
  EXPECT_FALSE(fields.has(0));
  EXPECT_EQ(1, fields.size());
}

TEST(StoredFieldsTest, SaveLoadRoundTripIncludingEmptyValue) {
  StoredFields fields;
  fields.set(0, "The quick brown fox");
  fields.set(3, "");
  fields.set(7, std::string("with\0embedded", 13));

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  fields.save(ss);

  StoredFields loaded;
  loaded.load(ss);

  EXPECT_EQ(3, loaded.size());
  EXPECT_EQ("The quick brown fox", *loaded.get(0));
  EXPECT_EQ("", *loaded.get(3));
  EXPECT_EQ(std::string("with\0embedded", 13), *loaded.get(7));
}

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
  EXPECT_EQ(0, title_hits->document_id(0));

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

  std::map<size_t, int> match_count_by_document;
  for (const auto &hit : hits) {
    auto document_id = hit.postings->document_id(hit.index_in_postings);
    match_count_by_document[document_id]++;
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
