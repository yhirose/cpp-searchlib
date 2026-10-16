#include <gtest/gtest.h>
#include <searchlib.h>

#include <set>
#include <sstream>

#include "test_utils.h"

using namespace searchlib;

namespace {

auto mf_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

// A key that is neither an integer nor a string: it needs serializers of its
// own, and the fields of a MultiFieldIndex have to be able to be handed them.
struct ShardKey {
  int shard;
  int id;
  bool operator==(const ShardKey &other) const {
    return shard == other.shard && id == other.id;
  }
};

} // namespace

namespace std {
template <> struct hash<ShardKey> {
  size_t operator()(const ShardKey &key) const {
    return hash<int>()(key.shard) * 31 + hash<int>()(key.id);
  }
};
} // namespace std

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

TEST(MultiFieldIndexTest, SaveLoadWithACustomKeyType) {
  using Index = MultiFieldIndex<TextRange, ShardKey>;
  Index index;
  {
    InMemoryIndexer indexer(index.field("title"), mf_normalizer);
    indexer.index_document({1, 10}, UTF8PlainTextTokenizer("Hello World"));
    indexer.index_document({2, 20}, UTF8PlainTextTokenizer("Goodbye"));
  }
  {
    InMemoryIndexer indexer(index.field("body"), mf_normalizer);
    indexer.index_document({2, 20}, UTF8PlainTextTokenizer("Hello there"));
  }

  InMemoryInvertedIndex<TextRange, ShardKey>::KeySerializer save_key =
      [](std::ostream &os, const ShardKey &key) {
        detail::write_scalar<int32_t>(os, key.shard);
        detail::write_scalar<int32_t>(os, key.id);
      };
  InMemoryInvertedIndex<TextRange, ShardKey>::KeyDeserializer load_key =
      [](std::istream &is) {
        auto shard = detail::read_scalar<int32_t>(is);
        auto id = detail::read_scalar<int32_t>(is);
        return ShardKey{shard, id};
      };

  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    index.save(ss, {}, format, save_key);

    Index loaded;
    loaded.load(ss, {}, load_key);
    EXPECT_EQ((std::vector<std::string>{"body", "title"}),
              loaded.field_names());

    auto hits = perform_multi_field_search(
        loaded, *parse_query(mf_normalizer, "hello"));
    ASSERT_EQ(2, hits.size());
    std::set<std::pair<int, int>> keys;
    for (const auto &hit : hits) {
      keys.emplace(hit.document_key.shard, hit.document_key.id);
    }
    EXPECT_EQ((std::set<std::pair<int, int>>{{1, 10}, {2, 20}}), keys);
  }
}

// A stream that fails part-way must not leave a half-loaded index: the
// fields already read are not kept, and the ones it had are not lost.
TEST(MultiFieldIndexTest, FailedLoadLeavesTheIndexAsItWas) {
  MultiFieldIndex<TextRange> source;
  {
    InMemoryIndexer indexer(source.field("body"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Hello there"));
    InMemoryIndexer title(source.field("title"), mf_normalizer);
    title.index_document(0, UTF8PlainTextTokenizer("Hello World"));
  }
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  source.save(ss);
  auto bytes = ss.str();
  // Cut inside the second field: the first loads, the second runs out.
  bytes.resize(bytes.size() - 8);

  MultiFieldIndex<TextRange> index;
  {
    InMemoryIndexer indexer(index.field("keep"), mf_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("Hello kept"));
  }

  std::stringstream in(bytes, std::ios::in | std::ios::binary);
  EXPECT_THROW(index.load(in), std::runtime_error);

  EXPECT_EQ((std::vector<std::string>{"keep"}), index.field_names());
  auto hits =
      perform_search(index.field("keep"), *parse_query(mf_normalizer, "kept"));
  EXPECT_EQ(1, hits->size());
}
