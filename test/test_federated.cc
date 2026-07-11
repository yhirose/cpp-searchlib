#include <gtest/gtest.h>
#include <searchlib.h>

#include "test_utils.h"

using namespace searchlib;

namespace {

auto federated_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

auto make_index(const std::vector<std::string> &documents) {
  auto invidx = std::make_shared<InMemoryInvertedIndex<TextRange>>();
  InMemoryIndexer indexer(*invidx, federated_normalizer);
  size_t document_id = 0;
  for (const auto &doc : documents) {
    indexer.index_document(document_id, UTF8PlainTextTokenizer(doc));
    document_id++;
  }
  return invidx;
}

// Records remove_document calls without actually deleting anything; real
// logical deletion is out of scope for this test (see roadmap section 3).
class MockMutableInvertedIndex : public IMutableInvertedIndex {
public:
  void remove_document(size_t document_id) override {
    removed_document_ids.push_back(document_id);
  }

  std::vector<size_t> removed_document_ids;
};

} // namespace

TEST(FederatedSearchTest, SearchAcrossMembers) {
  auto book = make_index({"This is the first book.", "The second chapter."});
  auto notes = make_index({"A note about the book.", "Unrelated memo."});

  FederatedIndex federation;
  federation.add(book);
  federation.add(notes);

  auto expr = parse_query(federated_normalizer, "book");
  ASSERT_TRUE(expr);

  auto hits = perform_federated_search(federation, *expr);
  ASSERT_EQ(2, hits.size());

  EXPECT_EQ(book, hits[0].index);
  EXPECT_EQ(0, hits[0].postings->document_id(hits[0].index_in_postings));

  EXPECT_EQ(notes, hits[1].index);
  EXPECT_EQ(0, hits[1].postings->document_id(hits[1].index_in_postings));
}

TEST(FederatedSearchTest, RemoveMember) {
  auto book = make_index({"This is the first book."});
  auto notes = make_index({"A note about the book."});

  FederatedIndex federation;
  federation.add(book);
  federation.add(notes);
  federation.remove(book);

  auto expr = parse_query(federated_normalizer, "book");
  ASSERT_TRUE(expr);

  auto hits = perform_federated_search(federation, *expr);
  ASSERT_EQ(1, hits.size());
  EXPECT_EQ(notes, hits[0].index);
}

TEST(FederatedSearchTest, MutableMemberWiring) {
  auto book = make_index({"read-only book"});
  auto notes = make_index({"editable notes"});
  auto mock_mutable = std::make_shared<MockMutableInvertedIndex>();

  FederatedIndex federation;
  federation.add(book);              // read-only: no mutable_index
  federation.add(notes, mock_mutable); // mutable member

  auto members = federation.snapshot();
  ASSERT_EQ(2, members.size());

  EXPECT_EQ(book, members[0].index);
  EXPECT_EQ(nullptr, members[0].mutable_index);

  EXPECT_EQ(notes, members[1].index);
  ASSERT_EQ(mock_mutable, members[1].mutable_index);

  members[1].mutable_index->remove_document(0);
  EXPECT_EQ(std::vector<size_t>{0}, mock_mutable->removed_document_ids);
}
