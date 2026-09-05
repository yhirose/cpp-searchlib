#include <gtest/gtest.h>
#include <searchlib.h>

#include "test_utils.h"

using namespace searchlib;

namespace {

auto federated_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

auto make_index(const std::vector<std::string> &documents) {
  auto invidx = std::make_shared<InMemoryInvertedIndex<TextRange>>();
  InMemoryIndexer indexer(*invidx, federated_normalizer);
  size_t document_key = 0;
  for (const auto &doc : documents) {
    indexer.index_document(document_key, UTF8PlainTextTokenizer(doc));
    document_key++;
  }
  return invidx;
}

// Records remove_document calls without actually deleting anything; real
// logical deletion is out of scope for this test (see roadmap section 3).
class MockMutableInvertedIndex : public IMutableInvertedIndex {
public:
  void remove_document(size_t document_key) override {
    removed_document_ids.push_back(document_key);
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
  EXPECT_EQ(0, hits[0].postings->document_ordinal(hits[0].index_in_postings));

  EXPECT_EQ(notes, hits[1].index);
  EXPECT_EQ(0, hits[1].postings->document_ordinal(hits[1].index_in_postings));
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

TEST(FederatedSearchTest, RemoveDocumentExcludesFromFederatedSearch) {
  // Unlike MutableMemberWiring (which uses a mock that records but does not
  // delete), this exercises real logical deletion end-to-end: a document
  // removed through a member's IMutableInvertedIndex must disappear from
  // perform_federated_search via the tombstone -> FilteredPostings path.
  auto book = make_index({"This is the first book.", "Another book here."});
  auto notes = make_index({"A note about the book.", "Unrelated memo."});

  FederatedIndex federation;
  federation.add(book);            // read-only member
  federation.add(notes, notes);    // mutable member (real InMemoryInvertedIndex)

  auto expr = parse_query(federated_normalizer, "book");
  ASSERT_TRUE(expr);

  // book/0, book/1 and notes/0 all mention "book".
  ASSERT_EQ(3, perform_federated_search(federation, *expr).size());

  // Remove notes/0 through the member's mutable handle from the snapshot.
  auto members = federation.snapshot();
  ASSERT_EQ(notes, members[1].index);
  ASSERT_NE(nullptr, members[1].mutable_index);
  members[1].mutable_index->remove_document(0);

  auto hits = perform_federated_search(federation, *expr);
  ASSERT_EQ(2, hits.size());
  for (const auto &hit : hits) {
    EXPECT_EQ(book, hit.index); // only the book members survive
  }

  // Re-indexing the same id clears the tombstone and it returns.
  {
    InMemoryIndexer indexer(*notes, federated_normalizer);
    indexer.index_document(0, UTF8PlainTextTokenizer("A note about the book."));
  }
  EXPECT_EQ(3, perform_federated_search(federation, *expr).size());
}

TEST(FederatedSearchTest, MixedInMemoryAndCompressedMembers) {
  // An installed, read-only corpus served by the compressed backend,
  // searched together with a mutable in-memory notes index.
  auto book_source =
      make_index({"This is the first book.", "The second chapter."});
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  book_source->save(ss, {}, IndexFormat::Compressed);
  auto book = load_compressed_index(ss);

  auto notes = make_index({"A note about the book.", "Unrelated memo."});

  FederatedIndex federation;
  federation.add(book); // read-only: no mutable_index
  federation.add(notes, notes);

  auto expr = parse_query(federated_normalizer, "book");
  ASSERT_TRUE(expr);

  auto hits = perform_federated_search(federation, *expr);
  ASSERT_EQ(2, hits.size());

  EXPECT_EQ(book, hits[0].index);
  EXPECT_EQ(0, hits[0].postings->document_ordinal(hits[0].index_in_postings));

  EXPECT_EQ(notes, hits[1].index);
  EXPECT_EQ(0, hits[1].postings->document_ordinal(hits[1].index_in_postings));

  // Highlighting works through the compressed member's text_range.
  auto range = book->text_range(*hits[0].postings, hits[0].index_in_postings, 0);
  EXPECT_EQ(std::string("book"),
            std::string("This is the first book.").substr(range.position,
                                                          range.length));
}
