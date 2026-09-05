#include <gtest/gtest.h>
#include <searchlib.h>

#include <cctype>

#include "test_utils.h"

using namespace searchlib;

namespace {

TermFilter lowercase_filter() {
  return to_term_filter(
      [](const std::u32string &s) { return unicode::to_lowercase(s); });
}

TermFilter stop_word_filter(std::unordered_set<std::u32string> words) {
  return [words = std::move(words)](const std::u32string &s,
                                    std::function<void(std::u32string)> emit) {
    if (words.find(s) == words.end()) {
      emit(s);
    }
  };
}

// Lowercases the query the same way the index chain does, so index/query
// term boundaries agree (parse_query still only takes a Normalizer here).
auto lc_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

std::vector<size_t> search_ids(const IInvertedIndex &index,
                               const std::string &query, Normalizer nz) {
  auto expr = parse_query(nz, query);
  std::vector<size_t> ids;
  if (!expr) return ids;
  auto postings = perform_search(index, *expr);
  for (size_t i = 0; i < postings->size(); i++) {
    ids.push_back(postings->document_ordinal(i));
  }
  return ids;
}

std::vector<size_t> search_ids(const IInvertedIndex &index,
                               const std::string &query, TermFilter filter) {
  auto expr = parse_query(filter, query);
  std::vector<size_t> ids;
  if (!expr) return ids;
  auto postings = perform_search(index, *expr);
  for (size_t i = 0; i < postings->size(); i++) {
    ids.push_back(postings->document_ordinal(i));
  }
  return ids;
}

// Stands in for a word-level morphological analyzer over a space-written
// language: it cuts the one compound these tests use, drops one word, and
// leaves everything else whole.
std::vector<std::string> demo_decompose(std::string_view term) {
  // A decomposer sees the surface bytes, before any normalizer, so it
  // matches case-insensitively and answers with the term's own bytes -- the
  // only thing a range can point at.
  std::string lower(term);
  for (auto &c : lower) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  if (lower == "helloworld") {
    return {std::string(term.substr(0, 5)), std::string(term.substr(5))};
  }
  if (lower == "dropme") {
    return {};
  }
  return {std::string(term)};
}

} // namespace

TEST(AnalyzerTest, QueryTermFilterMirrorsIndexChain) {
  // Same chain drives both index and query side, so a query for a stop word
  // matches nothing while a content word still matches regardless of case.
  auto chain = compose({lowercase_filter(), stop_word_filter({U"of", U"the"})});

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("apple of the tree"),
                             chain});

  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, "APPLE", chain));
  EXPECT_TRUE(search_ids(index, "the", chain).empty());

  // The same gap-closing trade-off from the index side (design 4.2) shows up
  // query-side too, since the query chain closes the gap the same way: a
  // phrase spanning a dropped stop word still matches.
  EXPECT_EQ((std::vector<size_t>{0}),
            search_ids(index, "\"apple tree\"", chain));
}

TEST(AnalyzerTest, QueryTermFilterOneToManyMapsToOr) {
  // Unlike the index-side Analyzer<T>, a 1->N filter is legal query-side and
  // maps to Or, not an implicit Adjacent phrase (design section 6).
  TermFilter synonyms = [](const std::u32string &s,
                           std::function<void(std::u32string)> emit) {
    emit(s);
    if (s == U"usa") emit(U"america");
  };

  auto expr = parse_query(synonyms, "usa");
  ASSERT_TRUE(expr.has_value());
  EXPECT_EQ(Operation::Or, expr->operation);
  ASSERT_EQ(2u, expr->nodes.size());
  EXPECT_EQ(U"usa", expr->nodes[0].term_str);
  EXPECT_EQ(U"america", expr->nodes[1].term_str);

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("america"), compose({})});
  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, "usa", synonyms));
}

TEST(AnalyzerTest, QueryTermFilterRawSplitStaysAdjacent) {
  // A token the raw tokenizer itself splits (e.g. `well-known`) is still an
  // implicit Adjacent phrase, distinct from a filter's 1->N expansion.
  auto expr = parse_query(TermFilter(nullptr), "well-known");
  ASSERT_TRUE(expr.has_value());
  EXPECT_EQ(Operation::Adjacent, expr->operation);
  ASSERT_EQ(2u, expr->nodes.size());
  EXPECT_EQ(U"well", expr->nodes[0].term_str);
  EXPECT_EQ(U"known", expr->nodes[1].term_str);
}

TEST(AnalyzerTest, LowercaseAndStopWordChain) {
  auto chain = compose({lowercase_filter(),
                        stop_word_filter({U"the", U"of", U"a", U"is"})});

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr); // normalization lives in the chain
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("The Quick Brown Fox"),
                             chain});
  indexer.index_document(
      1, Analyzer<TextRange>{UTF8PlainTextTokenizer("A lazy dog"), chain});

  // Content words are found regardless of the original casing.
  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, "quick", lc_normalizer));
  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, "QUICK", lc_normalizer));
  EXPECT_EQ((std::vector<size_t>{1}), search_ids(index, "dog", lc_normalizer));

  // Stop words were dropped at index time, so they match nothing.
  EXPECT_TRUE(search_ids(index, "the", lc_normalizer).empty());
  EXPECT_FALSE(index.term_exists(U"the"));
  EXPECT_FALSE(index.term_exists(U"a"));

  // Dropped tokens are not counted toward document length (design 4.3).
  EXPECT_EQ(3u, index.document_term_count(*index.document_ordinal(0))); // quick brown fox
  EXPECT_EQ(2u, index.document_term_count(*index.document_ordinal(1))); // lazy dog
}

TEST(AnalyzerTest, DropsCloseThePositionGap) {
  auto chain = compose({lowercase_filter(), stop_word_filter({U"of", U"the"})});

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("apple of the tree"),
                             chain});

  // Surviving tokens are renumbered 0,1 (the gap is closed): apple@0 tree@1.
  auto apple = index.postings(U"apple");
  auto tree = index.postings(U"tree");
  ASSERT_EQ(1u, apple->size());
  ASSERT_EQ(1u, tree->size());
  EXPECT_EQ(0u, apple->term_position(0, 0));
  EXPECT_EQ(1u, tree->term_position(0, 0));

  // Consequence of closing the gap: the phrase "apple tree" false-matches
  // across the removed stop words (documented known trade-off, design 4.2).
  EXPECT_EQ((std::vector<size_t>{0}),
            search_ids(index, "\"apple tree\"", lc_normalizer));

  // text_range still points into the original text via the carried-through
  // offsets, unaffected by the dropped tokens preceding "tree".
  auto range = index.text_range(*tree, 0, 0);
  EXPECT_EQ(std::string("tree"),
            std::string("apple of the tree")
                .substr(range.position, range.length));
}

TEST(AnalyzerTest, IndexSideOneToManyStacksOnOnePosition) {
  // A filter that emits twice for one token (index-time synonym expansion):
  // the outputs are alternatives at that token's position -- Lucene's
  // positionIncrement == 0 -- and the position's range is the token's. A
  // *sequence* of pieces is what subword_splitter is for (see
  // SubwordSplitterTest).
  TermFilter duplicating = [](const std::u32string &s,
                              std::function<void(std::u32string)> emit) {
    emit(s);
    emit(s + U"2");
  };

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("hello world"),
                             duplicating});

  auto hello2 = index.postings(U"hello2");
  auto world2 = index.postings(U"world2");
  ASSERT_EQ(1u, hello2->size());
  ASSERT_EQ(1u, world2->size());
  EXPECT_EQ(0u, hello2->term_position(0, 0));
  EXPECT_EQ(1u, world2->term_position(0, 0));
  // The document is as long as its positions, not its alternatives.
  EXPECT_EQ(2u, index.document_term_count(0));

  // Either alternative takes part in a phrase, and the two at one position
  // are never adjacent to each other.
  EXPECT_EQ((std::vector<size_t>{0}),
            search_ids(index, "\"hello2 world\"", lc_normalizer));
  EXPECT_EQ((std::vector<size_t>{}),
            search_ids(index, "\"hello hello2\"", lc_normalizer));

  auto range = index.text_range(*hello2, 0, 0);
  EXPECT_EQ("hello",
            std::string("hello world").substr(range.position, range.length));
}

TEST(AnalyzerTest, TheSameTermStackedTwiceIsOneOccurrence) {
  // A filter that answers the same string twice would otherwise count the
  // term twice in one position, inflating tf.
  TermFilter twice = [](const std::u32string &s,
                        std::function<void(std::u32string)> emit) {
    emit(s);
    emit(s);
  };

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("hello"), twice});

  auto hello = index.postings(U"hello");
  ASSERT_EQ(1u, hello->size());
  EXPECT_EQ(1u, hello->search_hit_count(0));
  EXPECT_EQ(1u, index.term_count(U"hello"));
}

TEST(AnalyzerTest, RawTokenizerMayStackButNotSkipOrGoBack) {
  // The same invariant one level down. Analyzer<T> is not the only way into
  // the indexer, and a hand-written Tokenizer<T> does not go through it: it
  // may repeat the position it just emitted (a stack, which brings no range
  // of its own), but a position skipped or revisited would hand a term a
  // neighbour's range, because the range vector is appended to once per
  // position and read back by term position.
  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);

  Tokenizer<TextRange> stacking = [](Normalizer, auto callback) {
    callback(U"seoul", 0, TextRange{0, 5});
    callback(U"seo", 0, TextRange{0, 3});
  };
  EXPECT_NO_THROW(indexer.index_document(0, stacking));
  auto seo = index.postings(U"seo");
  ASSERT_EQ(1u, seo->size());
  EXPECT_EQ(0u, seo->term_position(0, 0));
  EXPECT_EQ(5u, index.text_range(*seo, 0, 0).length); // seoul's, the first

  Tokenizer<TextRange> skipping = [](Normalizer, auto callback) {
    callback(U"seoul", 0, TextRange{0, 5});
    callback(U"tower", 2, TextRange{6, 5});
  };
  EXPECT_THROW(indexer.index_document(1, skipping), std::runtime_error);

  Tokenizer<TextRange> revisiting = [](Normalizer, auto callback) {
    callback(U"seoul", 0, TextRange{0, 5});
    callback(U"tower", 1, TextRange{6, 5});
    callback(U"seo", 0, TextRange{0, 3});
  };
  EXPECT_THROW(indexer.index_document(2, revisiting), std::runtime_error);

  Tokenizer<TextRange> dense = [](Normalizer, auto callback) {
    callback(U"seoul", 0, TextRange{0, 5});
    callback(U"tower", 1, TextRange{6, 5});
  };
  EXPECT_NO_THROW(indexer.index_document(3, dense));
}

TEST(AnalyzerTest, NormalizerAppliesAsStageZero) {
  // Identity chain, but the indexer carries a lowercasing normalizer. It must
  // be applied (as stage 0) rather than silently ignored.
  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, lc_normalizer);
  indexer.index_document(
      0, Analyzer<TextRange>{UTF8PlainTextTokenizer("HELLO World"),
                             compose({})});

  EXPECT_TRUE(index.term_exists(U"hello"));
  EXPECT_TRUE(index.term_exists(U"world"));
  EXPECT_FALSE(index.term_exists(U"HELLO"));
}

TEST(AnalyzerTest, ComposeIsLeftToRightAndEmptyIsIdentity) {
  std::vector<std::u32string> out;

  // Empty compose is the identity (emits input once).
  compose({})(U"Foo", [&](std::u32string s) { out.push_back(s); });
  EXPECT_EQ((std::vector<std::u32string>{U"Foo"}), out);

  // Order matters: lowercase THEN drop "foo" removes it; the reverse keeps it
  // because "Foo" != "foo" when the stop-word stage runs first.
  auto drop_foo = stop_word_filter({U"foo"});
  out.clear();
  compose({lowercase_filter(), drop_foo})(U"Foo",
                                          [&](std::u32string s) { out.push_back(s); });
  EXPECT_TRUE(out.empty());

  out.clear();
  compose({drop_foo, lowercase_filter()})(U"Foo",
                                          [&](std::u32string s) { out.push_back(s); });
  EXPECT_EQ((std::vector<std::u32string>{U"foo"}), out);
}

TEST(SubwordSplitterTest, PiecesGetConsecutivePositionsAndExactRanges) {
  auto splitter = subword_splitter(nullptr, demo_decompose);

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, lc_normalizer);
  std::string text = "Say HelloWorld dropme now";
  indexer.index_document(0, SplitterTokenizer(splitter, text));

  // say@0 hello@1 world@2 now@3: the compound became two terms at the
  // positions one term would have taken plus one, and the dropped word
  // closed its gap, as a dropped stop word does.
  EXPECT_EQ(4u, index.document_term_count(*index.document_ordinal(0)));
  EXPECT_FALSE(index.term_exists(U"helloworld"));
  EXPECT_FALSE(index.term_exists(U"dropme"));
  auto hello = index.postings(U"hello");
  auto world = index.postings(U"world");
  auto now = index.postings(U"now");
  ASSERT_EQ(1u, hello->size());
  ASSERT_EQ(1u, world->size());
  EXPECT_EQ(1u, hello->term_position(0, 0));
  EXPECT_EQ(2u, world->term_position(0, 0));
  EXPECT_EQ(3u, now->term_position(0, 0));

  // Each piece highlights its own bytes -- the reason this is a splitter and
  // not a filter, which would have had to give both pieces the word's range.
  auto range = index.text_range(*hello, 0, 0);
  EXPECT_EQ("Hello", text.substr(range.position, range.length));
  range = index.text_range(*world, 0, 0);
  EXPECT_EQ("World", text.substr(range.position, range.length));

  // The same splitter on the query side turns the compound into an implicit
  // phrase over its pieces, which is exactly where they sit.
  auto query = [&](const char *q) {
    return parse_query(splitter, to_term_filter(lc_normalizer), q);
  };
  auto expr = query("HelloWorld");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Adjacent, expr->operation);
  EXPECT_EQ(1u, perform_search(index, *expr)->size());
  EXPECT_EQ(1u, perform_search(index, *query("world"))->size());
  EXPECT_EQ(1u, perform_search(index, *query("\"say helloworld now\""))->size());
  EXPECT_EQ(0u, perform_search(index, *query("dropme"))->size());

  // The ranges survive both on-disk formats.
  for (auto format : {IndexFormat::Plain, IndexFormat::Compressed}) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    index.save(ss, {}, format);
    InMemoryInvertedIndex<TextRange> loaded;
    loaded.load(ss);
    auto loaded_world = loaded.postings(U"world");
    auto r = loaded.text_range(*loaded_world, 0, 0);
    EXPECT_EQ("World", text.substr(r.position, r.length));
  }
}

TEST(SubwordSplitterTest, RejectsAPieceOutsideItsTerm) {
  // A piece the term does not contain (from the previous piece on) has no
  // range to point at, and guessing one would corrupt highlighting.
  auto bad = subword_splitter(
      nullptr, [](std::string_view) -> std::vector<std::string> {
        return {"xyz"};
      });
  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  EXPECT_THROW(indexer.index_document(0, SplitterTokenizer(bad, "hello")),
               std::invalid_argument);

  auto reordered = subword_splitter(
      nullptr, [](std::string_view) -> std::vector<std::string> {
        return {"world", "hello"};
      });
  EXPECT_THROW(
      indexer.index_document(1, SplitterTokenizer(reordered, "helloworld")),
      std::invalid_argument);

  EXPECT_THROW(subword_splitter(nullptr, nullptr), std::invalid_argument);
}
