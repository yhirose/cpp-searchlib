#include <gtest/gtest.h>
#include <searchlib.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>

#include "test_utils.h"

using namespace searchlib;

const auto KJV_PATH = "../../test/t_kjv.tsv";

static auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

static auto kjv_index() {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);
  std::ifstream fs(KJV_PATH);
  if (fs) {
    std::string line;
    while (std::getline(fs, line)) {
      auto fields = split(line, '\t');
      auto document_key = std::stoi(fields[0]);
      const auto &s = fields[4];

      indexer.index_document(document_key, UTF8PlainTextTokenizer(s));
    }
  }
  return invidx;
}

TEST(KJVTest, SimpleTest) {
  const auto &invidx = kjv_index();

  {
    auto expr = parse_query(normalizer, R"( apple )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(8, postings->size());

    auto term = U"apple";
    EXPECT_EQ(8, invidx.df(term));

    EXPECT_AP(0.411, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.745, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(0.852, tf_idf_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.351, tf_idf_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.341, tf_idf_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.341, tf_idf_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.298, tf_idf_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.385, tf_idf_score(invidx, *expr, *postings, 7));

    EXPECT_AP(0.659, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(1.751, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(2.144, bm25_score(invidx, *expr, *postings, 2));
    EXPECT_AP(0.499, bm25_score(invidx, *expr, *postings, 3));
    EXPECT_AP(0.474, bm25_score(invidx, *expr, *postings, 4));
    EXPECT_AP(0.474, bm25_score(invidx, *expr, *postings, 5));
    EXPECT_AP(0.374, bm25_score(invidx, *expr, *postings, 6));
    EXPECT_AP(0.587, bm25_score(invidx, *expr, *postings, 7));
  }

  {
    auto expr = parse_query(normalizer, R"( "apple tree" )");
    ASSERT_TRUE(expr);

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings);
    ASSERT_EQ(3, postings->size());

    EXPECT_EQ(1, postings->search_hit_count(0));
    EXPECT_EQ(1, postings->search_hit_count(1));
    EXPECT_EQ(1, postings->search_hit_count(2));

    EXPECT_EQ(2, term_count_score(invidx, *expr, *postings, 0));
    EXPECT_EQ(2, term_count_score(invidx, *expr, *postings, 1));
    EXPECT_EQ(5, term_count_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.572, tf_idf_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.556, tf_idf_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.051, tf_idf_score(invidx, *expr, *postings, 2));

    EXPECT_AP(0.816, bm25_score(invidx, *expr, *postings, 0));
    EXPECT_AP(0.775, bm25_score(invidx, *expr, *postings, 1));
    EXPECT_AP(1.283, bm25_score(invidx, *expr, *postings, 2));
  }
}

TEST(KJVTest, CompressedPersistenceRoundTrip) {
  const auto &invidx = kjv_index();

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(plain);
  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto plain_size = plain.str().size();
  auto compressed_size = compressed.str().size();
  std::cout << "KJV index size: plain=" << plain_size
            << " compressed=" << compressed_size << " ("
            << (compressed_size * 100.0 / plain_size) << "%)" << std::endl;

  // A real bound rather than just "smaller than plain": the compressed
  // format measures 20.1% here (Elias-Fano postings and text ranges plus the
  // FST term dictionary), so 25% leaves room to move while still catching a
  // section that stopped being compressed.
  EXPECT_LT(compressed_size, static_cast<size_t>(plain_size * 0.25))
      << "the compressed format lost ground: " << compressed_size << " / "
      << plain_size << " = " << (compressed_size * 100.0 / plain_size) << "%";

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(compressed);

  EXPECT_EQ(invidx.document_count(), loaded.document_count());
  EXPECT_EQ(invidx.df(U"apple"), loaded.df(U"apple"));
  EXPECT_EQ(invidx.df(U"the"), loaded.df(U"the")); // high-df: EF-encoded

  auto expr = parse_query(normalizer, R"( apple )");
  auto expected = perform_search(invidx, *expr);
  auto actual = perform_search(loaded, *expr);
  ASSERT_EQ(expected->size(), actual->size());
  for (size_t i = 0; i < expected->size(); i++) {
    EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
    ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
    for (size_t h = 0; h < expected->search_hit_count(i); h++) {
      EXPECT_EQ(expected->term_position(i, h), actual->term_position(i, h));
    }
    EXPECT_AP(bm25_score(invidx, *expr, *expected, i),
              bm25_score(loaded, *expr, *actual, i));
  }
}

// BM25Scorer is an optimization of bm25_score, not a variant of it: it exists
// only to hoist the per-term lookups out of the per-hit loop. The two must
// therefore agree exactly -- not approximately -- on every hit of every query
// shape, including the ones whose terms come from a dictionary expansion.
TEST(KJVTest, BM25ScorerMatchesBM25Score) {
  const auto &invidx = kjv_index();

  const char *queries[] = {
      "apple",           // single term
      "zzzznotaterm",    // no hits at all
      "apple tree",      // And
      "apple | tree",    // Or
      R"("apple tree")", // Adjacent (phrase)
      "apple ~ tree",    // Near
      "tree -apple",     // Not
      "appl*",           // Prefix: terms come from a dictionary walk
      "ap*le",           // Wildcard
      "apple~1",         // Fuzzy
  };

  for (const auto *query : queries) {
    auto expr = parse_query(normalizer, query);
    ASSERT_TRUE(expr) << query;

    auto postings = perform_search(invidx, *expr);
    ASSERT_TRUE(postings) << query;

    // Ascending, the order top_k walks a result in, and the one the scorer's
    // per-term cursor is optimized for.
    {
      BM25Scorer scorer(invidx, *expr);
      for (size_t i = 0; i < postings->size(); i++) {
        EXPECT_DOUBLE_EQ(bm25_score(invidx, *expr, *postings, i),
                         scorer(*postings, i))
            << "query " << query << ", ascending, hit " << i;
      }
    }

    // Descending, which drives every lookup down the cursor's backwards
    // fallback. Galloping forward from a cursor already past the target
    // would silently score those hits as misses.
    {
      BM25Scorer scorer(invidx, *expr);
      for (size_t i = postings->size(); i > 0; i--) {
        EXPECT_DOUBLE_EQ(bm25_score(invidx, *expr, *postings, i - 1),
                         scorer(*postings, i - 1))
            << "query " << query << ", descending, hit " << (i - 1);
      }
    }

    // And an order that jumps around, alternating the two directions so the
    // cursor is left both ahead of and behind the next target.
    {
      BM25Scorer scorer(invidx, *expr);
      auto count = postings->size();
      for (size_t step = 0; step < count; step++) {
        auto i = (step % 2 == 0) ? step / 2 : count - 1 - step / 2;
        EXPECT_DOUBLE_EQ(bm25_score(invidx, *expr, *postings, i),
                         scorer(*postings, i))
            << "query " << query << ", zigzag, hit " << i;
      }
    }
  }
}

TEST(KJVTest, CompressedBackend) {
  const auto &invidx = kjv_index();

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto loaded = load_compressed_index(compressed);

  EXPECT_EQ(invidx.document_count(), loaded->document_count());
  EXPECT_DOUBLE_EQ(invidx.average_document_term_count(),
                   loaded->average_document_term_count());
  EXPECT_EQ(invidx.df(U"apple"), loaded->df(U"apple"));
  EXPECT_EQ(invidx.df(U"the"), loaded->df(U"the")); // high-df: EF-encoded
  EXPECT_EQ(invidx.term_count(U"the"), loaded->term_count(U"the"));
  EXPECT_TRUE(loaded->term_exists(U"apple"));
  EXPECT_FALSE(loaded->term_exists(U"zzzzz"));
  EXPECT_FALSE(loaded->has_removed_documents());

  // Both backends still answer prefix enumeration with a full dictionary
  // scan, so they must agree term for term.
  auto collect = [](const IInvertedIndex &index, const std::u32string &prefix) {
    std::vector<std::u32string> terms;
    index.enumerate_terms_with_prefix(
        prefix, [&](const auto &str) { terms.push_back(str); });
    std::sort(terms.begin(), terms.end());
    return terms;
  };
  auto expected_terms = collect(invidx, U"sanctif");
  ASSERT_FALSE(expected_terms.empty());
  EXPECT_EQ(expected_terms, collect(*loaded, U"sanctif"));

  // Same parity check for the wildcard path: the compressed backend walks
  // the FST with a custom automaton instead of scanning a hash map.
  auto collect_wildcard = [](const IInvertedIndex &index,
                             const std::u32string &pattern) {
    std::vector<std::u32string> terms;
    index.enumerate_terms_with_wildcard(
        pattern, [&](const auto &str) { terms.push_back(str); });
    std::sort(terms.begin(), terms.end());
    return terms;
  };
  auto expected_wildcard_terms = collect_wildcard(invidx, U"sanct*fy");
  ASSERT_FALSE(expected_wildcard_terms.empty());
  EXPECT_EQ(expected_wildcard_terms, collect_wildcard(*loaded, U"sanct*fy"));

  // And for the fuzzy path, where the compressed backend drives fstlib's
  // LevenshteinAutomaton over the FST and the in-memory one runs a DP per
  // term. A real vocabulary is what makes this worth checking: "lord" alone
  // has dozens of neighbours at distance 1 in KJV.
  auto collect_fuzzy = [](const IInvertedIndex &index,
                          const std::u32string &str, size_t max_edits) {
    std::vector<std::u32string> terms;
    index.enumerate_terms_with_edit_distance(
        str, max_edits, [&](const auto &s) { terms.push_back(s); });
    std::sort(terms.begin(), terms.end());
    return terms;
  };
  for (size_t max_edits : {size_t{1}, size_t{2}}) {
    auto expected_fuzzy_terms = collect_fuzzy(invidx, U"lord", max_edits);
    ASSERT_FALSE(expected_fuzzy_terms.empty()) << max_edits;
    EXPECT_EQ(expected_fuzzy_terms, collect_fuzzy(*loaded, U"lord", max_edits))
        << max_edits;
  }

  // A typo has to reach the word it was a typo of.
  auto typo_terms = collect_fuzzy(invidx, U"sanctifed", 1);
  EXPECT_NE(std::find(typo_terms.begin(), typo_terms.end(), U"sanctified"),
            typo_terms.end());

  // "apple" stays plain-coded, "the" is EF-coded, the phrase query exercises
  // is_term_position and multi-term text ranges, "sanctif*" exercises the
  // prefix expansion (dictionary scan plus union), "sanct*fy" the wildcard
  // expansion (automaton walk plus union), and "sanctifed~1" the fuzzy one,
  // on both paths.
  for (const auto *query : {R"( apple )", R"( the )", R"( "apple tree" )",
                            R"( "the lord" )", R"( sanctif* )",
                            R"( sanct*fy )", R"( sanctifed~1 )"}) {
    auto expr = parse_query(normalizer, query);
    ASSERT_TRUE(expr);

    auto expected = perform_search(invidx, *expr);
    auto actual = perform_search(*loaded, *expr);
    ASSERT_GT(expected->size(), 0u) << query;
    ASSERT_EQ(expected->size(), actual->size()) << query;
    for (size_t i = 0; i < expected->size(); i++) {
      EXPECT_EQ(expected->document_ordinal(i), actual->document_ordinal(i));
      ASSERT_EQ(expected->search_hit_count(i), actual->search_hit_count(i));
      for (size_t h = 0; h < expected->search_hit_count(i); h++) {
        EXPECT_EQ(expected->term_position(i, h), actual->term_position(i, h));

        auto expected_range = invidx.text_range(*expected, i, h);
        auto actual_range = loaded->text_range(*actual, i, h);
        EXPECT_EQ(expected_range.position, actual_range.position);
        EXPECT_EQ(expected_range.length, actual_range.length);
      }
      EXPECT_AP(bm25_score(invidx, *expr, *expected, i),
                bm25_score(*loaded, *expr, *actual, i));
    }
  }
}

TEST(KJVTest, CompressedBackendTombstones) {
  // Keys are the TSV's verse ids (book/chapter/verse), so Genesis 1:1 is
  // 1001001. This used to remove 1001 -- a key no verse has -- and passed
  // only because removal then tombstoned any number it was handed.
  auto invidx = kjv_index();
  ASSERT_TRUE(invidx.document_ordinal(1001001));
  invidx.remove_document(1001001);

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);

  auto loaded = load_compressed_index(compressed);
  EXPECT_TRUE(loaded->has_removed_documents());
  EXPECT_TRUE(loaded->is_document_removed(*loaded->document_ordinal(1001001)));
  EXPECT_FALSE(loaded->is_document_removed(*loaded->document_ordinal(1001002)));
  EXPECT_EQ(1001001, loaded->document_key(*loaded->document_ordinal(1001001)));
}

TEST(KJVTest, CompressedBackendRejectsPlainFormat) {
  const auto &invidx = kjv_index();

  std::stringstream plain(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(plain);

  EXPECT_THROW(load_compressed_index(plain), std::runtime_error);
}

TEST(KJVTest, UTF8DecodePerformance) {
  // auto normalizer = [](const auto &str) {
  //   return unicode::to_lowercase(str);
  // };
  auto normalizer = to_lowercase;

  std::ifstream fs(KJV_PATH);

  std::string s;
  while (std::getline(fs, s)) {
    UTF8PlainTextTokenizer tokenizer(s);
    tokenizer(normalizer, [&](auto &str, auto, auto) {});
  }
}

// Every document a result names, with the (position, length) of each hit.
using Hits = std::map<size_t, std::vector<std::pair<size_t, size_t>>>;

static Hits collect_hits(const IPostings &postings) {
  Hits hits;
  for (size_t i = 0; i < postings.size(); i++) {
    auto &positions = hits[postings.document_ordinal(i)];
    for (size_t j = 0; j < postings.search_hit_count(i); j++) {
      positions.emplace_back(postings.term_position(i, j),
                             postings.term_length(i, j));
    }
    std::sort(positions.begin(), positions.end());
  }
  return hits;
}

// And, Or and Not evaluated here, over maps, with every other node handed to
// perform_search whole. perform_search gives the node it is handed no
// filter, so as long as those nodes do not nest an And or an Or themselves,
// nothing on this side is filter-pushed-down, and the answer is what the
// query means rather than what the pushdown computes.
static Hits reference_hits(const IInvertedIndex &index,
                           const Expression &expr) {
  auto merge = [](std::vector<std::pair<size_t, size_t>> a,
                  const std::vector<std::pair<size_t, size_t>> &b) {
    a.insert(a.end(), b.begin(), b.end());
    std::sort(a.begin(), a.end());
    return a;
  };

  switch (expr.operation) {
  case Operation::And: {
    std::optional<Hits> hits;
    std::vector<Hits> negatives;
    for (const auto &node : expr.nodes) {
      if (node.operation == Operation::Not) {
        negatives.push_back(reference_hits(index, node.nodes[0]));
        continue;
      }
      auto operand = reference_hits(index, node);
      if (!hits) {
        hits = std::move(operand);
        continue;
      }
      Hits both;
      for (const auto &[ordinal, positions] : *hits) {
        auto it = operand.find(ordinal);
        if (it != operand.end()) {
          both[ordinal] = merge(positions, it->second);
        }
      }
      hits = std::move(both);
    }
    if (!hits) {
      return {};
    }
    for (const auto &negative : negatives) {
      for (const auto &[ordinal, _] : negative) {
        hits->erase(ordinal);
      }
    }
    return *hits;
  }
  case Operation::Or: {
    Hits hits;
    for (const auto &node : expr.nodes) {
      for (const auto &[ordinal, positions] : reference_hits(index, node)) {
        hits[ordinal] = merge(hits[ordinal], positions);
      }
    }
    return hits;
  }
  default:
    return collect_hits(*perform_search(index, expr));
  }
}

// An And hands its most selective operand to the rest as a filter, and an
// Or, a phrase or a Near under that filter answers only at the filter's
// documents. None of that may change what a query matches or where: each
// query here is one of those shapes, checked against reference_hits on both
// backends, since the probing goes through each backend's own advance.
TEST(KJVTest, FilterPushdownMatchesPlainEvaluation) {
  const auto &invidx = kjv_index();

  std::stringstream compressed(std::ios::in | std::ios::out | std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  const char *queries[] = {
      // A rare Term leads, and the Or is probed at its documents.
      "leviathan (the | and)",
      "(the | and) leviathan",
      "leviathan (the | and) -god",
      "leviathan -(the | and)",
      "leviathan (the | zzzzqqq)",
      "zzzzqqq (the | and)",
      // The filter passes down through an And into an Or, a phrase and a
      // Near, where it joins the intersection as the leading operand.
      "leviathan (the (and | of))",
      "leviathan \"of the\"",
      "leviathan (the ~ and)",
      "leviathan (the | \"of the\")",
      "jesus \"the lord\" -(peter | john)",
      // The Or leads, as the rarer side, with Terms filtered by it.
      "the (leviathan | behemoth)",
      "(love | hate) (god | lord) the",
      "peace (love | (joy (hope | faith)))",
      // Expansions rank last, and are probed only when the filter is short.
      "sanctif* (the | and)",
      "leviathan sanctif*",
      "leviathen~1 (the | and)",
      "l*viathan (the | of)",
      // A filter long enough that the Or is unioned in full instead.
      "god (lord | jesus) -(david | moses)",
  };

  size_t documents = 0;
  for (const IInvertedIndex *index :
       {static_cast<const IInvertedIndex *>(&invidx),
        static_cast<const IInvertedIndex *>(loaded.get())}) {
    for (auto query : queries) {
      SCOPED_TRACE(query);
      auto expr = parse_query(normalizer, query);
      ASSERT_TRUE(expr);
      auto expected = reference_hits(*index, *expr);
      EXPECT_EQ(expected, collect_hits(*perform_search(*index, *expr)));
      documents += expected.size();
    }
  }
  // Guards against a query set that matches nothing and so proves nothing.
  EXPECT_GT(documents, 1000u);
}
