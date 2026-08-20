#include <gtest/gtest.h>
#include <searchlib.h>

#include <chrono>
#include <sstream>

#include "test_utils.h"

using namespace searchlib;

// Performance regressions here are expressed as ratios between two
// measurements taken in the same process on the same machine, never as
// absolute times. A ratio cancels out machine speed and CI load, so the
// threshold can stay tight enough to be meaningful without being flaky.
//
// Each test pins an invariant about what a cost is allowed to depend on. The
// bug that motivated the first one made bm25 scoring O(hits * documents) and
// went unnoticed for a long time, even though the test suite itself was
// paying for it on every run.

namespace {

auto perf_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

// Minimum of several runs: the fastest observed run is the one least
// polluted by scheduling noise, so it is the stablest estimator here.
template <typename F> double best_of(size_t runs, F f) {
  f(); // warm up caches and any one-time allocation
  double best = std::numeric_limits<double>::max();
  for (size_t i = 0; i < runs; i++) {
    auto start = std::chrono::steady_clock::now();
    f();
    auto end = std::chrono::steady_clock::now();
    best = std::min(
        best, std::chrono::duration<double, std::micro>(end - start).count());
  }
  return best;
}

// `hit_count` documents contain "rareterm"; the rest are filler, so the
// postings list being scored is the same length regardless of index size.
InMemoryInvertedIndex<TextRange> index_with(size_t document_count,
                                            size_t hit_count) {
  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, perf_normalizer);
  for (size_t i = 0; i < document_count; i++) {
    indexer.index_document(i, UTF8PlainTextTokenizer(
                                  i < hit_count ? "alpha beta rareterm"
                                                : "alpha beta gamma"));
  }
  return invidx;
}

// The tokenizer keeps letter sequences only, so distinct terms have to be
// spelled with letters: "termaaa", "termbaa", ... (base-26, three digits).
std::string alpha_term(const std::string &prefix, size_t i) {
  auto term = prefix;
  for (int digit = 0; digit < 3; digit++) {
    term += static_cast<char>('a' + (i % 26));
    i /= 26;
  }
  return term;
}

double score_all_hits(const IInvertedIndex &invidx, const Expression &expr,
                      const IPostings &result) {
  double total = 0.0;
  for (size_t i = 0; i < result.size(); i++) {
    total += bm25_score(invidx, expr, result, i);
  }
  return total;
}

// The same loop through a prepared scorer, for the tests that pin
// BM25Scorer's own cost model.
double score_all_hits(const BM25Scorer &scorer, const IPostings &result) {
  double total = 0.0;
  for (size_t i = 0; i < result.size(); i++) {
    total += scorer(result, i);
  }
  return total;
}

} // namespace

// Scoring a fixed number of hits must not get more expensive just because the
// index holds more documents. average_document_term_count() used to walk
// documents_ on every call, and bm25_score calls it once per hit, so a 10x
// larger index made the same scoring work 10x slower.
TEST(PerfTest, ScoringDoesNotScaleWithDocumentCount) {
  constexpr size_t kHits = 50;
  auto small = index_with(1000, kHits);
  auto large = index_with(10000, kHits);

  auto expr = parse_query(perf_normalizer, "rareterm");
  ASSERT_TRUE(expr);

  auto small_result = perform_search(small, *expr);
  auto large_result = perform_search(large, *expr);
  ASSERT_EQ(kHits, small_result->size());
  ASSERT_EQ(kHits, large_result->size());

  auto small_us =
      best_of(20, [&] { score_all_hits(small, *expr, *small_result); });
  auto large_us =
      best_of(20, [&] { score_all_hits(large, *expr, *large_result); });

  // The index is 10x bigger, so the regression this guards against shows up
  // as roughly 10x. Anything below 4x means the cost is not tracking the
  // document count.
  auto ratio = large_us / small_us;
  EXPECT_LT(ratio, 4.0) << "scoring " << kHits << " hits took " << small_us
                        << "us against 1,000 documents but " << large_us
                        << "us against 10,000 -- bm25 cost is scaling with "
                        << "the document count";
}

// bm25_score answers a Prefix node by enumerating the dictionary on every
// call, so ranking a prefix query with it costs O(hits * vocabulary).
// BM25Scorer expands the node once, at construction. If that expansion ever
// moves back into the per-hit path, scoring starts tracking the vocabulary
// size again -- which is the whole reason the scorer exists.
TEST(PerfTest, ScorerDoesNotScaleWithVocabulary) {
  constexpr size_t kHits = 200;

  // Five "zqx..." terms spread over kHits documents, plus `vocabulary`
  // single-use filler terms. The postings actually being scored are
  // identical in both indexes; only the surrounding dictionary differs.
  auto build = [](size_t vocabulary) {
    InMemoryInvertedIndex<TextRange> invidx;
    InMemoryIndexer indexer(invidx, perf_normalizer);
    size_t document_id = 0;
    for (size_t i = 0; i < vocabulary; i++) {
      indexer.index_document(document_id++,
                             UTF8PlainTextTokenizer(alpha_term("term", i)));
    }
    for (size_t i = 0; i < kHits; i++) {
      indexer.index_document(document_id++,
                             UTF8PlainTextTokenizer(alpha_term("zqx", i % 5)));
    }
    return invidx;
  };

  auto small = build(500);
  auto large = build(5000);

  auto expr = parse_query(perf_normalizer, "zqx*");
  ASSERT_TRUE(expr);

  auto small_result = perform_search(small, *expr);
  auto large_result = perform_search(large, *expr);
  ASSERT_EQ(kHits, small_result->size());
  ASSERT_EQ(kHits, large_result->size());

  // Built outside the timed region deliberately: the one-time expansion is
  // allowed to cost more on a bigger dictionary, the per-hit scoring is not.
  BM25Scorer small_scorer(small, *expr);
  BM25Scorer large_scorer(large, *expr);

  auto small_us =
      best_of(20, [&] { score_all_hits(small_scorer, *small_result); });
  auto large_us =
      best_of(20, [&] { score_all_hits(large_scorer, *large_result); });

  // The dictionary is 10x bigger, so a per-hit expansion shows up as roughly
  // 10x. Anything under 2x means the cost is not tracking the vocabulary.
  auto ratio = large_us / small_us;
  EXPECT_LT(ratio, 2.0) << "scoring " << kHits << " prefix hits took "
                        << small_us << "us against a 500-term vocabulary but "
                        << large_us << "us against 5,000 -- the scorer is "
                        << "re-expanding the prefix per hit";
}

// Most hits of an Or carry only some of its terms, so "this document does not
// have this term" is the common answer, not the rare one. The scorer's cursor
// invariant is what makes it free: walking forward, everything before the
// cursor is already known to be below the target, so a cursor sitting past
// the target settles the question without searching. Lose that and every
// absent lookup falls back to a binary search over an ever-growing prefix.
TEST(PerfTest, ScoringAnOrDoesNotSearchForAbsentTerms) {
  constexpr size_t kHits = 100000;

  // The layout has to leave the absent term's cursor *ahead* of the target,
  // with entries on both sides of it -- that is the only shape the invariant
  // saves, and it is the shape a real scattered term has. Two layouts that
  // look plausible do not produce it: interleaving the terms leaves the
  // cursor trailing the target (the galloping branch), and giving beta a
  // contiguous block leaves it exhausted past the end (already O(1)).
  // Sprinkling beta every tenth document does, for 90% of the hits.
  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, perf_normalizer);
    for (size_t i = 0; i < kHits; i++) {
      indexer.index_document(i, UTF8PlainTextTokenizer(
                                    i % 10 == 0 ? "beta filler" : "alpha filler"));
    }
  }

  auto or_expr = parse_query(perf_normalizer, "alpha | beta");
  auto single_expr = parse_query(perf_normalizer, "alpha");
  ASSERT_TRUE(or_expr);
  ASSERT_TRUE(single_expr);

  auto or_result = perform_search(invidx, *or_expr);
  auto single_result = perform_search(invidx, *single_expr);
  ASSERT_EQ(kHits, or_result->size());
  ASSERT_EQ(kHits - kHits / 10, single_result->size());

  // Built inside the timed region, unlike the vocabulary test above. A
  // scorer's cursors are left at the end of a full pass, so reusing one
  // across runs would start every run but the first with a backwards walk
  // and measure the fallback instead of the forward path this test is about.
  // Construction here is two postings lookups and two logarithms.
  auto or_us = best_of(20, [&] {
    BM25Scorer scorer(invidx, *or_expr);
    score_all_hits(scorer, *or_result);
  });
  auto single_us = best_of(20, [&] {
    BM25Scorer scorer(invidx, *single_expr);
    score_all_hits(scorer, *single_result);
  });

  // The Or scores 10/9 as many hits and consults two terms per hit instead
  // of one, so a bit over twice the single-term work is the honest floor.
  // Measured at 1.8x with O(1) absent lookups and 3.4x when they fall back
  // to searching, both steady across runs; 2.5x sits between them.
  auto ratio = or_us / single_us;
  EXPECT_LT(ratio, 2.5) << "scoring " << kHits << " Or hits took "
                        << or_us << "us against " << single_us
                        << "us for " << single_result->size()
                        << " single-term hits ("
                        << ratio << "x) -- absent-term lookups are searching";
}

// Building a union used to put a Position plus two vectors on the heap for
// every matched document, so the construction cost was dominated by
// allocation rather than by the merge itself. Walking the finished result is
// the natural yardstick: both are linear in the hit count, so the ratio
// between them is flat unless per-hit allocation comes back -- at which point
// only the construction side moves. A "10x the hits costs 10x the time" check
// would not catch this, since per-hit allocation is itself linear.
TEST(PerfTest, UnionConstructionCostsLittleMoreThanWalkingIt) {
  constexpr size_t kDocuments = 5000;

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, perf_normalizer);
    for (size_t i = 0; i < kDocuments; i++) {
      // Both terms in every document, so the union merges two hits per
      // document rather than passing one straight through.
      indexer.index_document(i, UTF8PlainTextTokenizer("alpha beta gamma"));
    }
  }

  auto expr = parse_query(perf_normalizer, "alpha | beta");
  ASSERT_TRUE(expr);

  auto result = perform_search(invidx, *expr);
  ASSERT_EQ(kDocuments, result->size());

  auto build_us = best_of(20, [&] { perform_search(invidx, *expr); });
  auto walk_us = best_of(20, [&] {
    size_t sink = 0;
    for (size_t i = 0; i < result->size(); i++) {
      sink += result->document_id(i) + result->search_hit_count(i);
    }
    EXPECT_GT(sink, 0u);
  });

  // Most of this ratio is the merge itself -- walking two virtual calls per
  // hit is far cheaper than producing them -- so the bound is set from
  // measurement rather than from first principles: 31x on the flat layout
  // against 70x on the per-hit-allocating one it replaced, both stable to
  // within a few percent across runs. 50x sits between them.
  auto ratio = build_us / walk_us;
  EXPECT_LT(ratio, 50.0) << "building a " << kDocuments
                         << "-hit union took " << build_us
                         << "us against " << walk_us
                         << "us to walk it (" << ratio
                         << "x) -- the result is allocating per hit again";
}

// The compressed backend stores its dictionary as an FST specifically so that
// a prefix lookup descends to the matching subtree. If it ever falls back to
// testing every term, a selective prefix costs the same as enumerating the
// whole vocabulary.
TEST(PerfTest, CompressedPrefixEnumerationScalesWithHitsNotVocabulary) {
  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, perf_normalizer);
    // 5,000 distinct terms, of which only 5 start with "zqx".
    for (size_t i = 0; i < 5000; i++) {
      indexer.index_document(i,
                             UTF8PlainTextTokenizer(alpha_term("term", i)));
    }
    for (size_t i = 0; i < 5; i++) {
      indexer.index_document(5000 + i,
                             UTF8PlainTextTokenizer(alpha_term("zqx", i)));
    }
  }

  std::stringstream compressed(std::ios::in | std::ios::out |
                               std::ios::binary);
  invidx.save(compressed, {}, IndexFormat::Compressed);
  auto loaded = load_compressed_index(compressed);

  size_t selective_terms = 0;
  loaded->enumerate_terms_with_prefix(U"zqx",
                                      [&](const auto &) { selective_terms++; });
  size_t all_terms = 0;
  loaded->enumerate_terms_with_prefix(U"", [&](const auto &) { all_terms++; });
  ASSERT_EQ(5, selective_terms);
  ASSERT_EQ(5005, all_terms);

  size_t sink = 0;
  auto selective_us = best_of(20, [&] {
    loaded->enumerate_terms_with_prefix(
        U"zqx", [&](const auto &t) { sink += t.size(); });
  });
  auto all_us = best_of(20, [&] {
    loaded->enumerate_terms_with_prefix(U"",
                                        [&](const auto &t) { sink += t.size(); });
  });
  EXPECT_GT(sink, 0u);

  // 5 terms out of 5,005. A subtree descent is orders of magnitude cheaper
  // than a full walk; a full scan would make the two roughly equal.
  EXPECT_LT(selective_us, all_us / 5.0)
      << "enumerating 5 terms took " << selective_us
      << "us against " << all_us << "us for all " << all_terms
      << " -- the FST dictionary is not being descended";
}
