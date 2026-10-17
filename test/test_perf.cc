#include <gtest/gtest.h>
#include <searchlib.h>

#include <chrono>
#include <cstdio>
#include <limits>
#include <sstream>

#include "test_utils.h"

using namespace searchlib;

// Performance regressions here are expressed as ratios between two
// measurements taken in the same process on the same machine, never as
// absolute times. A ratio cancels out machine speed and CI load, so the
// threshold can stay tight enough to be meaningful without being flaky --
// as long as the two sides are measured alike, which measure_ratio sees to.
// Where the invariant is how often one part calls into another, the test
// counts the calls instead, and a count is the same on every machine.
//
// Each test pins an invariant about what a cost is allowed to depend on. The
// bug that motivated the first one made bm25 scoring O(hits * documents) and
// went unnoticed for a long time, even though the test suite itself was
// paying for it on every run.

namespace {

auto perf_normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

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

// A ratio of two costs, the numerator's over the denominator's, measured so
// that noise alone is unlikely to carry it over `bound`:
//
// - Each side is timed in batches of at least kBatchUs, repeated as many
//   times as that takes, so that the timer's resolution is a small part of
//   any one measurement even for a run of a few microseconds.
// - The two sides take turns, so that a change in clock speed or in the
//   load on a shared CI machine lands on both rather than on one.
// - Each side keeps its fastest batch, since noise only ever slows one.
//   Many short batches beat a few long ones here: on a machine running
//   four times as many busy processes as it has cores, 30 batches of 1ms
//   kept every ratio under its bound where 15 of 2ms did not.
// - A ratio at or over the bound is measured again, up to kAttempts times,
//   and the lowest is kept. A regression puts every attempt over the bound;
//   a rerun can only rescue one that noise pushed there.
//
// Every attempt is printed, so a CI log (ctest -V) carries each machine's
// numbers for when a bound needs revisiting.
struct Ratio {
  double value;
  double numerator_us;
  double denominator_us;
};

template <typename Numerator, typename Denominator>
Ratio measure_ratio(const char *what, double bound, Numerator numerator,
                    Denominator denominator) {
  constexpr double kBatchUs = 1000;
  constexpr int kRounds = 30;
  constexpr int kAttempts = 5;
  using clock = std::chrono::steady_clock;
  auto microseconds = [](clock::duration d) {
    return std::chrono::duration<double, std::micro>(d).count();
  };

  // A warm-up run, then one timed run to size the batches from.
  auto repetitions = [&](auto &f) {
    f();
    auto start = clock::now();
    f();
    auto us = microseconds(clock::now() - start);
    return std::max<size_t>(1,
                            static_cast<size_t>(kBatchUs / std::max(us, 0.01)));
  };
  auto batch_us = [&](auto &f, size_t times) {
    auto start = clock::now();
    for (size_t i = 0; i < times; i++) {
      f();
    }
    return microseconds(clock::now() - start) / static_cast<double>(times);
  };

  auto numerator_times = repetitions(numerator);
  auto denominator_times = repetitions(denominator);
  Ratio best{std::numeric_limits<double>::infinity(), 0, 0};
  for (int attempt = 0; attempt < kAttempts && !(best.value < bound);
       attempt++) {
    auto numerator_us = std::numeric_limits<double>::infinity();
    auto denominator_us = std::numeric_limits<double>::infinity();
    for (int round = 0; round < kRounds; round++) {
      numerator_us =
          std::min(numerator_us, batch_us(numerator, numerator_times));
      denominator_us =
          std::min(denominator_us, batch_us(denominator, denominator_times));
    }
    Ratio ratio{numerator_us / denominator_us, numerator_us, denominator_us};
    std::printf("[   perf   ] %s: %.2fx (%.2fus / %.2fus), bound %.2fx\n", what,
                ratio.value, numerator_us, denominator_us, bound);
    if (ratio.value < best.value) {
      best = ratio;
    }
  }
  return best;
}

// An operand that counts every call made into it, for an invariant about how
// often a result consults its operands.
class CountingPostings : public IPostings {
public:
  explicit CountingPostings(std::shared_ptr<IPostings> inner)
      : inner_(std::move(inner)) {}

  size_t calls() const { return calls_; }
  void reset() { calls_ = 0; }

  size_t size() const override {
    calls_++;
    return inner_->size();
  }
  size_t document_ordinal(size_t index) const override {
    calls_++;
    return inner_->document_ordinal(index);
  }
  size_t search_hit_count(size_t index) const override {
    calls_++;
    return inner_->search_hit_count(index);
  }
  bool prefers_block_reads() const override {
    calls_++;
    return inner_->prefers_block_reads();
  }
  size_t read_ordinals(size_t index, size_t *out, size_t count) const override {
    calls_++;
    return inner_->read_ordinals(index, out, count);
  }
  size_t read_hit_counts(size_t index, size_t *out,
                         size_t count) const override {
    calls_++;
    return inner_->read_hit_counts(index, out, count);
  }
  void read_term_positions(size_t index, size_t *out) const override {
    calls_++;
    inner_->read_term_positions(index, out);
  }
  size_t advance(size_t from, size_t ordinal) const override {
    calls_++;
    return inner_->advance(from, ordinal);
  }
  size_t term_position(size_t index, size_t search_hit_index) const override {
    calls_++;
    return inner_->term_position(index, search_hit_index);
  }
  size_t term_length(size_t index, size_t search_hit_index) const override {
    calls_++;
    return inner_->term_length(index, search_hit_index);
  }
  bool is_term_position(size_t index, size_t term_pos) const override {
    calls_++;
    return inner_->is_term_position(index, term_pos);
  }

private:
  std::shared_ptr<IPostings> inner_;
  mutable size_t calls_ = 0;
};

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

  // The index is 10x bigger, so the regression this guards against shows up
  // as roughly 10x. Anything below 4x means the cost is not tracking the
  // document count.
  auto ratio = measure_ratio(
      "scoring against 10x the documents", 4.0,
      [&] { score_all_hits(large, *expr, *large_result); },
      [&] { score_all_hits(small, *expr, *small_result); });
  EXPECT_LT(ratio.value, 4.0)
      << "scoring " << kHits << " hits took " << ratio.denominator_us
      << "us against 1,000 documents but " << ratio.numerator_us
      << "us against 10,000 -- bm25 cost is scaling with the document count";
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
  // [&]: MSVC does not let a lambda read a constexpr local without a capture.
  auto build = [&](size_t vocabulary) {
    InMemoryInvertedIndex<TextRange> invidx;
    InMemoryIndexer indexer(invidx, perf_normalizer);
    size_t document_key = 0;
    for (size_t i = 0; i < vocabulary; i++) {
      indexer.index_document(document_key++,
                             UTF8PlainTextTokenizer(alpha_term("term", i)));
    }
    for (size_t i = 0; i < kHits; i++) {
      indexer.index_document(document_key++,
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

  // The dictionary is 10x bigger, so a per-hit expansion shows up as roughly
  // 10x. Anything under 2x means the cost is not tracking the vocabulary.
  auto ratio = measure_ratio(
      "scoring a prefix over 10x the vocabulary", 2.0,
      [&] { score_all_hits(large_scorer, *large_result); },
      [&] { score_all_hits(small_scorer, *small_result); });
  EXPECT_LT(ratio.value, 2.0)
      << "scoring " << kHits << " prefix hits took " << ratio.denominator_us
      << "us against a 500-term vocabulary but " << ratio.numerator_us
      << "us against 5,000 -- the scorer is re-expanding the prefix per hit";
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
  // The Or scores 10/9 as many hits and consults two terms per hit instead
  // of one, so a bit over twice the single-term work is the honest floor.
  // Measured at 1.8x with O(1) absent lookups and 3.4x when they fall back
  // to searching, both steady across runs; 2.5x sits between them.
  auto ratio = measure_ratio(
      "scoring an Or against one term", 2.5,
      [&] {
        BM25Scorer scorer(invidx, *or_expr);
        score_all_hits(scorer, *or_result);
      },
      [&] {
        BM25Scorer scorer(invidx, *single_expr);
        score_all_hits(scorer, *single_result);
      });
  EXPECT_LT(ratio.value, 2.5)
      << "scoring " << kHits << " Or hits took " << ratio.numerator_us
      << "us against " << ratio.denominator_us << "us for "
      << single_result->size() << " single-term hits (" << ratio.value
      << "x) -- absent-term lookups are searching";
}

// An And/Or result is a view: it keeps the matched document ids and reads
// the operands' positions on demand. So building one has to stay proportional
// to the number of documents it matched, with nothing per hit -- which is
// what enumerating those document ids afterwards measures. Materializing the
// positions during the walk, as the result used to, shows up here and nowhere
// else, since no other part of a query reads them.
TEST(PerfTest, BuildingAUnionCostsLittleMoreThanEnumeratingItsDocumentIds) {
  constexpr size_t kDocuments = 5000;

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, perf_normalizer);
    for (size_t i = 0; i < kDocuments; i++) {
      // Both terms in every document, so the union has two hits per document
      // to merge rather than one to pass straight through.
      indexer.index_document(i, UTF8PlainTextTokenizer("alpha beta gamma"));
    }
  }

  auto expr = parse_query(perf_normalizer, "alpha | beta");
  ASSERT_TRUE(expr);

  auto result = perform_search(invidx, *expr);
  ASSERT_EQ(kDocuments, result->size());

  // Both sides are one virtual call per document plus the merge, so the
  // bound comes from measurement rather than first principles: this ratio
  // reads 3.9-5.2x on the view against 27.4x when the same walk also built
  // every document's positions. 12x sits between, with better than 2x of
  // margin on either side.
  auto ratio = measure_ratio(
      "building a union against walking its ids", 12.0,
      [&] { perform_search(invidx, *expr); },
      // document_ordinal only. search_hit_count would drag the operand
      // lookups into the denominator and hide exactly what this test is
      // about.
      [&] {
        size_t sink = 0;
        for (size_t i = 0; i < result->size(); i++) {
          sink += result->document_ordinal(i);
        }
        EXPECT_GT(sink, 0u);
      });
  EXPECT_LT(ratio.value, 12.0)
      << "building a " << kDocuments << "-hit union took " << ratio.numerator_us
      << "us against " << ratio.denominator_us
      << "us to enumerate its document ids (" << ratio.value
      << "x) -- construction is doing per-hit work";
}

// The other side of that bargain: a view has to find each document again in
// each operand before it can answer a position. Two things keep that from
// being a search -- a forward cursor per operand, and a one-row memo so that
// a document's hits cost one merge rather than one each -- and losing either
// turns a full position scan superlinear.
//
// Both come down to how often the view calls into its operands, so this
// counts the calls. It used to time the scan against the same scan over a
// bare term's postings, and that ratio depends on the machine: the view
// costs the same per document on x86 and on Apple's cores, but Apple's run
// the term's plain array walk twice as fast, which put the healthy ratio on
// macOS at the bound.
TEST(PerfTest, ReadingAUnionsPositionsResumesInsteadOfSearching) {
  constexpr size_t kDocuments = 20000;

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, perf_normalizer);
    for (size_t i = 0; i < kDocuments; i++) {
      indexer.index_document(i, UTF8PlainTextTokenizer("alpha beta gamma"));
    }
  }

  auto operand = [&](const char *term) {
    auto expr = parse_query(perf_normalizer, term);
    EXPECT_TRUE(expr);
    return std::make_shared<CountingPostings>(perform_search(invidx, *expr));
  };
  auto alpha = operand("alpha");
  auto beta = operand("beta");
  // What `alpha | beta` builds. The walk that builds it is the previous
  // test's business; this one counts from the first read.
  auto result = detail::union_postings({alpha, beta});
  ASSERT_EQ(kDocuments, result->size());
  alpha->reset();
  beta->reset();

  size_t hits = 0;
  size_t sink = 0;
  for (size_t i = 0; i < result->size(); i++) {
    auto count = result->search_hit_count(i);
    hits += count;
    for (size_t h = 0; h < count; h++) {
      sink += result->term_position(i, h) + result->term_length(i, h);
    }
  }
  EXPECT_GT(sink, 0u);
  ASSERT_EQ(2 * kDocuments, hits);

  // Finding a document from each operand's cursor, its hit count there, and
  // each hit's position and length come to 6.5 calls a hit as it stands,
  // whatever the document count. Without the cursors it is 18.9 here, a
  // search per document per operand, and climbs with the document count
  // (15.5 at 2,000); without the row memo it is 31.5, a merge per position
  // read. 10 sits between, 1.5x above the healthy count and 1.9x below the
  // nearest broken one.
  auto calls_per_hit =
      static_cast<double>(alpha->calls() + beta->calls()) / hits;
  std::printf("[   perf   ] operand calls per union hit: %.2f, bound 10\n",
              calls_per_hit);
  EXPECT_LT(calls_per_hit, 10.0)
      << "reading every position of a " << kDocuments << "-document union "
      << "made " << calls_per_hit << " calls into its operands per hit -- "
      << "it is searching them per lookup or merging a document per hit";
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
  // 5 terms out of 5,005. A subtree descent is orders of magnitude cheaper
  // than a full walk; a full scan would make the two roughly equal.
  auto ratio = measure_ratio(
      "enumerating a selective prefix against all terms", 0.2,
      [&] {
        loaded->enumerate_terms_with_prefix(
            U"zqx", [&](const auto &t) { sink += t.size(); });
      },
      [&] {
        loaded->enumerate_terms_with_prefix(
            U"", [&](const auto &t) { sink += t.size(); });
      });
  EXPECT_GT(sink, 0u);
  EXPECT_LT(ratio.value, 0.2)
      << "enumerating 5 terms took " << ratio.numerator_us << "us against "
      << ratio.denominator_us << "us for all " << all_terms
      << " -- the FST dictionary is not being descended";
}
