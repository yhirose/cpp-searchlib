//
//  bench.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

// Query benchmark harness.
//
// The search phase and the scoring phase are reported separately because they
// are dominated by different costs and fixed by different changes: the search
// phase by how the result set is laid out in memory, the scoring phase by how
// much per-term work is repeated for every hit. A single end-to-end number
// hides which of the two moved.
//
// Timings are the best of N runs, via the same best_of estimator the
// perf-regression tests use (shared through test/test_utils.h so the two
// cannot drift apart). Absolute numbers are only comparable against other
// runs on the same machine; what carries across machines is the ratio
// between two rows.

#include <searchlib.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "../test/test_utils.h"
#include "unicodelib/unicodelib.h"

using namespace searchlib;

namespace {

auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

// The KJV corpus, one document per verse (field 4 of the TSV). `repeat`
// concatenates the whole corpus that many times under fresh document ids,
// which lengthens every postings list without growing the vocabulary --
// exactly the axis the result-set layout is sensitive to.
std::vector<std::string> read_corpus(const std::string &path, size_t repeat) {
  std::ifstream is(path);
  if (!is) {
    std::fprintf(stderr, "bench: cannot open %s\n", path.c_str());
    std::exit(1);
  }

  std::vector<std::string> texts;
  std::string line;
  while (std::getline(is, line)) {
    if (line.empty()) {
      continue;
    }
    auto fields = split(line, '\t');
    if (fields.size() > 4) {
      texts.push_back(fields[4]);
    }
  }

  if (repeat > 1) {
    auto original = texts.size();
    texts.reserve(original * repeat);
    // Element-wise on purpose: insert()'s preconditions forbid a source
    // range inside the destination, and push_back is self-reference-safe.
    for (size_t r = 1; r < repeat; r++) {
      for (size_t i = 0; i < original; i++) {
        texts.push_back(texts[i]);
      }
    }
  }
  return texts;
}

struct Query {
  const char *name;
  const char *text;
};

// A common term, a rare one, and the boolean/phrase shapes in between. The
// spread matters: a single Term result aliases the term's postings and does
// no search work at all, so its cost is entirely scoring, while an Or builds
// a fresh result set and splits the cost between the two phases.
const Query kQueries[] = {
    {"single_common", "love"},
    {"single_rare", "leviathan"},
    {"and_two", "love god"},
    {"or_two", "love | hate"},
    {"phrase", "\"in the beginning\""},
};

} // namespace

int main(int argc, char **argv) {
  std::string path = "../../test/t_kjv.tsv";
  size_t repeat = 1;
  size_t runs = 20;

  for (int i = 1; i < argc; i++) {
    if (std::strcmp(argv[i], "--repeat") == 0 && i + 1 < argc) {
      repeat = std::stoul(argv[++i]);
    } else if (std::strcmp(argv[i], "--runs") == 0 && i + 1 < argc) {
      runs = std::stoul(argv[++i]);
    } else if (argv[i][0] == '-') {
      std::fprintf(stderr,
                   "usage: bench [--repeat N] [--runs N] [KJV_TSV_PATH]\n");
      return 1;
    } else {
      path = argv[i];
    }
  }

  auto texts = read_corpus(path, repeat);

  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer);
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < texts.size(); i++) {
      indexer.index_document(i, UTF8PlainTextTokenizer(texts[i]));
    }
    auto end = std::chrono::steady_clock::now();
    std::printf("documents  %zu\n", texts.size());
    std::printf("build      %.1f ms\n\n",
                std::chrono::duration<double, std::milli>(end - start).count());
  }

  std::printf("%-15s %8s %10s %10s %10s %10s\n", "query", "hits", "search_us",
              "score_us", "scorer_us", "top10_us");

  for (const auto &query : kQueries) {
    auto parsed = parse_query(normalizer, query.text);
    if (!parsed) {
      std::printf("%-15s  PARSE FAILED\n", query.name);
      continue;
    }
    const auto &expr = *parsed;

    auto search_us =
        best_of(runs, [&] { auto r = perform_search(invidx, expr); });

    auto result = perform_search(invidx, expr);
    auto score_us = best_of(runs, [&] {
      double total = 0.0;
      for (size_t i = 0; i < result->size(); i++) {
        total += bm25_score(invidx, expr, *result, i);
      }
      // Keep the loop from being optimized away entirely.
      if (total == -1.0) {
        std::printf(" ");
      }
    });

    // The same scoring work through BM25Scorer, construction included, so
    // the column is a fair swap for score_us rather than a best case.
    auto scorer_us = best_of(runs, [&] {
      BM25Scorer scorer(invidx, expr);
      double total = 0.0;
      for (size_t i = 0; i < result->size(); i++) {
        total += scorer(*result, i);
      }
      if (total == -1.0) {
        std::printf(" ");
      }
    });

    // Search and rank together: the number a caller actually pays, and the
    // one directly comparable to another engine's top-k latency.
    auto top10_us = best_of(runs, [&] {
      auto r = perform_search(invidx, expr);
      BM25Scorer scorer(invidx, expr);
      auto hits =
          top_k(*r, 10, [&](size_t i) { return scorer(*r, i); });
      if (hits.size() == 999) {
        std::printf(" ");
      }
    });

    std::printf("%-15s %8zu %10.2f %10.2f %10.2f %10.2f\n", query.name,
                result->size(), search_us, score_us, scorer_us, top10_us);
  }

  return 0;
}
