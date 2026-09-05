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
//
// Everything the harness needs is an option, so that the same binary measures
// the small English corpus the optimization work was tuned against and a
// corpus large enough that building it is a memory problem:
//
//   searchlib-bench                                   # KJV x10, defaults
//   searchlib-bench --corpus aozora.tsv --text-field 3 \
//                   --repeat 1 --queries ja.txt \
//                   --splitter ja-ud-gsd.mod --text-ranges skip \
//                   --index /tmp/aozora.idx
//
// With --index the index is also saved, dropped and re-loaded, so the report
// covers what an installed corpus costs to ship and to open, not only what it
// costs to query.

#include <searchlib.h>
#include <searchlib_segment.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include "../test/test_utils.h"
#include <unicodelib.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <unistd.h>
#endif

using namespace searchlib;

namespace {

auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

// Resident set size in MiB, or 0 where we do not know how to ask. Reported
// rather than asserted on: it is the number that decides whether a corpus can
// be indexed at all on a given machine, and it is not comparable across
// operating systems.
//
// Signed, because a caller subtracts two samples and the second can be the
// smaller one: freeing the source corpus, or an allocator returning pages,
// makes the delta negative, and an unsigned subtraction turns that into
// 18 exabytes rather than into "nothing to report".
long long resident_mib() {
#if defined(__APPLE__)
  mach_task_basic_info info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
    return 0;
  }
  return static_cast<long long>(info.resident_size / (1024 * 1024));
#elif defined(__linux__)
  std::ifstream is("/proc/self/statm");
  size_t total_pages = 0, resident_pages = 0;
  if (!(is >> total_pages >> resident_pages)) {
    return 0;
  }
  return static_cast<long long>(resident_pages *
                                static_cast<size_t>(sysconf(_SC_PAGESIZE)) /
                                (1024 * 1024));
#else
  return 0;
#endif
}

size_t file_size(const std::string &path) {
  std::ifstream is(path, std::ios::binary | std::ios::ate);
  return is ? static_cast<size_t>(is.tellg()) : 0;
}

// One document's text per element, taken from `text_field` of a tab-separated
// file (the KJV corpus keeps the verse in field 4; an Aozora export written by
// bench/aozora.py keeps the work in field 3). `repeat` concatenates the whole
// corpus that many times under fresh document ids, which lengthens every
// postings list without growing the vocabulary -- exactly the axis the result
// -set layout is sensitive to, and the reason the small corpus stays useful.
std::vector<std::string> read_corpus(const std::string &path, size_t text_field,
                                     size_t repeat) {
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
    if (fields.size() > text_field) {
      texts.push_back(std::move(fields[text_field]));
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
  std::string name;
  std::string text;
};

// A common term, a rare one, and the boolean/phrase shapes in between. The
// spread matters: a single Term result aliases the term's postings and does
// no search work at all, so its cost is entirely scoring, while an Or builds
// a fresh result set and splits the cost between the two phases.
const Query kDefaultQueries[] = {
    {"single_common", "love"},
    {"single_rare", "leviathan"},
    {"and_two", "love god"},
    {"or_two", "love | hate"},
    {"phrase", "\"in the beginning\""},
};

// `name<TAB>query` per line, or just `query` (then the name is the query).
// Blank lines and lines starting with # are ignored, so a query set can carry
// its own notes.
std::vector<Query> read_queries(const std::string &path) {
  std::ifstream is(path);
  if (!is) {
    std::fprintf(stderr, "bench: cannot open %s\n", path.c_str());
    std::exit(1);
  }

  std::vector<Query> queries;
  std::string line;
  while (std::getline(is, line)) {
    if (line.empty() || line[0] == '#') {
      continue;
    }
    auto tab = line.find('\t');
    if (tab == std::string::npos) {
      queries.push_back({line, line});
    } else {
      queries.push_back({line.substr(0, tab), line.substr(tab + 1)});
    }
  }
  return queries;
}

// bm25_score walks the expression tree per hit, so on a corpus where a common
// term matches millions of documents the naive column costs minutes while the
// scorer column costs milliseconds. Above this many hits it is reported as
// "-" rather than measured; the BM25Scorer column is the one a caller would
// actually pay anyway.
constexpr size_t kNaiveScoreHitLimit = 100000;

} // namespace

int main(int argc, char **argv) {
  std::string corpus_path = "../../test/t_kjv.tsv";
  std::string queries_path;
  std::string splitter_path;
  std::string index_path;
  size_t text_field = 4;
  size_t repeat = 1;
  size_t runs = 20;
  auto text_ranges = TextRangeStorage::Store;

  auto need_value = [&](int i, const char *what) {
    if (i >= argc) {
      std::fprintf(stderr, "bench: %s needs a value\n", what);
      std::exit(1);
    }
    return std::string(argv[i]);
  };

  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if (arg == "--repeat") {
      repeat = std::stoul(need_value(++i, "--repeat"));
    } else if (arg == "--runs") {
      runs = std::stoul(need_value(++i, "--runs"));
    } else if (arg == "--corpus") {
      corpus_path = need_value(++i, "--corpus");
    } else if (arg == "--text-field") {
      text_field = std::stoul(need_value(++i, "--text-field"));
    } else if (arg == "--queries") {
      queries_path = need_value(++i, "--queries");
    } else if (arg == "--splitter") {
      splitter_path = need_value(++i, "--splitter");
    } else if (arg == "--index") {
      index_path = need_value(++i, "--index");
    } else if (arg == "--text-ranges") {
      auto mode = need_value(++i, "--text-ranges");
      if (mode == "skip") {
        text_ranges = TextRangeStorage::Skip;
      } else if (mode != "store") {
        std::fprintf(stderr, "bench: --text-ranges takes store or skip\n");
        return 1;
      }
    } else if (!arg.empty() && arg[0] == '-') {
      std::fprintf(stderr,
                   "usage: bench [--corpus PATH] [--text-field N] "
                   "[--repeat N] [--runs N]\n"
                   "             [--queries PATH] [--splitter MODEL_PATH]\n"
                   "             [--text-ranges store|skip] [--index PATH]\n");
      return 1;
    } else {
      corpus_path = arg;
    }
  }

  // A splitter is shared by both sides or by neither: indexing with one and
  // querying without it gives an index where 東京 is findable and 東京タワー
  // is not (see load_segmenting_splitter).
  TextSplitter splitter;
  if (!splitter_path.empty()) {
    splitter = load_segmenting_splitter(splitter_path);
  }

  auto texts = read_corpus(corpus_path, text_field, repeat);
  size_t corpus_bytes = 0;
  for (const auto &text : texts) {
    corpus_bytes += text.size();
  }

  auto queries = queries_path.empty()
                     ? std::vector<Query>(std::begin(kDefaultQueries),
                                          std::end(kDefaultQueries))
                     : read_queries(queries_path);

  auto before_build = resident_mib();
  InMemoryInvertedIndex<TextRange> invidx;
  {
    InMemoryIndexer indexer(invidx, normalizer, text_ranges);
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < texts.size(); i++) {
      if (splitter) {
        indexer.index_document(i, SplitterTokenizer(splitter, texts[i]));
      } else {
        indexer.index_document(i, UTF8PlainTextTokenizer(texts[i]));
      }
    }
    auto end = std::chrono::steady_clock::now();

    // Keys 0..n-1 were indexed in that order, so ordinal i is document i.
    size_t tokens = 0;
    for (size_t i = 0; i < texts.size(); i++) {
      tokens += invidx.document_term_count(i);
    }
    auto build_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
    auto build_mib = resident_mib() - before_build;

    std::printf("corpus     %s\n", corpus_path.c_str());
    std::printf("documents  %zu\n", texts.size());
    std::printf("text       %.1f MiB\n", corpus_bytes / 1048576.0);
    std::printf("tokens     %zu\n", tokens);
    std::printf("splitter   %s\n",
                splitter_path.empty() ? "utf8_plain_text" : splitter_path.c_str());
    std::printf("ranges     %s\n",
                text_ranges == TextRangeStorage::Skip ? "skip" : "store");
    std::printf("build      %.1f ms\n", build_ms);
    if (build_mib > 0 && tokens > 0) {
      std::printf("build RSS  %lld MiB (%.1f bytes/token)\n", build_mib,
                  build_mib * 1048576.0 / tokens);
    }
  }

  // The index the queries run against. With --index it is the one that came
  // back off disk, so the report describes a shipped corpus rather than the
  // in-process build. Held by shared_ptr because the read-only backend hands
  // one back and the in-memory index does not.
  std::shared_ptr<IInvertedIndexWithTextRange<TextRange>> loaded;
  if (!index_path.empty()) {
    auto save_start = std::chrono::steady_clock::now();
    invidx.save(index_path, {}, IndexFormat::Compressed);
    auto save_end = std::chrono::steady_clock::now();

    auto before_load = resident_mib();
    auto load_start = std::chrono::steady_clock::now();
    loaded = load_compressed_index(index_path);
    auto load_end = std::chrono::steady_clock::now();

    std::printf("save       %.1f ms\n", std::chrono::duration<double, std::milli>(
                                            save_end - save_start)
                                            .count());
    std::printf("index file %.1f MiB\n", file_size(index_path) / 1048576.0);
    std::printf("load       %.1f ms\n", std::chrono::duration<double, std::milli>(
                                            load_end - load_start)
                                            .count());
    // Only meaningful when it grew, and even then only as a lower bound: the
    // in-memory index and the corpus are both still alive here, so the
    // allocator satisfies much of this load out of pages it already holds.
    // Measured from a clean process the compressed backend wants about 1.4x
    // its file size; measured here it under-reports by several times. Read
    // this column as "at least", not as the footprint.
    auto load_mib = resident_mib() - before_load;
    if (load_mib > 0) {
      std::printf("load RSS   %lld MiB\n", load_mib);
    }
  }
  const IInvertedIndex &index =
      loaded ? static_cast<const IInvertedIndex &>(*loaded) : invidx;

  std::printf("\n%-24s %10s %10s %10s %10s %10s\n", "query", "hits",
              "search_us", "score_us", "scorer_us", "top10_us");

  for (const auto &query : queries) {
    auto parsed = splitter ? parse_query(splitter, to_term_filter(normalizer),
                                         query.text)
                           : parse_query(normalizer, query.text);
    if (!parsed) {
      std::printf("%-24s  PARSE FAILED\n", query.name.c_str());
      continue;
    }
    const auto &expr = *parsed;

    auto search_us =
        best_of(runs, [&] { auto r = perform_search(index, expr); });

    auto result = perform_search(index, expr);

    double score_us = -1.0;
    if (result->size() <= kNaiveScoreHitLimit) {
      score_us = best_of(runs, [&] {
        double total = 0.0;
        for (size_t i = 0; i < result->size(); i++) {
          total += bm25_score(index, expr, *result, i);
        }
        // Keep the loop from being optimized away entirely.
        if (total == -1.0) {
          std::printf(" ");
        }
      });
    }

    // The same scoring work through BM25Scorer, construction included, so
    // the column is a fair swap for score_us rather than a best case.
    auto scorer_us = best_of(runs, [&] {
      BM25Scorer scorer(index, expr);
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
      auto r = perform_search(index, expr);
      BM25Scorer scorer(index, expr);
      auto hits = top_k(*r, 10, [&](size_t i) { return scorer(*r, i); });
      if (hits.size() == 999) {
        std::printf(" ");
      }
    });

    std::printf("%-24s %10zu %10.2f ", query.name.c_str(), result->size(),
                search_us);
    if (score_us < 0.0) {
      std::printf("%10s ", "-");
    } else {
      std::printf("%10.2f ", score_us);
    }
    std::printf("%10.2f %10.2f\n", scorer_us, top10_us);
  }

  return 0;
}
