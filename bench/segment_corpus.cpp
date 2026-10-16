//
//  segment_corpus.cpp
//
//  Copyright (c) 2026 Yuji Hirose. All rights reserved.
//  MIT License
//

// Corpus preparation for a cross-engine benchmark.
//
// Reads `id \t title \t author \t text` and writes the same shape with the
// text replaced by its terms, joined with single spaces. Two reasons:
//
//   - Another engine can then index exactly the same term sequence with a
//     whitespace tokenizer. Without this, a comparison against an engine
//     that segments Japanese with a different model measures the two
//     segmenters as much as the two search engines. Only a whitespace
//     tokenizer reads back the same terms (UTF8PlainTextTokenizer does not,
//     see bench/README.md), and only while no term contains whitespace,
//     which is checked below.
//   - With --chunk N each work is cut into N-term documents (the id becomes
//     "id:piece"; without --chunk the id is written as it came). Aozora Bunko
//     is a few thousand book-length works, and picking the top 10 out of a few
//     thousand documents does not exercise ranking; cutting them into passages
//     gives a document count that does, and matches what a reader would
//     actually want found.
//
// One command, wrapped here (no trailing backslashes: in a // comment they
// would splice the next line into it):
//
//   build/bench/searchlib-segment-corpus --in aozora.tsv
//     --out segmented.tsv --splitter test/models/ja-ud-gsd.mod --chunk 300
//
// Built with the bench by CMake, so that it compiles, warning-clean, on every
// CI run rather than only on the day a corpus is rebuilt.

#include <searchlib.h>
#include <searchlib_segment.h>

#include <algorithm>
#include <cstdio>
#include <exception>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "bench_options.h"
using namespace searchlib;

static std::vector<std::string> split_tsv(const std::string &in) {
  std::istringstream ss(in); std::string f; std::vector<std::string> r;
  while (std::getline(ss, f, '\t')) r.push_back(f);
  return r;
}

static void usage() {
  std::fprintf(
      stderr,
      "usage: searchlib-segment-corpus --in PATH --out PATH "
      "--splitter MODEL_PATH\n"
      "                                [--text-field N] [--chunk N]\n");
}

static int run(int argc, char **argv) {
  std::string in_path, out_path, model;
  size_t text_field = 3, chunk = 0;
  auto need_value = [&](int i, const char *what) {
    return bench_options::need_value(argc, argv, i, "segment_corpus", what);
  };
  auto need_count = [&](int i, const char *what) {
    return bench_options::parse_count(need_value(i, what), "segment_corpus",
                                      what);
  };
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--in") {
      in_path = need_value(++i, "--in");
    } else if (a == "--out") {
      out_path = need_value(++i, "--out");
    } else if (a == "--splitter") {
      model = need_value(++i, "--splitter");
    } else if (a == "--text-field") {
      text_field = need_count(++i, "--text-field");
    } else if (a == "--chunk") {
      chunk = need_count(++i, "--chunk");
    } else {
      usage();
      return 1;
    }
  }
  if (in_path.empty() || out_path.empty() || model.empty()) {
    usage();
    return 1;
  }

  auto splitter = load_segmenting_splitter(model);
  std::ifstream is(in_path);
  if (!is) {
    std::fprintf(stderr, "segment_corpus: cannot open %s\n", in_path.c_str());
    return 1;
  }
  std::ofstream os(out_path);
  if (!os) {
    std::fprintf(stderr, "segment_corpus: cannot open %s for writing\n",
                 out_path.c_str());
    return 1;
  }
  std::string line;
  size_t docs = 0, tokens = 0;
  while (std::getline(is, line)) {
    if (line.empty()) continue;
    auto f = split_tsv(line);
    if (f.size() <= text_field) continue;
    std::vector<std::string> terms;
    bool split_term = false;
    SplitterTokenizer tk(splitter, f[text_field]);
    tk(nullptr, [&](const std::u32string &s, size_t, TextRange) {
      // A whitespace tokenizer reading the output would cut such a term in
      // two (and a tab or newline would break the TSV line).
      split_term |= std::any_of(s.begin(), s.end(), [](char32_t cp) {
        return unicode::is_white_space(cp);
      });
      terms.push_back(searchlib::u8(s));
    });
    if (split_term) {
      std::fprintf(stderr,
                   "segment_corpus: %s: a term contains whitespace, so a "
                   "whitespace tokenizer would not read back the same terms\n",
                   f[0].c_str());
      return 1;
    }
    tokens += terms.size();
    size_t piece = 0;
    size_t step = chunk ? chunk : terms.size();
    if (step == 0) continue;
    for (size_t start = 0; start < terms.size(); start += step) {
      std::string joined;
      for (size_t j = start; j < std::min(start + step, terms.size()); j++) {
        if (j > start) joined += ' ';
        joined += terms[j];
      }
      if (joined.empty()) continue;
      os << f[0];
      if (chunk)
        os << ':' << piece++;
      os << '\t' << (f.size() > 1 ? f[1] : "") << '\t'
         << (f.size() > 2 ? f[2] : "") << '\t' << joined << '\n';
      docs++;
    }
  }
  // getline stops on a read error just as it does at the end of the file;
  // only the end is a finished corpus.
  if (is.bad()) {
    std::fprintf(stderr, "segment_corpus: error reading %s\n", in_path.c_str());
    return 1;
  }
  // A full disk shows up here, not at the first <<, which only buffers.
  os.close();
  if (!os) {
    std::fprintf(stderr, "segment_corpus: error writing %s\n",
                 out_path.c_str());
    return 1;
  }
  std::fprintf(stderr, "documents %zu, tokens %zu\n", docs, tokens);
  return 0;
}

// load_segmenting_splitter throws for a model it cannot load; reported here
// in one line instead of a bare "terminate called".
int main(int argc, char **argv) {
  try {
    return run(argc, argv);
  } catch (const std::exception &e) {
    std::fprintf(stderr, "segment_corpus: %s\n", e.what());
    return 1;
  }
}
