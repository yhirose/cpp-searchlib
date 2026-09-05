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
//     segmenters as much as the two search engines. cpp-searchlib reading
//     the output back through UTF8PlainTextTokenizer reproduces the term
//     count exactly, which is the check that the round trip is lossless.
//   - With --chunk N each work is cut into N-term documents (the id becomes
//     "id:piece"). Aozora Bunko is a few thousand book-length works, and
//     picking the top 10 out of a few thousand documents does not exercise
//     ranking; cutting them into passages gives a document count that does,
//     and matches what a reader would actually want found.
//
//   segment_corpus --in aozora.tsv --out segmented.tsv \
//                  --splitter test/models/ja-ud-gsd.mod --chunk 300
//
// Not built by CMake: it is a one-off corpus tool, not part of the library
// or its tests. Build it beside the bench target when you need it:
//
//   clang++ -O2 -DNDEBUG -std=c++17 -I include -isystem third_party \
//     -isystem third_party/fstlib -isystem third_party/peglib \
//     -isystem third_party/unicodelib \
//     bench/segment_corpus.cpp -o /tmp/segment_corpus

#include <searchlib.h>
#include <searchlib_segment.h>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
using namespace searchlib;

static std::vector<std::string> split_tsv(const std::string &in) {
  std::istringstream ss(in); std::string f; std::vector<std::string> r;
  while (std::getline(ss, f, '\t')) r.push_back(f);
  return r;
}

int main(int argc, char **argv) {
  std::string in_path, out_path, model;
  size_t text_field = 3, chunk = 0;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--in") in_path = argv[++i];
    else if (a == "--out") out_path = argv[++i];
    else if (a == "--splitter") model = argv[++i];
    else if (a == "--text-field") text_field = std::stoul(argv[++i]);
    else if (a == "--chunk") chunk = std::stoul(argv[++i]);
  }
  auto splitter = load_segmenting_splitter(model);
  std::ifstream is(in_path);
  std::ofstream os(out_path);
  std::string line;
  size_t docs = 0, tokens = 0;
  while (std::getline(is, line)) {
    if (line.empty()) continue;
    auto f = split_tsv(line);
    if (f.size() <= text_field) continue;
    std::vector<std::string> terms;
    SplitterTokenizer tk(splitter, f[text_field]);
    tk(nullptr, [&](const std::u32string &s, size_t, TextRange) {
      terms.push_back(searchlib::u8(s));
    });
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
      os << f[0] << ':' << piece++ << '\t' << (f.size() > 1 ? f[1] : "")
         << '\t' << (f.size() > 2 ? f[2] : "") << '\t' << joined << '\n';
      docs++;
    }
  }
  std::fprintf(stderr, "documents %zu, tokens %zu\n", docs, tokens);
  return 0;
}
