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
//     segmenters as much as the two search engines. The round trip is
//     lossless only for a reader that splits on whitespace and nowhere else,
//     and only while no term has whitespace inside it -- so that is checked
//     below, and a term that would come back as two stops the run. It is not
//     lossless through UTF8PlainTextTokenizer: that one cuts Han text into
//     single characters, so `東京 タワー に 行っ た` (five terms) reads back
//     as seven.
//   - With --chunk N each work is cut into N-term documents (the id becomes
//     "id:piece"; without --chunk the id is written as it came). Aozora Bunko
//     is a few thousand book-length works, and picking the top 10 out of a few
//     thousand documents does not exercise ranking; cutting them into passages
//     gives a document count that does, and matches what a reader would
//     actually want found.
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
#include <cstdlib>
#include <exception>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
using namespace searchlib;

static std::vector<std::string> split_tsv(const std::string &in) {
  std::istringstream ss(in); std::string f; std::vector<std::string> r;
  while (std::getline(ss, f, '\t')) r.push_back(f);
  return r;
}

static void usage() {
  std::fprintf(
      stderr,
      "usage: segment_corpus --in PATH --out PATH --splitter MODEL_PATH\n"
      "                      [--text-field N] [--chunk N]\n");
}

static std::string need_value(int argc, char **argv, int i, const char *what) {
  if (i >= argc) {
    std::fprintf(stderr, "segment_corpus: %s needs a value\n", what);
    std::exit(1);
  }
  return argv[i];
}

// The value of a counting option, or a message naming the option and exit
// status 1 -- std::stoul alone throws on "abc", reads "12x" as 12 and wraps
// "-1" round to the largest size_t.
static size_t parse_count(const std::string &value, const char *what) {
  auto ok = !value.empty() &&
            value.find_first_not_of("0123456789") == std::string::npos;
  unsigned long long n = 0;
  if (ok) {
    try {
      n = std::stoull(value);
    } catch (const std::out_of_range &) {
      ok = false;
    }
  }
  if (!ok) {
    std::fprintf(stderr,
                 "segment_corpus: %s takes a non-negative integer, got '%s'\n",
                 what, value.c_str());
    std::exit(1);
  }
  return static_cast<size_t>(n);
}

// Whether a whitespace tokenizer reading the output would cut `term` in two
// (or a tab or newline in it would break the TSV line).
static bool has_white_space(const std::u32string &term) {
  for (auto cp : term) {
    if (unicode::is_white_space(cp)) {
      return true;
    }
  }
  return false;
}

static int run(int argc, char **argv) {
  std::string in_path, out_path, model;
  size_t text_field = 3, chunk = 0;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--in")
      in_path = need_value(argc, argv, ++i, "--in");
    else if (a == "--out")
      out_path = need_value(argc, argv, ++i, "--out");
    else if (a == "--splitter")
      model = need_value(argc, argv, ++i, "--splitter");
    else if (a == "--text-field")
      text_field =
          parse_count(need_value(argc, argv, ++i, a.c_str()), "--text-field");
    else if (a == "--chunk")
      chunk = parse_count(need_value(argc, argv, ++i, a.c_str()), "--chunk");
    else {
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
      split_term = split_term || has_white_space(s);
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

// load_segmenting_splitter throws for a model it cannot load; caught here so
// that is one line saying so, not a bare "terminate called".
int main(int argc, char **argv) {
  try {
    return run(argc, argv);
  } catch (const std::exception &e) {
    std::fprintf(stderr, "segment_corpus: %s\n", e.what());
    return 1;
  }
}
