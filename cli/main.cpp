#include <searchlib.h>

#include "lib/args.h"
#include <unicodelib.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

using namespace searchlib;
namespace fs = std::filesystem;

namespace {

auto normalizer = [](auto sv) { return unicode::to_lowercase(sv); };

void usage() {
  std::cout << R"(usage: searchlib-cli [options] <command> [<args>]

  commands:
    index  SOURCE INDEX_PATH   - index a file or directory into INDEX_PATH
    search INDEX_PATH QUERY    - search INDEX_PATH for QUERY

  options:
    -n N, --limit N   limit results to N hits (search only, default 10)
    -v, --verbose     verbose output
    -h, --help        show this help
    --                end of options (put it before a query that starts
                      with '-', which is how a query writes NOT)
)";
}

// A mistake in how the command was given: say so and show how it is meant.
int usage_error(const std::string &message) {
  std::cerr << "error: " << message << std::endl;
  usage();
  return 1;
}

// A failure of the work itself, which the usage text would only bury.
int failure(const std::string &message) {
  std::cerr << "error: " << message << std::endl;
  return 1;
}

// Collects regular files under `source` (or just `source` itself if it's a
// file), sorted by path so that the index is the same across runs (its
// ordinals follow indexing order). `index_path` is left out: indexing into
// a directory it is already in would otherwise read last run's index file
// as a document.
std::vector<fs::path> collect_source_files(const fs::path &source,
                                           const fs::path &index_path) {
  auto skipped = fs::weakly_canonical(index_path);
  std::vector<fs::path> files;
  if (fs::is_regular_file(source)) {
    files.push_back(fs::absolute(source));
  } else if (fs::is_directory(source)) {
    for (const auto &entry : fs::recursive_directory_iterator(source)) {
      if (entry.is_regular_file() &&
          fs::weakly_canonical(entry.path()) != skipped) {
        files.push_back(fs::absolute(entry.path()));
      }
    }
  } else {
    throw std::runtime_error("no such file or directory: " + source.string());
  }
  std::sort(files.begin(), files.end());
  return files;
}

std::string read_file(const fs::path &path) {
  std::ifstream is(path, std::ios::binary);
  if (!is) {
    throw std::runtime_error("cannot open file: " + path.string());
  }
  std::ostringstream ss;
  ss << is.rdbuf();
  return ss.str();
}

int cmd_index(const std::string &source, const std::string &index_path,
             bool verbose) {
  auto files = collect_source_files(source, index_path);
  if (files.empty()) {
    return failure("no files found under: " + source);
  }

  // A file's path is its document key, so a hit comes back as the path it
  // was read from and nothing has to be kept beside the index file.
  InMemoryInvertedIndex<TextRange, std::string> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  for (const auto &path : files) {
    if (verbose) {
      std::cout << "indexing " << path.string() << std::endl;
    }
    auto content = read_file(path);
    UTF8PlainTextTokenizer tokenizer(content);
    indexer.index_document(path.string(), tokenizer);
  }

  invidx.save(index_path);

  std::cout << "indexed " << files.size() << " document(s) into "
            << index_path << std::endl;
  return 0;
}

int cmd_search(const std::string &index_path, const std::string &query_str,
               size_t limit, bool verbose) {
  InMemoryInvertedIndex<TextRange, std::string> invidx;
  invidx.load(index_path);

  auto parsed = parse_query(normalizer, query_str);
  if (!parsed) {
    return failure("invalid query: " + query_str);
  }

  const auto &expr = *parsed;

  auto result = perform_search(invidx, expr);
  if (verbose) {
    std::cout << result->size() << " matching document(s)" << std::endl;
  }

  // The scorer expands any Prefix/Wildcard/Fuzzy node against the dictionary
  // once, so scoring every hit below no longer re-walks it per hit -- which
  // is what expand_prefixes() used to be called up front to avoid.
  BM25Scorer scorer(invidx, expr);
  auto hits =
      top_k(*result, limit, [&](size_t i) { return scorer(*result, i); });

  size_t rank = 1;
  for (const auto &hit : hits) {
    const auto &path = invidx.document_key(result->document_ordinal(hit.index));

    std::cout << rank << ". " << path << "  score=" << hit.score
              << std::endl;

    // The source is read again to show the matches, and may have gone or
    // changed since it was indexed: that costs the snippets, not the hits
    // after this one.
    std::string content;
    try {
      content = read_file(path);
    } catch (const std::exception &e) {
      std::cout << "   (" << e.what() << ")" << std::endl;
    }
    auto hit_count = result->search_hit_count(hit.index);
    constexpr size_t kMaxShownHits = 3;
    for (size_t h = 0; h < std::min(hit_count, kMaxShownHits); h++) {
      auto rng = invidx.text_range(*result, hit.index, h);
      if (rng.position > content.size() ||
          rng.length > content.size() - rng.position) {
        continue;
      }
      std::cout << "   [" << rng.position << ':' << rng.length << "] \""
                << content.substr(rng.position, rng.length) << '"'
                << std::endl;
    }
    if (hit_count > kMaxShownHits) {
      std::cout << "   ... and " << (hit_count - kMaxShownHits)
                << " more hit(s)" << std::endl;
    }

    rank++;
  }

  return 0;
}

} // namespace

int main(int argc, char **argv) {
  cli::Args args;
  try {
    args = cli::parse_args(argc, argv);
  } catch (const std::invalid_argument &e) {
    return usage_error(e.what());
  }
  if (args.help) {
    usage();
    return 0;
  }
  if (args.positional.empty()) {
    return usage_error("missing command");
  }

  const auto &cmd = args.positional[0];
  const auto &rest = args.positional;

  try {
    if (cmd == "index") {
      if (rest.size() != 3) {
        return usage_error("usage: searchlib-cli index SOURCE INDEX_PATH");
      }
      return cmd_index(rest[1], rest[2], args.verbose);
    } else if (cmd == "search") {
      if (rest.size() != 3) {
        return usage_error("usage: searchlib-cli search INDEX_PATH QUERY");
      }
      return cmd_search(rest[1], rest[2], args.limit.value_or(10),
                        args.verbose);
    } else {
      return usage_error("unknown command: " + cmd);
    }
  } catch (const std::exception &e) {
    return failure(e.what());
  }
}
