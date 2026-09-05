#include <searchlib.h>

#include "lib/flags.h"
#include "unicodelib/unicodelib.h"

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
    -n N         limit results to N hits (search only, default 10)
    -v           verbose output
)";
}

int error(const std::string &message) {
  std::cerr << "error: " << message << std::endl;
  usage();
  return 1;
}

// Collects regular files under `source` (or just `source` itself if it's a
// file), sorted by path so that the index is the same across runs (its
// ordinals follow indexing order).
std::vector<fs::path> collect_source_files(const fs::path &source) {
  std::vector<fs::path> files;
  if (fs::is_regular_file(source)) {
    files.push_back(fs::absolute(source));
  } else if (fs::is_directory(source)) {
    for (const auto &entry : fs::recursive_directory_iterator(source)) {
      if (entry.is_regular_file()) {
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
  auto files = collect_source_files(source);
  if (files.empty()) {
    return error("no files found under: " + source);
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
    return error("invalid query: " + query_str);
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

    auto content = read_file(path);
    auto hit_count = result->search_hit_count(hit.index);
    constexpr size_t kMaxShownHits = 3;
    for (size_t h = 0; h < std::min(hit_count, kMaxShownHits); h++) {
      auto rng = invidx.text_range(*result, hit.index, h);
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
  const flags::args args(argc, argv);

  if (args.positional().size() < 1) {
    return error("missing command");
  }

  auto verbose = args.get<bool>("v", false);
  auto cmd = args.positional().at(0);

  try {
    if (cmd == "index") {
      auto source = args.get<std::string>(1);
      auto index_path = args.get<std::string>(2);
      if (!source || !index_path) {
        return error("usage: searchlib-cli index SOURCE INDEX_PATH");
      }
      return cmd_index(*source, *index_path, verbose);
    } else if (cmd == "search") {
      auto index_path = args.get<std::string>(1);
      auto query = args.get<std::string>(2);
      if (!index_path || !query) {
        return error("usage: searchlib-cli search INDEX_PATH QUERY");
      }
      auto limit = args.get<size_t>("n", size_t(10));
      return cmd_search(*index_path, *query, limit, verbose);
    } else {
      return error("unknown command: " + std::string(cmd));
    }
  } catch (const std::exception &e) {
    return error(e.what());
  }
}
