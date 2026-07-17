#include <searchlib.h>

#include "lib/flags.h"
#include "lib/unicodelib.h"

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

std::string manifest_path(const std::string &index_path) {
  return index_path + ".manifest";
}

// Collects regular files under `source` (or just `source` itself if it's a
// file), sorted by path so that document ids are deterministic across runs.
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

  InMemoryInvertedIndex<TextRange> invidx;
  InMemoryIndexer indexer(invidx, normalizer);

  size_t document_id = 0;
  for (const auto &path : files) {
    if (verbose) {
      std::cout << "indexing [" << document_id << "] " << path.string()
                << std::endl;
    }
    auto content = read_file(path);
    UTF8PlainTextTokenizer tokenizer(content);
    indexer.index_document(document_id, tokenizer);
    document_id++;
  }

  invidx.save(index_path);

  std::ofstream manifest(manifest_path(index_path), std::ios::binary);
  if (!manifest) {
    return error("cannot write manifest file for: " + index_path);
  }
  for (const auto &path : files) {
    manifest << path.string() << '\n';
  }

  std::cout << "indexed " << files.size() << " document(s) into "
            << index_path << std::endl;
  return 0;
}

std::vector<std::string> load_manifest(const std::string &index_path) {
  std::ifstream is(manifest_path(index_path));
  if (!is) {
    throw std::runtime_error("cannot open manifest file for: " + index_path +
                             " (was it indexed with this CLI?)");
  }
  std::vector<std::string> paths;
  std::string line;
  while (std::getline(is, line)) {
    if (!line.empty()) {
      paths.push_back(line);
    }
  }
  return paths;
}

int cmd_search(const std::string &index_path, const std::string &query_str,
               size_t limit, bool verbose) {
  InMemoryInvertedIndex<TextRange> invidx;
  invidx.load(index_path);
  auto document_paths = load_manifest(index_path);

  auto expr = parse_query(normalizer, query_str);
  if (!expr) {
    return error("invalid query: " + query_str);
  }

  auto result = perform_search(invidx, *expr);
  if (verbose) {
    std::cout << result->size() << " matching document(s)" << std::endl;
  }

  auto hits = top_k(*result, limit, [&](size_t i) {
    return bm25_score(invidx, *expr, *result, i);
  });

  size_t rank = 1;
  for (const auto &hit : hits) {
    auto document_id = result->document_id(hit.index);
    const auto &path = document_id < document_paths.size()
                           ? document_paths[document_id]
                           : "<unknown>";

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
