#include <iostream>

#include "lib/flags.h"

void usage() {
  std::cout << R"(usage: scope [options] <command> [<args>]

  commends:
    index        source INDEX_PATH   - index documents
    search       INDEX_PATH [query]  - search in documents

  options:
    -v           verbose output
)";
}

int error(int code) {
  usage();
  return code;
}

int main(int argc, char **argv) {
  const flags::args args(argc, argv);

  if (args.positional().size() < 2) {
    return error(1);
  }

  auto opt_verbose = args.get<bool>("v", false);

  auto cmd = args.positional().at(0);
  const std::string index_path{args.positional().at(1)};

  if (cmd == "index") {
  } else if (cmd == "search") {
  } else {
    return error(1);
  }

  return 0;
}
