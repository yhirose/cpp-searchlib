#ifndef SEARCHLIB_CLI_ARGS_H_
#define SEARCHLIB_CLI_ARGS_H_

#include <charconv>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace cli {

struct Args {
  bool help = false;
  bool verbose = false;
  std::optional<size_t> limit; // -n
  std::vector<std::string> positional;
};

// The command line, in the one shape the CLI has: a command and its
// arguments, with a few options allowed anywhere among them.
//
//   -h, --help          show usage
//   -v, --verbose       verbose output
//   -n N, --limit N     limit results to N hits (also -n=N, --limit=N)
//   --                  everything after it is positional
//
// Options are told from positionals by their leading '-', which is also what
// a query's NOT looks like (`-draft`); `--` ahead of such a query settles it.
// An empty argument, and a lone '-', are positionals. Anything else starting
// with '-' that is not listed above is an error, rather than a value that
// quietly lands in the wrong place.
//
// Throws std::invalid_argument for a bad command line.
inline Args parse_args(const std::vector<std::string_view> &argv) {
  Args args;
  bool options_done = false;

  auto parse_limit = [](std::string_view text) {
    size_t value = 0;
    auto [end, ec] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (text.empty() || ec != std::errc() || end != text.data() + text.size() ||
        value == 0) {
      throw std::invalid_argument("-n takes a positive number, got: '" +
                                  std::string(text) + "'");
    }
    return value;
  };

  for (size_t i = 0; i < argv.size(); i++) {
    auto arg = argv[i];
    if (options_done || arg.size() < 2 || arg[0] != '-') {
      args.positional.emplace_back(arg);
      continue;
    }
    if (arg == "--") {
      options_done = true;
    } else if (arg == "-v" || arg == "--verbose") {
      args.verbose = true;
    } else if (arg == "-h" || arg == "--help") {
      args.help = true;
    } else if (arg == "-n" || arg == "--limit") {
      if (i + 1 >= argv.size()) {
        throw std::invalid_argument(std::string(arg) + " needs a value");
      }
      args.limit = parse_limit(argv[++i]);
    } else if (arg.rfind("-n=", 0) == 0) {
      args.limit = parse_limit(arg.substr(3));
    } else if (arg.rfind("--limit=", 0) == 0) {
      args.limit = parse_limit(arg.substr(8));
    } else {
      throw std::invalid_argument(
          "unknown option: " + std::string(arg) +
          " (put -- before a query that starts with '-')");
    }
  }
  return args;
}

inline Args parse_args(int argc, char **argv) {
  return parse_args(std::vector<std::string_view>(argv + 1, argv + argc));
}

} // namespace cli

#endif // SEARCHLIB_CLI_ARGS_H_
