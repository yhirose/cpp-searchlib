#include <gtest/gtest.h>

#include <cli_args.h>

using cli::parse_args;

namespace {
cli::Args parse(std::vector<std::string_view> argv) { return parse_args(argv); }
} // namespace

TEST(CliArgsTest, CommandAndPositionals) {
  auto args = parse({"search", "idx", "apple pie"});
  EXPECT_EQ((std::vector<std::string>{"search", "idx", "apple pie"}),
            args.positional);
  EXPECT_FALSE(args.verbose);
  EXPECT_FALSE(args.limit);
  EXPECT_FALSE(args.help);
}

// The usage text says `[options] <command>`; -v used to swallow the next
// word as its value, so `-v index src idx` lost the command.
TEST(CliArgsTest, FlagsTakeNoValueAndMayComeAnywhere) {
  for (auto argv : std::vector<std::vector<std::string_view>>{
           {"-v", "index", "src", "idx"},
           {"index", "-v", "src", "idx"},
           {"index", "src", "idx", "-v"},
           {"--verbose", "index", "src", "idx"}}) {
    auto args = parse(argv);
    EXPECT_TRUE(args.verbose);
    EXPECT_EQ((std::vector<std::string>{"index", "src", "idx"}),
              args.positional);
  }
}

TEST(CliArgsTest, LimitTakesTheNextWordOrAnEquals) {
  for (auto argv : std::vector<std::vector<std::string_view>>{
           {"search", "idx", "q", "-n", "25"},
           {"-n", "25", "search", "idx", "q"},
           {"search", "idx", "q", "-n=25"},
           {"search", "idx", "q", "--limit", "25"},
           {"search", "idx", "q", "--limit=25"}}) {
    auto args = parse(argv);
    ASSERT_TRUE(args.limit);
    EXPECT_EQ(25u, *args.limit);
    EXPECT_EQ((std::vector<std::string>{"search", "idx", "q"}),
              args.positional);
  }
}

// -n abc and -n -5 used to fall back to 10 quietly, and -n=-5 wrapped
// around to 18446744073709551611.
TEST(CliArgsTest, BadLimitIsAnError) {
  for (auto bad : {"abc", "-5", "0", "", "5x", "99999999999999999999999"}) {
    EXPECT_THROW(parse({"search", "idx", "q", "-n", bad}),
                 std::invalid_argument)
        << bad;
    EXPECT_THROW(parse({"-n=" + std::string(bad)}), std::invalid_argument)
        << bad;
  }
  EXPECT_THROW(parse({"search", "-n"}), std::invalid_argument);
}

// A query's NOT starts with '-', which is an option to the parser unless
// `--` says otherwise.
TEST(CliArgsTest, DoubleDashEndsOptions) {
  auto args = parse({"search", "idx", "--", "-draft"});
  EXPECT_EQ((std::vector<std::string>{"search", "idx", "-draft"}),
            args.positional);
  EXPECT_FALSE(args.verbose);

  // After it, even an option's own spelling is a plain word.
  args = parse({"-v", "search", "idx", "--", "-v", "-n", "--"});
  EXPECT_TRUE(args.verbose);
  EXPECT_EQ((std::vector<std::string>{"search", "idx", "-v", "-n", "--"}),
            args.positional);

  EXPECT_THROW(parse({"search", "idx", "-draft"}), std::invalid_argument);
}

// An empty argument and a lone '-' used to crash the parser.
TEST(CliArgsTest, EmptyAndLoneDashArePositional) {
  auto args = parse({"search", "idx", "", "-"});
  EXPECT_EQ((std::vector<std::string>{"search", "idx", "", "-"}),
            args.positional);
}

TEST(CliArgsTest, UnknownOptionAndHelp) {
  EXPECT_THROW(parse({"--nope"}), std::invalid_argument);
  EXPECT_TRUE(parse({"-h"}).help);
  EXPECT_TRUE(parse({"index", "--help"}).help);
}
