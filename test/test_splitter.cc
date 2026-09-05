#include <gtest/gtest.h>
#include <searchlib.h>

#include <chrono>

#include "test_utils.h"

using namespace searchlib;

namespace {

struct Word {
  std::string str;
  TextRange range;
};

std::vector<Word> split(const TextSplitter &splitter, std::string_view text) {
  std::vector<Word> words;
  splitter(text, [&](const std::u32string &str, TextRange range) {
    words.push_back({u8(str), range});
  });
  return words;
}

std::vector<std::string> terms(const std::vector<Word> &words) {
  std::vector<std::string> out;
  for (const auto &word : words) {
    out.push_back(word.str);
  }
  return out;
}

std::vector<std::string> terms(const TextSplitter &splitter,
                               std::string_view text) {
  return terms(split(splitter, text));
}

// Every range spans exactly the bytes of its term, and the starts never go
// back (a segmenter's words may stack or overlap; the default rule's are
// disjoint, which RangesAreTheBytesOfTheTerm pins on its own).
void expect_ranges_exact(std::string_view text, const std::vector<Word> &words) {
  size_t previous_start = 0;
  for (const auto &word : words) {
    EXPECT_GE(word.range.position, previous_start) << word.str;
    EXPECT_EQ(word.str, text.substr(word.range.position, word.range.length));
    previous_start = word.range.position;
  }
}

// The (term, position) pairs SplitterTokenizer hands an index.
std::vector<std::pair<std::string, size_t>>
positioned(const TextSplitter &splitter, std::string_view text) {
  std::vector<std::pair<std::string, size_t>> out;
  SplitterTokenizer tokenizer(splitter, text);
  tokenizer(nullptr, [&](const std::u32string &str, size_t term_pos,
                         TextRange) { out.emplace_back(u8(str), term_pos); });
  return out;
}

using Positioned = std::vector<std::pair<std::string, size_t>>;

using Strings = std::vector<std::string>;

} // namespace

//-----------------------------------------------------------------------------
// The default splitter: UAX #29 word segments containing a letter or a number
//-----------------------------------------------------------------------------

TEST(SplitterTest, KeepsTheSegmentsUax29CallsWords) {
  auto splitter = utf8_plain_text_splitter();

  // Letters and numbers, with the punctuation UAX #29 keeps inside a word
  // (WB6/7 apostrophes and periods between letters, WB11/12 separators
  // between digits); everything between words is dropped.
  EXPECT_EQ((Strings{"version", "2.0"}), terms(splitter, "version 2.0"));
  EXPECT_EQ((Strings{"don't", "U.S.A"}), terms(splitter, "don't U.S.A."));
  EXPECT_EQ((Strings{"1,234.56"}), terms(splitter, "1,234.56"));
  EXPECT_EQ((Strings{"well", "known"}), terms(splitter, "well-known!"));
  EXPECT_EQ((Strings{}), terms(splitter, " ... !? -- "));
  EXPECT_EQ((Strings{}), terms(splitter, ""));

  // Emoji and other symbols are segments but not words.
  EXPECT_EQ((Strings{"ok"}), terms(splitter, "😀 ok 🇺🇸"));
}

TEST(SplitterTest, ScriptsWrittenWithSpacesComeOutAsWords) {
  auto splitter = utf8_plain_text_splitter();

  EXPECT_EQ((Strings{"Русский", "язык"}), terms(splitter, "Русский язык"));
  EXPECT_EQ((Strings{"اللغة", "العربية"}), terms(splitter, "اللغة العربية"));
  EXPECT_EQ((Strings{"한국어", "문장"}), terms(splitter, "한국어 문장."));
}

TEST(SplitterTest, ScriptsWrittenWithoutSpacesComeOutOneScalarAtATime) {
  // This is where UAX #29 itself defers to a dictionary: Han and Hiragana are
  // one scalar per segment, Thai too, while Katakana runs are joined (WB13).
  // utf8_plain_text_splitter(segmenter, scripts) is the hook for the
  // dictionary; without one, this is the unigram baseline.
  auto splitter = utf8_plain_text_splitter();

  EXPECT_EQ((Strings{"東", "京", "タワー", "に", "行", "っ", "た"}),
            terms(splitter, "東京タワーに行った"));
  EXPECT_EQ((Strings{"Tokyo", "東", "京", "tower"}),
            terms(splitter, "Tokyo東京tower"));
  EXPECT_EQ((Strings{"ภ", "า", "ษ", "า", "ไ", "ท", "ย"}),
            terms(splitter, "ภาษาไทย"));
}

TEST(SplitterTest, RangesAreTheBytesOfTheTerm) {
  auto splitter = utf8_plain_text_splitter();
  for (std::string_view text :
       {"version 2.0, don't U.S.A. 1,234.56", "Tokyo東京tower タワー",
        "Русский язык 한국어", "ภาษาไทย"}) {
    auto words = split(splitter, text);
    ASSERT_FALSE(words.empty()) << text;
    expect_ranges_exact(text, words);
  }
}

TEST(SplitterTest, IllFormedBytesSeparateWordsAndNeverJoinThem) {
  // An undecodable byte becomes U+FFFD in the walk: never a term itself, a
  // boundary between its neighbours, and one byte wide so that the ranges
  // around it stay exact and no term spans bytes that were never decoded.
  auto splitter = utf8_plain_text_splitter();
  const std::vector<std::pair<const char *, std::string>> ill_formed = {
      {"truncated", "\xE3"},
      {"bad continuation", "\xE3\x81\x21"},
      {"overlong U+0000", "\xC0\x80"},
      {"surrogate U+D800", "\xED\xA0\x80"},
      {"past U+10FFFF", "\xF7\xBF\xBF\xBF"},
      {"invalid lead bytes", "\xFF\xFE"},
  };
  for (const auto &[what, bytes] : ill_formed) {
    auto text = "ab" + bytes + "cd";
    auto words = split(splitter, text);
    ASSERT_EQ(2u, words.size()) << what;
    EXPECT_EQ("ab", words[0].str) << what;
    EXPECT_EQ("cd", words[1].str) << what;
    expect_ranges_exact(text, words);
  }
}

TEST(SplitterTest, TheQuerySideSplitsTheSameWay) {
  // parse_query with no splitter uses the same default, so a term the index
  // holds as `2.0` is what the query asks for.
  auto expr = parse_query(TermFilter(nullptr), "2.0");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Term, expr->operation);
  EXPECT_EQ(U"2.0", expr->term_str);

  expr = parse_query(TermFilter(nullptr), "don't");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Term, expr->operation);
  EXPECT_EQ(U"don't", expr->term_str);

  // A token UAX #29 cuts is an implicit phrase, as `well-known` always was.
  expr = parse_query(TermFilter(nullptr), "東京");
  ASSERT_TRUE(expr);
  EXPECT_EQ(Operation::Adjacent, expr->operation);
  ASSERT_EQ(2u, expr->nodes.size());
  EXPECT_EQ(U"東", expr->nodes[0].term_str);
  EXPECT_EQ(U"京", expr->nodes[1].term_str);
}

TEST(SplitterTest, StaysLinearOverARunOfFlags) {
  // The walk unicodelib's segment_length describes: a per-position one would
  // take minutes here, and both documents and queries come from outside.
  std::string text;
  for (size_t i = 0; i < 200000; i++) {
    text += "🇺🇸";
  }

  auto start = std::chrono::steady_clock::now();
  auto words = split(utf8_plain_text_splitter(), text);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(words.empty());
  EXPECT_LT(std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(),
            2);
}

//-----------------------------------------------------------------------------
// Delegation to a Segmenter, and what the caller does with its answer
//-----------------------------------------------------------------------------

namespace {

// A segmenter scripted by the test: every call is recorded, and `answer`
// decides what it emits and what it says it consumed.
struct Scripted {
  struct Call {
    size_t offset;
    std::string rest;
  };
  std::vector<Call> calls;
  Segmenter answer;

  Segmenter segmenter() {
    return [this](std::string_view text, size_t offset, const SplitEmit &emit) {
      calls.push_back({offset, std::string(text.substr(offset))});
      return answer(text, offset, emit);
    };
  }
};

// Emits `words` (each a byte range relative to `offset`) and reports
// `consumed`.
Segmenter answer_with(std::vector<std::pair<size_t, size_t>> words,
                      size_t consumed) {
  return [=](std::string_view text, size_t offset, const SplitEmit &emit) {
    for (auto [from, to] : words) {
      auto range = TextRange{offset + from, to - from};
      emit(u32(text.substr(range.position, range.length)), range);
    }
    return consumed;
  };
}

bool kHan(char32_t cp) { return unicode::script(cp) == unicode::Script::Han; }

} // namespace

TEST(SplitterDelegationTest, ClaimedSegmentsGoToTheSegmenterWithTheWholeText) {
  // 東京 is two UAX #29 segments; the segmenter is called at the first, sees
  // the whole text from there, consumes both and the walk resumes after them.
  // タワー starts with Katakana, which is not claimed, so it is cut by the
  // default rule; the Latin around it too.
  Scripted scripted;
  scripted.answer = answer_with({{0, 6}}, 6); // 東京 as one word
  auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

  std::string text = "Tokyo東京タワーtower";
  auto words = split(splitter, text);

  EXPECT_EQ((Strings{"Tokyo", "東京", "タワー", "tower"}), terms(words));
  expect_ranges_exact(text, words);
  ASSERT_EQ(1u, scripted.calls.size());
  EXPECT_EQ(5u, scripted.calls[0].offset);
  EXPECT_EQ("東京タワーtower", scripted.calls[0].rest);
}

TEST(SplitterDelegationTest, ASegmenterMayConsumePastTheScriptItClaimed) {
  // How far a span reaches is the segmenter's call: consuming into the Latin
  // that follows is valid (the span ends on a grapheme boundary), and the
  // words it emits there are kept.
  Scripted scripted;
  scripted.answer = answer_with({{0, 6}, {6, 11}}, 11); // 東京 + tower
  auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

  EXPECT_EQ((Strings{"東京", "tower", "x"}), terms(splitter, "東京tower x"));
}

TEST(SplitterDelegationTest, AWholeTextSplitterFitsTheSameShape) {
  // Claim the script the text starts with and consume to the end: the
  // segmenter is then the splitter, and the default rule never runs.
  Scripted scripted;
  scripted.answer = [](std::string_view text, size_t offset, const SplitEmit &emit) {
    emit(U"everything", TextRange{offset, text.size() - offset});
    return text.size() - offset;
  };
  auto splitter = utf8_plain_text_splitter(scripted.segmenter(), [](char32_t cp) {
    return unicode::script(cp) == unicode::Script::Latin;
  });

  EXPECT_EQ((Strings{"everything"}), terms(splitter, "any text 2.0 東京"));
  EXPECT_EQ(1u, scripted.calls.size());
}

TEST(SplitterDelegationTest, ABadSpanIsDroppedAndTheWalkSkipsOneGraphemeCluster) {
  // consumed == 0, past the end, not on a scalar boundary, or inside a
  // grapheme cluster: the span's words are all dropped, and the segmenter is
  // asked again one grapheme cluster on.
  struct Case {
    const char *what;
    std::string text;
    size_t consumed;
    Strings expected;
    std::vector<size_t> offsets_asked;
  };
  // 東 + U+3099 (combining voiced sound mark) is one grapheme cluster of 6
  // bytes; a segmenter that consumes only the 東 cut it in half.
  const std::string voiced = "東\xE3\x82\x99";

  for (const auto &c : std::vector<Case>{
           {"zero", "東京", 0, {}, {0, 3}},
           {"past the end", "東京 x", 100, {"x"}, {0, 3}},
           {"mid-scalar", "東京 x", 1, {"x"}, {0, 3}},
           {"mid-cluster", voiced + " x", 3, {"x"}, {0}},
       }) {
    Scripted scripted;
    scripted.answer = answer_with({{0, 3}}, c.consumed);
    auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

    EXPECT_EQ(c.expected, terms(splitter, c.text)) << c.what;
    std::vector<size_t> asked;
    for (const auto &call : scripted.calls) {
      asked.push_back(call.offset);
    }
    EXPECT_EQ(c.offsets_asked, asked) << c.what;
  }
}

TEST(SplitterDelegationTest, ABadWordIsDroppedOnItsOwn) {
  // Within a valid span, each word is checked by itself: outside the span,
  // empty, starting before the word before it, or cut off a grapheme cluster
  // boundary -- the others stay. Starting where the previous word started
  // (a stack) or inside it (a later start) is not a fault.
  const std::string text = "東京都庁 x";
  const std::string voiced = "東\xE3\x82\x99京 x";
  struct Case {
    const char *what;
    std::string text;
    std::vector<std::pair<size_t, size_t>> words;
    Strings expected;
  };
  for (const auto &c : std::vector<Case>{
           {"all good", text, {{0, 6}, {6, 12}}, {"東京", "都庁", "x"}},
           {"outside the span", text, {{0, 6}, {6, 12}, {13, 14}}, {"東京", "都庁", "x"}},
           {"empty", text, {{0, 6}, {6, 6}, {6, 12}}, {"東京", "都庁", "x"}},
           {"a later start", text, {{0, 6}, {3, 9}, {9, 12}}, {"東京", "京都", "庁", "x"}},
           {"the same start", text, {{0, 6}, {0, 3}, {6, 12}}, {"東京", "東", "都庁", "x"}},
           {"out of order", text, {{6, 12}, {0, 6}}, {"都庁", "x"}},
           {"mid-scalar", text, {{0, 4}, {6, 12}}, {"都庁", "x"}},
           {"mid-cluster", voiced, {{0, 3}, {6, 9}}, {"京", "x"}},
       }) {
    Scripted scripted;
    // The span is the whole run of Han (12 bytes in `text`, 9 in `voiced`).
    auto span = c.text.find(' ');
    scripted.answer = answer_with(c.words, span);
    auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

    auto words = split(splitter, c.text);
    EXPECT_EQ(c.expected, terms(words)) << c.what;
    expect_ranges_exact(c.text, words);
  }
}

TEST(SplitterDelegationTest, WordsStartingAtOneByteShareATermPosition) {
  // TextSplitter's stacking rule as SplitterTokenizer applies it: a word
  // starting where the previous one started is an alternative at that
  // position (a compound beside its parts), and any later start -- inside
  // the previous word or after it -- is the next position.
  Scripted scripted;
  scripted.answer = answer_with({{0, 12}, {0, 6}, {6, 12}}, 12);
  auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

  EXPECT_EQ((Positioned{{"東京都庁", 0}, {"東京", 0}, {"都庁", 1}, {"x", 2}}),
            positioned(splitter, "東京都庁 x"));
}

TEST(SplitterDelegationTest, ASegmenterMayCallTheDefaultSplitterBack) {
  // A segmenter that hands a span it does not handle back to the default
  // splitter re-enters the walk from inside it. The outer walk must come out
  // unaffected (the per-thread scratch is leased, not shared).
  Scripted scripted;
  scripted.answer = [](std::string_view text, size_t offset,
                       const SplitEmit &emit) {
    auto span = text.substr(offset, 6); // 東京, two scalars
    utf8_plain_text_splitter()(span, [&](const std::u32string &str,
                                         TextRange range) {
      emit(str, TextRange{offset + range.position, range.length});
    });
    return span.size();
  };
  auto splitter = utf8_plain_text_splitter(scripted.segmenter(), kHan);

  std::string text = "Tokyo東京タワーtower 2.0";
  auto words = split(splitter, text);
  EXPECT_EQ((Strings{"Tokyo", "東", "京", "タワー", "tower", "2.0"}), terms(words));
  expect_ranges_exact(text, words);
}

TEST(SplitterDelegationTest, ASegmenterAndItsClaimAreRequired) {
  EXPECT_THROW(utf8_plain_text_splitter(nullptr, kHan), std::invalid_argument);
  EXPECT_THROW(utf8_plain_text_splitter(answer_with({}, 1), nullptr),
               std::invalid_argument);
}
