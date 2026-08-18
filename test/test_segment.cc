#include <gtest/gtest.h>
#include <searchlib.h>
#include <searchlib_segment.h>

#include <sstream>

#include "test_utils.h"

using namespace searchlib;

namespace {

// Same convention as test_kjv.cc's KJV_PATH: relative to the build directory
// the tests run from. The model is CC BY-SA 4.0, not MIT like the rest of the
// tree -- see test/models/NOTICE.
const auto MODEL_PATH = "../../test/models/ja-ud-gsd.mod";

// Loaded once: it is a 2 MB model, and Segmenter is immutable and
// thread-safe, so every test can share one splitter.
const TextSplitter &segmenting_splitter() {
  static TextSplitter splitter = load_segmenting_splitter(MODEL_PATH);
  return splitter;
}

struct Token {
  std::string str;
  size_t term_pos;
  TextRange range;
};

std::vector<Token> tokenize(TextSplitter splitter, std::string_view text,
                            Normalizer normalizer = nullptr) {
  std::vector<Token> tokens;
  SplitterTokenizer tokenizer(std::move(splitter), text);
  tokenizer(std::move(normalizer),
            [&](const std::u32string &str, size_t term_pos, TextRange range) {
              tokens.push_back({u8(str), term_pos, range});
            });
  return tokens;
}

std::vector<std::string> terms(const std::vector<Token> &tokens) {
  std::vector<std::string> out;
  for (const auto &token : tokens) {
    out.push_back(token.str);
  }
  return out;
}

// Renders an Expression as a compact s-expression, so a test can pin the
// shape of a parsed query (Adjacent vs Or vs Term) in one string compare.
std::string to_string(const Expression &expr) {
  auto children = [&](const char *name) {
    std::string s = std::string("(") + name;
    for (const auto &node : expr.nodes) {
      s += " " + to_string(node);
    }
    return s + ")";
  };
  switch (expr.operation) {
  case Operation::Term:
    return u8(expr.term_str);
  case Operation::Prefix:
    return u8(expr.term_str) + "*";
  case Operation::Wildcard:
    return "[" + u8(expr.term_str) + "]";
  case Operation::Fuzzy:
    return u8(expr.term_str) + "~" +
           std::to_string(expr.near_operation_distance);
  case Operation::And:
    return children("and");
  case Operation::Or:
    return children("or");
  case Operation::Adjacent:
    return children("adj");
  case Operation::Near:
    return children("near");
  case Operation::Not:
    return children("not");
  default:
    return children("?");
  }
}

std::string parsed(TextSplitter splitter, const std::string &query,
                   TermFilter filter = nullptr) {
  auto expr = parse_query(std::move(splitter), std::move(filter), query);
  if (!expr) {
    return "<parse error>";
  }
  return to_string(*expr);
}

std::vector<size_t> search_ids(const IInvertedIndex &index,
                               const Expression &expr) {
  std::vector<size_t> ids;
  auto postings = perform_search(index, expr);
  for (size_t i = 0; i < postings->size(); i++) {
    ids.push_back(postings->document_id(i));
  }
  return ids;
}

} // namespace

//-----------------------------------------------------------------------------
// The splitter itself
//-----------------------------------------------------------------------------

TEST(SegmentTest, SegmentsJapaneseIntoWords) {
  auto tokens = tokenize(segmenting_splitter(), "私は東京タワーに行った");

  EXPECT_EQ((std::vector<std::string>{"私", "は", "東京", "タワー", "に", "行っ",
                                      "た"}),
            terms(tokens));
}

TEST(SegmentTest, TokenPositionsAndRangesAreConsistent) {
  // The three invariants InMemoryIndexer and the text-range machinery depend
  // on: term_pos is a dense 0,1,2,... sequence (it doubles as the index into
  // the per-document text-range array), ranges advance monotonically, and each
  // range actually spans the bytes of its term.
  std::string text = "私は東京タワーに行った。The quick brown fox。";
  auto tokens = tokenize(segmenting_splitter(), text);

  ASSERT_FALSE(tokens.empty());
  size_t previous_end = 0;
  for (size_t i = 0; i < tokens.size(); i++) {
    EXPECT_EQ(i, tokens[i].term_pos);
    EXPECT_GE(tokens[i].range.position, previous_end);
    EXPECT_EQ(tokens[i].str,
              text.substr(tokens[i].range.position, tokens[i].range.length));
    previous_end = tokens[i].range.position + tokens[i].range.length;
  }
}

TEST(SegmentTest, NonCJKTextIsUnchangedFromTheDefaultSplitter) {
  // The regression wall around every existing English index: because only
  // Han/Hiragana/Katakana runs reach the model, everything else must come out
  // byte-identical to what utf8_plain_text_splitter() produces. Without the
  // gate the model mangles individual words -- "jumps" segments as ju/mps,
  // "created" as create/d, "known" as know/n, "iPhone" as i/Phone -- which is
  // what the first string below catches.
  const char *texts[] = {
      "The quick brown fox jumps over the lazy dog",
      "iPhone",
      "well-known ASCII, punctuation; and 2024 digits!",
      "In the beginning God created the heaven and the earth.",
      "",
  };

  for (const auto *text : texts) {
    auto expected = tokenize(utf8_plain_text_splitter(), text);
    auto actual = tokenize(segmenting_splitter(), text);

    ASSERT_EQ(expected.size(), actual.size()) << text;
    for (size_t i = 0; i < expected.size(); i++) {
      EXPECT_EQ(expected[i].str, actual[i].str) << text;
      EXPECT_EQ(expected[i].range.position, actual[i].range.position) << text;
      EXPECT_EQ(expected[i].range.length, actual[i].range.length) << text;
    }
  }
}

TEST(SegmentTest, NormalizerIsAppliedToEverySegment) {
  // SplitterTokenizer must honour the Normalizer it is handed, or a
  // normalizer-equipped InMemoryIndexer would silently index unnormalized
  // terms. Uses mixed-script text so both the model path and the pass-through
  // path are covered.
  auto normalizer = [](const std::u32string &s) {
    return unicode::to_lowercase(s);
  };
  auto tokens = tokenize(segmenting_splitter(), "私はTOKYOに行った", normalizer);

  EXPECT_EQ((std::vector<std::string>{"私", "は", "tokyo", "に", "行っ", "た"}),
            terms(tokens));
}

TEST(SegmentTest, MalformedUtf8TerminatesInsteadOfHanging) {
  // decode_codepoint returns 0 for bytes it cannot decode, so the letter-run
  // loop used to advance by 0 and spin forever on a truncated sequence. That
  // is reachable from an end-user query string through parse_query, not just
  // from documents, so it has to terminate on anything.
  const std::string truncated = "ab\xE3";        // 3-byte sequence cut short
  const std::string bad_continuation = "\xE3\x81\x41";

  // Terminating at all is the assertion; the terms are pinned only to catch a
  // future "fix" that silently drops the valid text around the bad bytes.
  EXPECT_EQ((std::vector<std::string>{"ab"}),
            terms(tokenize(utf8_plain_text_splitter(), truncated)));
  EXPECT_EQ(1u, tokenize(utf8_plain_text_splitter(), bad_continuation).size());

  // The segmenting splitter must survive it too: bad_continuation decodes to a
  // Hiragana codepoint, so the CJK gate opens and the raw bytes reach the
  // model, which rejects them. That path falls back to the unsegmented run.
  EXPECT_NO_THROW({
    tokenize(segmenting_splitter(), truncated);
    tokenize(segmenting_splitter(), bad_continuation);
  });

  // And the query side, which is where untrusted input actually arrives.
  EXPECT_NO_THROW(parse_query(segmenting_splitter(), nullptr, truncated));
}

TEST(SegmentTest, KnownLimitationsCarriedOverFromTheLetterRunRule) {
  // Segmentation refines letter runs, it does not redefine them, so the
  // existing rules still hold and are pinned here rather than left to be
  // rediscovered: punctuation separates runs and is dropped, and digits are
  // not letters (unicode::is_letter excludes Nd), so `2024年` indexes as `年`
  // alone. Changing the latter means changing is_letter for every language at
  // once, which is a separate decision.
  EXPECT_EQ((std::vector<std::string>{"東京"}),
            terms(tokenize(segmenting_splitter(), "「東京」。")));
  EXPECT_EQ((std::vector<std::string>{"年"}),
            terms(tokenize(segmenting_splitter(), "2024年")));
}

//-----------------------------------------------------------------------------
// The query side
//-----------------------------------------------------------------------------

TEST(SegmentTest, QueryTokenSplitsIntoAnImplicitPhrase) {
  auto splitter = segmenting_splitter();

  // A query token the splitter cuts up is an implicit phrase, the same
  // treatment `well-known` already gets -- not an And, which would match a
  // document containing 東京 and タワー far apart.
  EXPECT_EQ("(adj 東京 タワー)", parsed(splitter, "東京タワー"));

  // Whitespace still separates And operands; each of them is segmented.
  EXPECT_EQ("(and (adj 東京 タワー) 港区)", parsed(splitter, "東京タワー 港区"));

  // A token the splitter leaves whole stays a plain Term.
  EXPECT_EQ("東京", parsed(splitter, "東京"));
}

TEST(SegmentTest, TrailingOperatorsAttachToTheLastSegment) {
  auto splitter = segmenting_splitter();

  // `*` and `~N` convert only the last term of an implicit phrase (the
  // existing convert_operator_target rule), which is the useful reading:
  // 東京 exactly, followed by something starting with タワ.
  EXPECT_EQ("(adj 東京 タワ*)", parsed(splitter, "東京タワ*"));
  EXPECT_EQ("(adj 東京 タワー~1)", parsed(splitter, "東京タワー~1"));
}

TEST(SegmentTest, WildcardPatternsAreNotSegmented) {
  // A `*`-delimited piece is a pattern fragment, not a term. Segmenting it
  // would insert boundaries the pattern never asked for, so build_wildcard
  // deliberately bypasses the splitter and the whole token stays one pattern.
  EXPECT_EQ("[東京*タワー]", parsed(segmenting_splitter(), "東京*タワー"));
}

TEST(SegmentTest, TermFilterExpansionStillBecomesAnOr) {
  // The two ways one query token can become several terms stay distinct:
  // splitting is a phrase (Adjacent), filter expansion is a choice (Or).
  TermFilter synonyms = [](const std::u32string &s,
                           std::function<void(std::u32string)> emit) {
    emit(s);
    if (s == U"東京") {
      emit(U"TOKYO");
    }
  };

  EXPECT_EQ("(adj (or 東京 TOKYO) タワー)",
            parsed(segmenting_splitter(), "東京タワー", synonyms));
}

//-----------------------------------------------------------------------------
// Index and search, end to end
//-----------------------------------------------------------------------------

namespace {

// Same splitter on both sides, which is the whole point of the design.
InMemoryInvertedIndex<TextRange> japanese_index(TextSplitter splitter) {
  const char *documents[] = {
      "私は東京タワーに行った",
      "京都タワーは京都駅の前にある",
      "東京は日本の首都です",
  };

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  for (size_t id = 0; id < std::size(documents); id++) {
    indexer.index_document(id, SplitterTokenizer(splitter, documents[id]));
  }
  return index;
}

} // namespace

TEST(SegmentTest, PhraseSearchFindsTheSegmentedTermButNotASubstring) {
  auto splitter = segmenting_splitter();
  auto index = japanese_index(splitter);

  // 東京タワー is only in document 0 ...
  auto expr = parse_query(splitter, nullptr, "東京タワー");
  ASSERT_TRUE(expr);
  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, *expr));

  // ... while 東京 alone also matches document 2. This is the pair that fails
  // without segmentation: the whole sentence would be one term, so neither
  // query would match anything.
  auto expr_tokyo = parse_query(splitter, nullptr, "東京");
  ASSERT_TRUE(expr_tokyo);
  EXPECT_EQ((std::vector<size_t>{0, 2}), search_ids(index, *expr_tokyo));

  // 京タ straddles a word boundary (東`京タ`ワー), so it is not a term and
  // matches nothing -- a substring search would wrongly return document 0.
  auto expr_substring = parse_query(splitter, nullptr, "京タ");
  ASSERT_TRUE(expr_substring);
  EXPECT_TRUE(search_ids(index, *expr_substring).empty());
}

TEST(SegmentTest, TextRangeSpansThePhraseInTheOriginalText) {
  const std::string document = "私は東京タワーに行った";

  auto splitter = segmenting_splitter();
  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(0, SplitterTokenizer(splitter, document));

  auto expr = parse_query(splitter, nullptr, "東京タワー");
  ASSERT_TRUE(expr);
  auto postings = perform_search(index, *expr);
  ASSERT_EQ(1u, postings->size());
  ASSERT_EQ(1u, postings->search_hit_count(0));

  // The Adjacent hit spans both segments, so highlighting recovers the phrase
  // as it appears in the source text.
  auto range = index.text_range(*postings, 0, 0);
  EXPECT_EQ("東京タワー", document.substr(range.position, range.length));
}

TEST(SegmentTest, CompressedRoundTripPreservesTextRanges) {
  const std::string document = "私は東京タワーに行った";

  auto splitter = segmenting_splitter();
  InMemoryInvertedIndex<TextRange> index;
  {
    InMemoryIndexer indexer(index, nullptr);
    indexer.index_document(0, SplitterTokenizer(splitter, document));
  }

  // format_type=2 stores the text ranges as Elias-Fano structures, which
  // assume monotone positions -- an assumption the splitter has to keep
  // holding once a model, rather than a linear scan, decides the boundaries.
  std::stringstream ss;
  index.save(ss, {}, IndexFormat::Compressed);

  InMemoryInvertedIndex<TextRange> loaded;
  loaded.load(ss);

  auto expr = parse_query(splitter, nullptr, "東京タワー");
  ASSERT_TRUE(expr);
  auto postings = perform_search(loaded, *expr);
  ASSERT_EQ(1u, postings->size());

  auto range = loaded.text_range(*postings, 0, 0);
  EXPECT_EQ("東京タワー", document.substr(range.position, range.length));
}

TEST(SegmentTest, AnalyzerAndParseQueryStaySymmetric) {
  // The splitter composes with the TermFilter chain on both sides: index
  // through Analyzer<TextRange>, query through the same chain, and a stop word
  // dropped on one side is dropped on the other.
  TermFilter chain = [](const std::u32string &s,
                        std::function<void(std::u32string)> emit) {
    if (s != U"は") {
      emit(s);
    }
  };

  auto splitter = segmenting_splitter();
  const std::string document = "私は東京タワーに行った";

  InMemoryInvertedIndex<TextRange> index;
  InMemoryIndexer indexer(index, nullptr);
  indexer.index_document(0, Analyzer<TextRange>{
                                SplitterTokenizer(splitter, document), chain});

  // The dropped particle is gone from the index ...
  EXPECT_FALSE(index.term_exists(U"は"));
  EXPECT_TRUE(index.term_exists(U"東京"));

  // ... and the query side drops it too, so 私は東京 becomes the phrase
  // 私 + 東京, which the gap-closing index-side positions match.
  auto expr = parse_query(splitter, chain, "私は東京");
  ASSERT_TRUE(expr);
  EXPECT_EQ("(adj 私 東京)", to_string(*expr));
  EXPECT_EQ((std::vector<size_t>{0}), search_ids(index, *expr));
}
