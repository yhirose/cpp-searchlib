//
//  query.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "lib/peglib.h"
#include "searchlib.h"
#include "utils.h"

namespace searchlib {

// `Not` is allowed only as a direct child of `And`, and `And` must have at
// least one positive child, since the index cannot enumerate all documents.
static bool is_valid_expression(const Expression &expr) {
  switch (expr.operation) {
  case Operation::Term:
    return true;
  case Operation::Not:
    return false;
  case Operation::And: {
    size_t positive_count = 0;
    for (const auto &node : expr.nodes) {
      if (node.operation == Operation::Not) {
        if (!is_valid_expression(node.nodes[0])) {
          return false;
        }
      } else {
        if (!is_valid_expression(node)) {
          return false;
        }
        positive_count++;
      }
    }
    return positive_count > 0;
  }
  default:
    for (const auto &node : expr.nodes) {
      if (!is_valid_expression(node)) {
        return false;
      }
    }
    return true;
  }
}

static const size_t DEFAULT_NEAR_SIZE = 4;

// Fuzzy queries are capped rather than taken at face value: the digits come
// straight from an end-user query string, and a large edit distance defeats
// the pruning both backends rely on (the FST automaton's can_match() stops
// rejecting subtrees, the in-memory scan's length filter stops rejecting
// terms), so `x~9` would quietly turn into "match most of the dictionary".
// Lucene caps fuzzy queries at 2 for the same reason. The C++ API
// (enumerate_terms_with_edit_distance) is left uncapped, since a caller
// naming a distance directly is not untrusted input.
static const size_t MAX_FUZZY_EDITS = 2;

// Saturating parse of the digits after `~`. The grammar guarantees they are
// digits, so the only thing to defend against is a value too large to hold --
// `apple~99999999999999999999` must clamp, not overflow or throw. The break is
// tested before the multiply, so `value` is at most MAX_FUZZY_EDITS going in
// and MAX_FUZZY_EDITS * 10 + 9 coming out, nowhere near size_t's range however
// many digits follow.
static size_t parse_max_edits(std::string_view digits) {
  size_t value = 0;
  for (auto c : digits) {
    if (value > MAX_FUZZY_EDITS) {
      break;
    }
    value = value * 10 + static_cast<size_t>(c - '0');
  }
  return std::min(value, MAX_FUZZY_EDITS);
}

// Applies `convert` to the term a trailing operator (`*`, `~N`) was attached
// to. Only the last term converts, so a token the raw tokenizer split into an
// implicit phrase (`well-kno*`, `well-known~1`) keeps its leading terms exact.
// Anything else -- an Or a filter produced from synonyms, a Prefix from
// `app*~2` -- is left alone and the operator silently dropped, since a prefix
// of, or a distance around, a synonym set has no useful meaning.
//
// An empty term means the filter dropped the whole token (a stop word). It has
// to stay a Term, which matches nothing: as a Prefix the empty string
// enumerates the entire dictionary, and as a Fuzzy it matches every term of
// length <= N, so `the*` and `the~2` would silently become near-match-alls.
template <typename Convert>
static Expression convert_operator_target(Expression expr, Convert convert) {
  auto *target = &expr;
  if (expr.operation == Operation::Adjacent && !expr.nodes.empty()) {
    target = &expr.nodes.back();
  }
  if (target->operation == Operation::Term && !target->term_str.empty()) {
    convert(*target);
  }
  return expr;
}

static Expression as_prefix(Expression expr) {
  return convert_operator_target(
      std::move(expr), [](Expression &e) { e.operation = Operation::Prefix; });
}

static Expression as_fuzzy(Expression expr, size_t max_edits) {
  return convert_operator_target(std::move(expr), [&](Expression &e) {
    e.operation = Operation::Fuzzy;
    e.near_operation_distance = max_edits;
  });
}

// Shared grammar for both parse_query overloads; only TERM handling (how a
// raw query token becomes an Expression) differs between them.
static std::optional<Expression>
parse_query_impl(const std::function<Expression(std::string_view)> &term_handler,
                 std::string_view query) {
  // FUZZY has to precede TERM (PEG picks the first alternative that matches,
  // and TERM alone would consume `apple` out of `apple~2` and leave `~2` to be
  // read as a NEAR operator -- which is exactly how `apple~2` used to parse).
  // It is one `<...>` token so that `%whitespace` cannot creep between the
  // term and its `~N`, keeping `apple~2` a fuzzy query while `apple ~ 2` stays
  // the NEAR it has always been. Requiring digits is what protects the other
  // established spellings: `apple~tree` has none, so it stays NEAR. The
  // trailing lookahead rejects `apple~2x`, where the digits are not the end of
  // the token -- without it, FUZZY would take `apple~2` and orphan the `x`.
  // That lookahead has to sit inside the `<...>`: outside it, %whitespace is
  // skipped first and it would test the start of the *next* token instead,
  // which breaks `apple~2 banana`.
  static peg::parser parser(R"(
    ROOT        <- OR?
    OR          <- AND ('|' AND)*
    AND         <- NOT+
    NOT         <- '-' PRIMARY / NEAR
    NEAR        <- PRIMARY ('~' PRIMARY)*
    PRIMARY     <- PHRASE / FUZZY / TERM / '(' OR ')'
    PHRASE      <- '"' TERM+ '"'
    FUZZY       <- < (![-"|~() \t\r\n] .) (!["|~() \t\r\n] .)* '~' [0-9]+ !(!["|~() \t\r\n] .) >
    TERM        <- < (![-"|~() \t\r\n] .) (!["|~() \t\r\n] .)* >
    %whitespace <- [ \t]*
  )");

  parser["ROOT"] =
      [&](const peg::SemanticValues &vs) -> std::optional<Expression> {
    if (!vs.empty()) {
      return std::any_cast<Expression>(vs[0]);
    }
    return std::nullopt;
  };

  auto list_handler = [&](Operation operation) {
    return [=](const peg::SemanticValues &vs) {
      if (vs.size() == 1) {
        return std::any_cast<Expression>(vs[0]);
      }
      return Expression{operation, std::u32string(), DEFAULT_NEAR_SIZE,
                        vs.transform<Expression>()};
    };
  };
  parser["OR"] = list_handler(Operation::Or);
  parser["AND"] = list_handler(Operation::And);
  parser["NEAR"] = list_handler(Operation::Near);

  parser["PHRASE"] = [=](const peg::SemanticValues &vs) {
    // Flatten implicit phrases made from a single token (e.g. `well-known`).
    std::vector<Expression> nodes;
    for (const auto &v : vs) {
      auto expr = std::any_cast<Expression>(v);
      if (expr.operation == Operation::Adjacent) {
        nodes.insert(nodes.end(), expr.nodes.begin(), expr.nodes.end());
      } else {
        nodes.push_back(expr);
      }
    }
    if (nodes.size() == 1) {
      return nodes[0];
    }
    return Expression{Operation::Adjacent, std::u32string(), DEFAULT_NEAR_SIZE,
                      std::move(nodes)};
  };

  parser["NOT"] = [](const peg::SemanticValues &vs) {
    if (vs.choice() == 0) {
      return Expression{Operation::Not, std::u32string(), 0,
                        {std::any_cast<Expression>(vs[0])}};
    }
    return std::any_cast<Expression>(vs[0]);
  };

  parser["TERM"] = [&](const peg::SemanticValues &vs) {
    return term_handler(vs.token());
  };

  // The `~N` is stripped here rather than inside term_handler because, unlike
  // the `*` of a prefix query, `~` is a grammar operator: term_handler never
  // sees a token containing one. The literal part still goes through
  // term_handler, so a fuzzy term is tokenized and filtered exactly like a
  // plain one.
  parser["FUZZY"] = [&](const peg::SemanticValues &vs) {
    auto token = vs.token();
    auto tilde = token.rfind('~');
    return as_fuzzy(term_handler(token.substr(0, tilde)),
                    parse_max_edits(token.substr(tilde + 1)));
  };

  // parser.log = [](size_t line, size_t col, const std::string& msg) {
  //   std::cerr << line << ":" << col << ": " << msg << "\n";
  // };

  std::optional<Expression> expr;
  if (!parser.parse(query, expr)) {
    return std::nullopt;
  }

  if (expr && !is_valid_expression(*expr)) {
    return std::nullopt;
  }

  return expr;
}

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query) {
  return parse_query(to_term_filter(std::move(normalizer)), query);
}

std::optional<Expression> parse_query(TermFilter filter,
                                      std::string_view query) {
  return parse_query(nullptr, std::move(filter), query);
}

std::optional<Expression> parse_query(TextSplitter splitter, TermFilter filter,
                                      std::string_view query) {
  // A null splitter means "the default", so the rest of this function never
  // has to test for one. This is what makes the two shorter overloads exactly
  // this one with utf8_plain_text_splitter().
  if (!splitter) {
    splitter = utf8_plain_text_splitter();
  }

  // Split the raw query token with the same splitter the documents were
  // indexed with (index side and query side always agree on term
  // boundaries), then run each split piece through the same TermFilter
  // chain an index-side Analyzer<T> would use.
  auto term_handler = [&](std::string_view token) -> Expression {
    // A trailing `*` turns the token into a prefix query, provided it is the
    // token's only `*` -- that keeps the common `foo*` case on the cheaper
    // literal-prefix path (see Operation::Prefix) instead of the general
    // wildcard automaton below. This is resolved here rather than in the
    // grammar so that the star binds tightly to its token (`foo*` is a
    // prefix, `foo *` is not) without having to fight %whitespace skipping.
    // A bare `*` is left alone: it stays an ordinary term that no index can
    // contain, rather than becoming a match-all.
    auto is_prefix = token.size() > 1 && token.back() == '*' &&
                     token.find('*') == token.size() - 1;
    if (is_prefix) {
      token.remove_suffix(1);
    }

    auto run_filter = [&](const std::u32string &str) {
      std::vector<std::u32string> emitted;
      if (filter) {
        filter(str, [&](std::u32string out) { emitted.push_back(std::move(out)); });
      } else {
        emitted.push_back(str);
      }
      return emitted;
    };

    // Any other placement of `*` (leading, interior, or more than one) makes
    // the whole token a wildcard pattern instead of a prefix. Handled
    // separately from build() below, because the splitter treats `*` as a
    // non-letter separator and would otherwise fragment the token into
    // unrelated words -- the same way it fragments `well-known` -- which
    // would lose the pattern structure. Each `*`-delimited piece is filtered
    // as one opaque chunk rather than re-split into its own letter runs, so a
    // wildcard segment that itself contains punctuation (`well-kno*n`) is not
    // decomposed the way a plain phrase term would be; out of scope for v1.
    //
    // The splitter is not applied here for the same reason: a `*`-delimited
    // piece is a pattern fragment, not a term, and segmenting it would insert
    // boundaries the pattern never asked for (東京タワ* must stay one prefix
    // fragment, not become 東京 followed by a pattern starting at タワ).
    auto build_wildcard = [&]() -> Expression {
      std::u32string pattern;
      size_t start = 0;
      while (true) {
        auto star_pos = token.find('*', start);
        auto segment = token.substr(
            start, star_pos == std::string_view::npos
                       ? std::string_view::npos
                       : star_pos - start);
        if (!segment.empty()) {
          auto emitted = run_filter(u32(segment));
          // A filter that expands one chunk into several (synonyms) has no
          // sensible meaning inside a wildcard pattern, so only the first
          // survives; one that drops the chunk (stop word) just closes the
          // gap, the same treatment build() gives a dropped word.
          if (!emitted.empty()) {
            pattern += emitted[0];
          }
        }
        if (star_pos == std::string_view::npos) {
          break;
        }
        // Collapses consecutive/duplicate stars (`a**b`) to one, which is
        // semantically identical and keeps the automaton's state count down.
        if (pattern.empty() || pattern.back() != U'*') {
          pattern += U'*';
        }
        start = star_pos + 1;
      }
      return Expression{Operation::Wildcard, pattern};
    };

    if (!is_prefix && token.size() > 1 &&
        token.find('*') != std::string_view::npos) {
      return build_wildcard();
    }

    auto to_expression = [](std::vector<std::u32string> emitted) {
      // A single emit is a plain Term; a filter that expanded one token into
      // several (e.g. synonyms) maps to an Or, never an implicit Adjacent
      // phrase (that would build the wrong query, e.g. "usa united states").
      if (emitted.size() == 1) {
        return Expression{Operation::Term, emitted[0]};
      }
      std::vector<Expression> nodes;
      for (auto &term : emitted) {
        nodes.push_back(Expression{Operation::Term, term});
      }
      return Expression{Operation::Or, std::u32string(), 0, std::move(nodes)};
    };

    auto build = [&]() -> Expression {
      std::vector<Expression> nodes;
      bool split_any = false;
      splitter(token, [&](const std::u32string &str, TextRange) {
        split_any = true;
        auto emitted = run_filter(str);
        if (emitted.empty()) {
          // Dropped (e.g. stop word); close the gap, matching how
          // Analyzer<T> closes position gaps on the index side.
          return;
        }
        nodes.push_back(to_expression(std::move(emitted)));
      });

      if (!split_any) {
        // No letter sequence in the token (e.g. digits only); such a term can
        // never exist in the index. Run it through the filter as-is so a
        // configured chain (e.g. lowercasing) still applies, but an empty
        // result still matches nothing.
        auto emitted = run_filter(u32(token));
        if (emitted.empty()) {
          return Expression{Operation::Term, u32(token)};
        }
        return to_expression(std::move(emitted));
      }

      if (nodes.empty()) {
        // Every split piece was dropped by the filter; matches nothing.
        return Expression{Operation::Term, std::u32string()};
      }
      if (nodes.size() == 1) {
        return nodes[0];
      }
      // A token split into multiple pieces (e.g. `well-known`) is an implicit
      // phrase.
      return Expression{Operation::Adjacent, std::u32string(),
                        DEFAULT_NEAR_SIZE, std::move(nodes)};
    };

    auto expr = build();
    if (is_prefix) {
      return as_prefix(std::move(expr));
    }
    return expr;
  };

  return parse_query_impl(term_handler, query);
}

} // namespace searchlib

