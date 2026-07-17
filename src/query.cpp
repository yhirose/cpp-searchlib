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

// Shared grammar for both parse_query overloads; only TERM handling (how a
// raw query token becomes an Expression) differs between them.
static std::optional<Expression>
parse_query_impl(const std::function<Expression(std::string_view)> &term_handler,
                 std::string_view query) {
  static peg::parser parser(R"(
    ROOT        <- OR?
    OR          <- AND ('|' AND)*
    AND         <- NOT+
    NOT         <- '-' PRIMARY / NEAR
    NEAR        <- PRIMARY ('~' PRIMARY)*
    PRIMARY     <- PHRASE / TERM / '(' OR ')'
    PHRASE      <- '"' TERM+ '"'
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
  // Tokenize the raw query token with the same splitting logic as
  // documents (index side and query side always agree on term
  // boundaries), then run each split piece through the same TermFilter
  // chain an index-side Analyzer<T> would use.
  auto term_handler = [&](std::string_view token) -> Expression {
    auto run_filter = [&](const std::u32string &str) {
      std::vector<std::u32string> emitted;
      if (filter) {
        filter(str, [&](std::u32string out) { emitted.push_back(std::move(out)); });
      } else {
        emitted.push_back(str);
      }
      return emitted;
    };

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

    std::vector<Expression> nodes;
    bool split_any = false;
    UTF8PlainTextTokenizer tokenizer(token);
    tokenizer(nullptr, [&](const auto &str, auto, auto) {
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
    return Expression{Operation::Adjacent, std::u32string(), DEFAULT_NEAR_SIZE,
                      std::move(nodes)};
  };

  return parse_query_impl(term_handler, query);
}

} // namespace searchlib

