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

std::optional<Expression> parse_query(Normalizer normalizer,
                                      std::string_view query) {
  static peg::parser parser(R"(
    ROOT        <- OR?
    OR          <- AND ('|' AND)*
    AND         <- NOT+
    NOT         <- '-' PRIMARY / NEAR
    NEAR        <- PRIMARY ('~' PRIMARY)*
    PRIMARY     <- PHRASE / TERM / '(' OR ')'
    PHRASE      <- '"' TERM+ '"'
    TERM        <- < [a-zA-Z0-9] [a-zA-Z0-9-]* >
    %whitespace <- [ \t]*
  )");

  parser["ROOT"] =
      [&](const peg::SemanticValues &vs) -> std::optional<Expression> {
    if (!vs.empty()) {
      return std::any_cast<Expression>(vs[0]);
    }
    return std::nullopt;
  };

  size_t DEFAULT_NEAR_SIZE = 4;

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
  parser["PHRASE"] = list_handler(Operation::Adjacent);

  parser["NOT"] = [](const peg::SemanticValues &vs) {
    if (vs.choice() == 0) {
      return Expression{Operation::Not, std::u32string(), 0,
                        {std::any_cast<Expression>(vs[0])}};
    }
    return std::any_cast<Expression>(vs[0]);
  };

  parser["TERM"] = [&](const peg::SemanticValues &vs) {
    auto term = normalizer(u32(vs.token()));
    return Expression{Operation::Term, term};
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

} // namespace searchlib

