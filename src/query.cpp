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

std::optional<Expression> parse_query(const InvertedIndex &inverted_index,
                                      std::string_view sv) {
  static peg::parser parser(R"(
    ROOT        <- OR?
    OR          <- AND ('|' AND)*
    AND         <- PRIMARY+
    PRIMARY     <- PHRASE / TERM / '(' OR ')'
    PHRASE      <- '"' TERM+ '"'
    TERM        <- < [a-zA-Z0-9-]+ >
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
      return Expression{operation, static_cast<size_t>(-1),
                        vs.transform<Expression>()};
    };
  };
  parser["OR"] = list_handler(Operation::Or);
  parser["AND"] = list_handler(Operation::And);
  parser["PHRASE"] = list_handler(Operation::Adjacent);

  parser["TERM"] = [&](const peg::SemanticValues &vs) {
    auto term = u32(vs.token());

    if (inverted_index.normalizer) {
      term = inverted_index.normalizer(term);
    }

    if (!contains(inverted_index.term_dictionary, term)) {
      std::string msg = "invalid term '" + vs.token_to_string() + "'.";
      throw peg::parse_error(msg.c_str());
    }

    auto term_id = inverted_index.term_dictionary.at(term);
    return Expression{Operation::Term, term_id};
  };

  // parser.log = [](size_t line, size_t col, const std::string& msg) {
  //   std::cerr << line << ":" << col << ": " << msg << "\n";
  // };

  std::optional<Expression> expr;
  if (!parser.parse(sv, expr)) {
    return std::nullopt;
  }

  return *expr;
}

}  // namespace searchlib

