//
//  indexer.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"
#include "utils.h"

namespace searchlib {

ITokenizer::~ITokenizer() = default;

void Indexer::set_normalizer(
    InvertedIndex &invidx,
    std::function<std::u32string(const std::u32string &str)> normalizer) {
  invidx.normalizer_ = normalizer;
}

void Indexer::indexing(InvertedIndex &invidx, size_t document_id,
                       ITokenizer &tokenizer) {
  size_t term_count = 0;
  tokenizer.tokenize(invidx.normalizer_, [&](const auto &str, auto term_pos) {
    if (!contains(invidx.term_dictionary_, str)) {
      invidx.term_dictionary_[str] = {str, 0};
    }

    auto &term = invidx.term_dictionary_.at(str);
    term.term_occurrences++;
    term.postings.add_term_position(document_id, term_pos);

    term_count++;
  });

  invidx.documents_[document_id] = {term_count};
}

}  // namespace searchlib

