//
//  indexing.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <cassert>

#include "searchlib.h"
#include "utils.h"

namespace searchlib {

void indexing(InvertedIndex &inverted_index, Tokenizer &tokenizer,
              size_t document_id) {
  tokenizer.tokenize([&](const auto &str, auto term_pos) {
    if (!contains(inverted_index.term_dictionary, str)) {
      inverted_index.terms.push_back({str, 0});

      auto term_id = inverted_index.term_dictionary.size();
      inverted_index.term_dictionary[str] = term_id;
      inverted_index.posting_list.resize(term_id + 1);
    }

    auto term_id = inverted_index.term_dictionary[str];
    assert(term_id < inverted_index.term_dictionary.size());
    assert(term_id < inverted_index.posting_list.size());

    inverted_index.terms[term_id].tf++;

    auto & positional_list = inverted_index.posting_list[term_id];
    positional_list[document_id].push_back(term_pos);
  });

  inverted_index.document_count++;
}

}  // namespace searchlib

