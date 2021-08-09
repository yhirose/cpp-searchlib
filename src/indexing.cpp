//
//  indexing.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"
#include "utils.h"

namespace searchlib {

void indexing(Index &index, Tokenizer &tokenizer, size_t document_id) {
  tokenizer.tokenize([&](auto& str, auto term_pos) {
    if (!contains(index.term_dictionary, str)) {
      index.terms.push_back(str);
      index.term_dictionary[str] = index.term_dictionary.size();
    }

    auto term_id = index.term_dictionary[str];
    assert(term_id <= index.posting_list.size());

    if (term_id == index.posting_list.size()) {
      index.posting_list.resize(term_id + 1);
    }

    index.posting_list[term_id][document_id].push_back(term_pos);
  });
}


}  // namespace searchlib

