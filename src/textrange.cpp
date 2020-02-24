//
//  textrange.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

TextRange text_range(const TextRangeList &text_range_list,
                     const SearchResult &result, size_t index,
                     size_t search_hit_index) {
  auto document_id = result.document_id(index);
  auto term_pos = result.term_position(index, search_hit_index);
  return text_range_list.at(document_id)[term_pos];
}

}  // namespace searchlib
