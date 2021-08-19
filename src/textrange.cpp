//
//  textrange.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

TextRange text_range(const TextRangeList &text_range_list,
                     const ISearchResult &result, size_t index,
                     size_t search_hit_index) {
  auto document_id = result.document_id(index);
  auto term_pos = result.term_position(index, search_hit_index);
  auto term_count = result.term_count(index, search_hit_index);
  if (term_count == 1) {
    return text_range_list.at(document_id)[term_pos];
  } else {
    auto beg = text_range_list.at(document_id)[term_pos];
    auto end = text_range_list.at(document_id)[term_pos + term_count - 1];
    auto length = end.position + end.length - beg.position;
    return TextRange{beg.position, length};
  }
}

}  // namespace searchlib
