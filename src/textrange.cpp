//
//  textrange.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

TextRange text_range(const TextRangeList &text_range_list,
                     const IPostings &positions, size_t index,
                     size_t search_hit_index) {
  auto document_id = positions.document_id(index);
  auto term_pos = positions.term_position(index, search_hit_index);
  auto term_length = positions.term_length(index, search_hit_index);
  if (term_length == 1) {
    return text_range_list.at(document_id)[term_pos];
  } else {
    auto beg = text_range_list.at(document_id)[term_pos];
    auto end = text_range_list.at(document_id)[term_pos + term_length - 1];
    auto length = end.position + end.length - beg.position;
    return TextRange{beg.position, length};
  }
}

}  // namespace searchlib
