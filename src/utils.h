//
//  utils.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <string>

namespace searchlib {

template <typename T, typename U>
inline bool contains(const T &cont, const U &key) {
  return cont.find(key) != cont.end();
}

std::string u8(std::u32string_view u32);

std::u32string u32(std::string_view u8);

}  // namespace searchlib
