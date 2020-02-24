//
//  utils.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "utils.h"

#include "lib/unicodelib_encodings.h"

namespace searchlib {

std::string u8(std::u32string_view u32) { return unicode::utf8::encode(u32); }

std::u32string u32(std::string_view u8) { return unicode::utf8::decode(u8); }

}  // namespace searchlib

