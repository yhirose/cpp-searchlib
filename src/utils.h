//
//  utils.h
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#pragma once

#include <string>

namespace searchlib {

std::string u8(std::u32string_view u32);

std::u32string u32(std::string_view u8);

} // namespace searchlib
