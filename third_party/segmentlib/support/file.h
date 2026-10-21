#pragma once

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <system_error>
#include <vector>

#include "segmentlib/support/expected.h"
#include "segmentlib/types.h"

namespace segmentlib::detail {

// Reads a whole model file. Non-regular files are rejected: a directory opens
// as an ifstream on Linux and reports a bogus size.
inline Expected<std::vector<std::byte>, Error> read_model_file(
    const std::filesystem::path& path) {
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec)) {
        return Unexpected(Error{ErrorCode::IoError, "cannot open model file"});
    }
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return Unexpected(Error{ErrorCode::IoError, "cannot open model file"});
    }
    in.seekg(0, std::ios::end);
    const auto size = in.tellg();
    if (size < 0) {
        return Unexpected(Error{ErrorCode::IoError, "cannot read model file"});
    }
    std::vector<std::byte> buf(static_cast<std::size_t>(size));
    in.seekg(0, std::ios::beg);
    in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
    if (!in) {
        return Unexpected(Error{ErrorCode::IoError, "cannot read model file"});
    }
    return buf;
}

}  // namespace segmentlib::detail
