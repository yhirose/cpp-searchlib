//
//  storedfields.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include "searchlib.h"

namespace searchlib {

void StoredFields::set(size_t document_id, std::string value) {
  values_[document_id] = std::move(value);
}

bool StoredFields::has(size_t document_id) const {
  return values_.find(document_id) != values_.end();
}

const std::string *StoredFields::get(size_t document_id) const {
  auto it = values_.find(document_id);
  if (it == values_.end()) {
    return nullptr;
  }
  return &it->second;
}

void StoredFields::remove(size_t document_id) { values_.erase(document_id); }

size_t StoredFields::size() const { return values_.size(); }

void StoredFields::save(std::ostream &os) const {
  detail::save_sorted_map(
      os, values_, [](std::ostream &os, const std::string &value) {
        detail::write_scalar<uint64_t>(os, value.size());
        os.write(value.data(), static_cast<std::streamsize>(value.size()));
      });
}

void StoredFields::load(std::istream &is) {
  detail::load_sorted_map(is, values_, [](std::istream &is) {
    auto length = static_cast<size_t>(detail::read_scalar<uint64_t>(is));
    std::string value(length, '\0');
    if (length > 0) {
      is.read(value.data(), static_cast<std::streamsize>(length));
      if (!is) {
        throw std::runtime_error(
            "searchlib: unexpected end of stored-fields stream");
      }
    }
    return value;
  });
}

} // namespace searchlib
