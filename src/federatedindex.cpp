//
//  federatedindex.cpp
//
//  Copyright (c) 2021 Yuji Hirose. All rights reserved.
//  MIT License
//

#include <algorithm>
#include <mutex>

#include "searchlib.h"

namespace searchlib {

void FederatedIndex::add(std::shared_ptr<IInvertedIndex> index,
                         std::shared_ptr<IMutableInvertedIndex> mutable_index) {
  std::lock_guard<std::mutex> lock(mutex_);
  members_.push_back({std::move(index), std::move(mutable_index)});
}

void FederatedIndex::remove(const std::shared_ptr<IInvertedIndex> &index) {
  std::lock_guard<std::mutex> lock(mutex_);
  members_.erase(std::remove_if(members_.begin(), members_.end(),
                                [&](const auto &member) {
                                  return member.index == index;
                                }),
                 members_.end());
}

std::vector<FederationMember> FederatedIndex::snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return members_;
}

std::vector<FederatedHit> perform_federated_search(const FederatedIndex &federation,
                                                   const Expression &expr) {
  std::vector<FederatedHit> hits;
  for (const auto &member : federation.snapshot()) {
    auto postings = perform_search(*member.index, expr);
    for (size_t i = 0; i < postings->size(); i++) {
      hits.push_back({member.index, postings, i});
    }
  }
  return hits;
}

} // namespace searchlib
