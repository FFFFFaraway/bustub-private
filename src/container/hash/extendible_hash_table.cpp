//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::shared_ptr<Bucket>(new Bucket(bucket_size_, 0)));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto idx = IndexOf(key);
  return dir_[idx]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto idx = IndexOf(key);
  return dir_[idx]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    auto idx = IndexOf(key);
    auto bucket = dir_[idx];
    if (bucket->Insert(key, value)) {
      return;
    }
    auto d = bucket->GetDepth();
    if (d == global_depth_) {
      global_depth_++;
      auto len = dir_.size();
      for (size_t i = 0; i < len; i++) {
        dir_.push_back(dir_[i]);
      }
    }
    auto a = std::shared_ptr<Bucket>(new Bucket(bucket_size_, d + 1));
    auto b = std::shared_ptr<Bucket>(new Bucket(bucket_size_, d + 1));
    for (size_t i = 0; i < (1 << global_depth_); i++) {
      if ((i & ((1 << d) - 1)) == (idx & ((1 << d) - 1))) {
        if (i & (1 << d)) {
          dir_[i] = a;
        } else {
          dir_[i] = b;
        }
      }
    }
    num_buckets_++;
    RedistributeBucket(bucket);
  }
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket) -> void {
  for (auto &item : bucket->GetItems()) {
    auto idx = IndexOf(item.first);
    dir_[idx]->Insert(item.first, item.second);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = list_.begin();
  for (; it != list_.end(); it++) {
    if (it->first == key) {
      break;
    }
  }
  if (it == list_.end()) {
    return false;
  }
  value = it->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = list_.begin();
  for (; it != list_.end(); it++) {
    if (it->first == key) {
      break;
    }
  }
  if (it == list_.end()) {
    return false;
  }
  list_.erase(it);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  auto it = list_.begin();
  for (; it != list_.end(); it++) {
    if (it->first == key) {
      break;
    }
  }
  if (it == list_.end()) {
    list_.push_back(std::pair<K, V>(key, value));
    return true;
  }
  it->second = value;
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
