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
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  this->dir_.push_back(std::make_shared<Bucket>(this->bucket_size_, 0));
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
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(this->latch_);
  size_t index = this->IndexOf(key);
  std::shared_ptr<Bucket> target_bucket = this->dir_[index];
  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(this->latch_);
  size_t index = this->IndexOf(key);
  std::shared_ptr<Bucket> target_bucket = this->dir_[index];
  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(this->latch_);
  while (!this->dir_[this->IndexOf(key)]->Insert(key, value)) {
    auto index = this->IndexOf(key);
    auto target_bucket = this->dir_[index];
    if (target_bucket->GetDepth() == this->GetGlobalDepthInternal()) {
      // Now need to increment the global depth
      this->global_depth_ += 1;
      // Increment the local depth of the buckect
      target_bucket->IncrementDepth();
      // Redistribute the buckets
      auto target_index = index + (1 << (GetGlobalDepthInternal() - 1));
      auto new_bucket = this->RedistributeBucket(target_bucket, target_index, key, value);
      // Make a new dir
      std::vector<std::shared_ptr<Bucket>> new_dir(1 << GetGlobalDepthInternal(), nullptr);
      for (size_t i = 0; i < (1 << (GetGlobalDepthInternal() - 1)); i++) {
        if (i == index) {
          new_dir[i] = dir_[i];
          new_dir[target_index] = new_bucket;
        } else {
          new_dir[i] = dir_[i];
          new_dir[i + (1 << (GetGlobalDepthInternal() - 1))] = dir_[i];
        }
      }
      this->dir_ = new_dir;
      this->num_buckets_ += 1;
    } else {
      // Increment local depth
      target_bucket->IncrementDepth();
      // Redistrubute buckets
      auto mask = (1 << target_bucket->GetDepth()) - 1;
      auto target_index = std::hash<K>()(key) & mask;
      auto new_bucket = this->RedistributeBucket(target_bucket, target_index, key, value);
      auto pre_num_ptr = GetGlobalDepthInternal() - target_bucket->GetDepth() + 1;
      auto now_num_ptr = GetGlobalDepthInternal() - target_bucket->GetDepth();
      for (size_t i = 0; i < ((1 << pre_num_ptr) - (1 << now_num_ptr)); i++) {
        this->dir_[target_index + i * (1 << target_bucket->GetDepth())] = new_bucket;
      }
      this->num_buckets_ += 1;
    }
  }
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<Bucket> bucket, size_t index, const K &key,
                                                   const V &value) -> std::shared_ptr<Bucket> {
  std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(this->bucket_size_, bucket->GetDepth());
  for (auto item = bucket->GetItems().begin(); item != bucket->GetItems().end();) {
    auto current_item_hash = std::hash<K>()(item->first);
    auto lower_bits = current_item_hash & ((1 << bucket->GetDepth()) - 1);
    if (lower_bits == index) {
      new_bucket->Insert(item->first, item->second);
      item = bucket->GetItems().erase(item);
    } else {
      item++;
    }
  }
  return new_bucket;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  for (auto item = this->list_.begin(); item != this->list_.end(); item++) {
    if (item->first == key) {
      value = item->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  for (auto item = this->list_.begin(); item != this->list_.end(); item++) {
    if (item->first == key) {
      this->list_.erase(item);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  // UNREACHABLE("not implemented")
  for (auto item = this->list_.begin(); item != this->list_.end(); item++) {
    if (item->first == key) {
      item->second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  this->list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
