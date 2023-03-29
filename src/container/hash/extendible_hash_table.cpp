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
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
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
  std::lock_guard<std::mutex> guard(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  size_t index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> guard(latch_);
  // 判断桶是不是已经满了
  while (dir_[IndexOf(key)]->IsFull()) {
    size_t index = IndexOf(key);
    auto target_bucket = dir_[index];
    int bucket_localdepth = target_bucket->GetDepth();
    // 全局深度和局部深度一样那就扩容
    if (global_depth_ == target_bucket->GetDepth()) {
      // 目录翻倍
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      // 翻倍后新产生的目录指向原本的桶
      for (int inx = 0; inx < capacity; ++inx) {
        dir_[capacity + inx] = dir_[inx];
      }
      // 全局深度+1
      global_depth_++;
    }
    // 分裂桶
    // 先将局部深度+1
    int mask = 1 << bucket_localdepth;
    // 用原本目标的局部深度+1创建出1和0两个桶
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, bucket_localdepth + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, bucket_localdepth + 1);
    // 遍历桶的list，将key的哈希值的最低位是0还是1分发给新桶
    for (auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      // 判断局部深度的最低位和当前桶key的哈希值，如果不是0那就将他插入到one_Bucket中
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }
    // 确保这两个桶都可用
    if (!bucket_1->GetItems().empty() && !bucket_0->GetItems().empty()) {
      num_buckets_++;
    }
    for (size_t i = 0; i < dir_.size(); ++i) {
      if (dir_[i] == target_bucket) {  // 这里是目标桶 ，因为分裂了产生了桶1和桶0所以需要对原本的数据进行重新分配数据
        // 当前下标最低位与上mask，为0就让dir_[i]指向zeroBucket,反之同理
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }

  // 获取目标桶的下标
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  // 如果key已经存在，更新
  for (auto &it : target_bucket->GetItems()) {
    if (it.first == key) {
      it.second = value;
      return;
    }
  }
  // 不存在，直接插入
  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto &it : list_) {
    if (it.first == key) {
      value = it.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  if (list_.empty()) {
    return false;
  }
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &pair) { return pair.first == key; });
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
  auto it = std::find_if(list_.begin(), list_.end(), [&](const auto &pair) { return pair.first == key; });
  if (it != list_.end()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
