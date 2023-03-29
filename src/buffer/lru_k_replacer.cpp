//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  // 如果没有可以淘汰的frame_id直接返回
  if (curr_size_ == 0) {
    return false;
  }
  // 逆向访问历史链表,如果是可以淘汰的那就将他淘汰,同时将他的frame_id传给参数
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
    if (is_evictable_[*it]) {
      // *it指的是要被淘汰的frame_it
      *frame_id = *it;
      access_count_[*frame_id] = 0;
      history_list_.erase(history_map_[*frame_id]);
      history_map_.erase(*frame_id);
      is_evictable_[*frame_id] = false;
      curr_size_--;
      return true;
    }
  }
  for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); ++it) {
    if (is_evictable_[*it]) {
      // *it指的是要被淘汰的frame_it
      *frame_id = *it;
      access_count_[*frame_id] = 0;
      cache_list_.erase(cache_map_[*frame_id]);
      cache_map_.erase(*frame_id);
      is_evictable_[*frame_id] = false;
      curr_size_--;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  // 添加访问次数
  access_count_[frame_id]++;
  if (access_count_[frame_id] == k_) {
    // 查看是否能在历史哈希表中找到frame_id
    if (history_map_.count(frame_id) != 0U) {
      // 找到他记录的历史链表迭代器的位置,删除
      auto it = history_map_[frame_id];
      history_list_.erase(it);
    }
    // 从历史哈希表中删除
    history_map_.erase(frame_id);
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else if (access_count_[frame_id] > k_) {
    // 如果他的访问次数已经大于k并且已经在缓存链表中，更新他的位置
    if (cache_map_.count(frame_id) != 0U) {
      auto it = cache_map_[frame_id];
      cache_list_.erase(it);
    }
    cache_list_.push_front(frame_id);
    cache_map_[frame_id] = cache_list_.begin();
  } else {
    // 如果他只是第一次访问还未添加到历史链表中，那就添加
    if (history_map_.count(frame_id) == 0U) {
      history_list_.push_front(frame_id);
      history_map_[frame_id] = history_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  // frame_id没有访问过，直接返回
  if (access_count_[frame_id] == 0) {
    return;
  }
  // set_evictable == true,并且frame_id已经设置过为不可淘汰的时候将frame_id设置为可淘汰并且增加可淘汰数量
  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  // 当frame_id可以淘汰时，并且set_evictable==false的时候将frame_id设置为不可淘汰，可淘汰数减一
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (access_count_[frame_id] == 0) {
    return;
  }
  // 判断当前frame_id是在缓存链表还是历史链表中,然后通过哈希表获取迭代器位置从链表中删除再通过frame_id从哈希表中删除
  if (access_count_[frame_id] >= k_) {
    auto it = cache_map_[frame_id];
    cache_list_.erase(it);
    cache_map_.erase(frame_id);
  } else {
    auto it = history_map_[frame_id];
    history_list_.erase(it);
    history_map_.erase(frame_id);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(latch_);
  return curr_size_;
}

}  // namespace bustub
