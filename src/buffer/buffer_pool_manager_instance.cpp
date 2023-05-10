//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  // 先访问所有的frame如果引用计数不为0那就说明有这个frame不可被驱逐
  // 如果所有的frame都不可被驱逐直接返回nullptr
  size_t i = 0;
  for (; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }
  frame_id_t frame_id;
  // 空闲链表不为空，代表frame还有空闲那就直接获取空闲链表尾部存储的frame_id
  // 因为每次都只是取出链表尾部的frame_id 当被使用就应该从链表中弹出
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 如果全部都没有办法驱逐那就返回nullptr
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 获取驱逐的frame的page_id
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    // 判断是否驱逐的是脏页
    if (pages_[frame_id].IsDirty()) {
      // 将驱逐frame的数据落回磁盘
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(evict_page_id);
  }
  // 分配页id
  *page_id = AllocatePage();
  // 将页id和frame_id插入到哈希表中
  page_table_->Insert(*page_id, frame_id);
  // 让replacer_访问一遍，同时将这一个frame设置为不可驱逐
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 在页中记录page_id，同时将引用计数设置为1表示不可被驱逐
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // 先看page_id是否已经在缓冲池中
  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  // 因为缓冲池有，之后要么插入要么替换，先做一层判断，看是否所有的frame都不可被驱逐
  size_t i = 0;
  for (; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ == 0) {
      break;
    }
  }
  if (i == pool_size_) {
    return nullptr;
  }

  // 缓冲池中没有，但是空闲链表不为空就插入，空闲链表为空就用替换器替换
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 驱逐的page_id
    page_id_t evict_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      // 落盘
      disk_manager_->WritePage(evict_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(evict_page_id);
  }

  // page_id = AllocatePage();
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;

  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

// 对给定page的pin_count不为0的话就减一如果减完发现为0那就在replacer中将这个页设置为可驱逐
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // 如果缓冲池中找不到page_id获取page_id的格式不合法，返回false
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 在缓冲池中找到了，先查看他的引用计数如果已经已经小于等于0了说明可以被驱逐了不再需要后续操作了
  if (pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  // 引用计数减一，如果等于0了那就代表这个frame可以被驱逐，evict设置为true
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // 如果缓冲池中找不到page_id获取page_id的格式不合法，返回false
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t frame_id = 0; frame_id < pool_size_; ++frame_id) {
    FlushPgImp(pages_[frame_id].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  // 判断能否在缓冲池中找到page_id以及page_id的合法性
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return true;
  }
  // 如果引用计数不为0不能删除,直接返回
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }
  replacer_->Remove(frame_id);

  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;

  page_table_->Remove(page_id);
  // 将新空闲的frame_id插入到链表头
  free_list_.emplace_front(frame_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
