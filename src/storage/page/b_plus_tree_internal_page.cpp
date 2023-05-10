//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 ******auto ***********************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  // 通过二分找到key的下标
  int l = 1;
  int r = GetSize();
  if (l >= r) {
    return GetSize();
  }
  while (l < r) {
    int mid = (r + l) / 2;
    if (comparator(array_[mid].first, key) < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) -> void {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) -> ValueType {
  // 内部节点的下标由1开始
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(array_[i].first, key) > 0) {
      return array_[i - 1].second;
    }
  }
  return array_[GetSize() - 1].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const MappingType &value, const KeyComparator &comparator) -> void {
  // 找到合适位置将value插入进去
  for (int i = GetSize() - 1; i > 0; --i) {
    // 从array的最后一位开始插入，如果key大于array_[i] 那就将value插入到value[i+1]处反之将array_[i]向后移动一位
    if (comparator(array_[i].first, value.first) > 0) {
      array_[i + 1] = array_[i];
    } else {
      array_[i + 1] = value;
      IncreaseSize(1);
      return;
    }
  }
  // 如果value的key是最小的那就放在第一位size的大小要增加1
  SetKeyAt(1, value.first);
  SetValueAt(1, value.second);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) -> void {
  for (int i = GetSize(); i > 0; --i) {
    array_[i] = array_[i - 1];
  }
  SetValueAt(0, value);
  SetKeyAt(1, key);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) -> bool {
  // 找到key的下标
  int index = KeyIndex(key, comparator);
  // 通过key查找到的下标在array中的位置与key判断相等返回false 或者index小于0 内部节点的index必须大于0 0是非法下标
  if (index >= GetSize() || comparator(KeyAt(index), key) != 0) {
    return false;
  }
  for (int i = index + 1; i < GetSize(); ++i) {
    array_[i - 1] = array_[i];
  }
  // 删除完需要对size进行更改
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteFirst() -> void {
  for (int i = 1; i < GetSize(); ++i) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBrotherPage(page_id_t child_page_id, Page *&brother_page, KeyType &key,
                                                    bool &isPre, BufferPoolManager *buffer_pool_manager) -> void {
  int i;
  for (i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == child_page_id) {
      break;
    }
  }
  if ((i - 1) >= 0) {
    brother_page = buffer_pool_manager->FetchPage(ValueAt(i - 1));
    brother_page->WLatch();
    key = KeyAt(i);
    isPre = true;
    return;
  }
  brother_page = buffer_pool_manager->FetchPage(ValueAt(i + 1));
  brother_page->WLatch();
  key = KeyAt(i + 1);
  isPre = false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBrotherPageRW(page_id_t child_page_id, Page *&brother_page, KeyType &key,
                                                      bool &isPre, BufferPoolManager *buffer_pool_manager,
                                                      Transaction *transaction) -> void {
  int i = 0;
  for (i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == child_page_id) {
      break;
    }
  }
  if ((i - 1) >= 0) {
    brother_page = buffer_pool_manager->FetchPage(ValueAt(i - 1));
    brother_page->WLatch();
    transaction->AddIntoPageSet(brother_page);
    key = KeyAt(i);
    isPre = true;
    return;
  }
  brother_page = buffer_pool_manager->FetchPage(ValueAt(i + 1));
  brother_page->WLatch();
  transaction->AddIntoPageSet(brother_page);
  key = KeyAt(i + 1);
  isPre = false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(const KeyType &key, Page *brother_page, Page *parent_page,
                                           const KeyComparator &comparator, BufferPoolManager *buffer_pool_manager_)
    -> void {
  // 定义一个临时数组 tmp 用于保存待分裂节点的所有键值对
  auto *temp = static_cast<MappingType *>(malloc(sizeof(MappingType) * (GetMaxSize() + 1)));
  temp[0] = array_[0];
  int i = 1;
  // 将原节点的第一个键值对复制到 tmp 数组的第一个位置，然后将其余键值对复制到 tmp 数组中的后续位置。
  for (; i < GetMaxSize(); ++i) {
    temp[i] = array_[i];
  }
  // 从 tmp 数组的最后一个位置开始往前遍历，寻找需要插入的位置
  for (i = GetMaxSize() - 1; i > 0; --i) {
    if (comparator(temp[i].first, key) > 0) {
      temp[i + 1] = temp[i];
    } else {
      temp[i + 1] = std::make_pair(key, brother_page->GetPageId());
      break;
    }
  }
  if (i == 0) {
    temp[1] = std::make_pair(key, brother_page->GetPageId());
  }
  // 分裂之后也应该让node都处于半满状态 所以向上取整然后取到mid，将mid前后的内容分别插入
  int mid = (GetMaxSize() + 1) / 2;
  auto parent_page_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(parent_page->GetData());
  auto brother_page_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(brother_page->GetData());
  brother_page_node->SetParentPageId(GetPageId());
  IncreaseSize(1);
  for (i = 0; i < mid; ++i) {
    array_[i] = temp[i];
  }
  i = 0;
  while (mid <= (GetMaxSize())) {
    Page *child = buffer_pool_manager_->FetchPage(temp[mid].second);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child->GetData());
    child_node->SetParentPageId(parent_page_node->GetPageId());
    parent_page_node->array_[i] = temp[mid];
    i++;
    mid++;
    parent_page_node->IncreaseSize(1);
    IncreaseSize(-1);
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
  free(temp);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(const KeyType &key, bustub::Page *right_page,
                                           bustub::BufferPoolManager *buffer_pool_manager) -> void {
  auto right = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(right_page->GetData());
  int size = GetSize();
  // 将右兄弟节点的第0位的value获取填充到当前节点的array中
  array_[GetSize()] = std::make_pair(key, right->ValueAt(0));
  IncreaseSize(1);

  // 将右兄弟节点的kv全部移动到当前节点
  for (int i = GetSize(), j = 1; j < right->GetSize(); ++i, ++j) {
    array_[i] = std::make_pair(right->KeyAt(j), right->ValueAt(j));
    IncreaseSize(1);
  }
  // 右兄弟节点空了从缓冲池中删掉他 注意删掉之前需要unpin
  right_page->WUnlatch();
  buffer_pool_manager->UnpinPage(right_page->GetPageId(), true);
  buffer_pool_manager->DeletePage(right_page->GetPageId());

  // 将右兄弟节点的子节点也转移
  for (int i = size; i < GetSize(); ++i) {
    page_id_t child_page_id = ValueAt(i);
    auto child_page = buffer_pool_manager->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(child_page->GetData());
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
