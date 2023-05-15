#include <string>

#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/**********************auto *******************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  Page *page = FindLeafPage(key, transaction, READ);
  if (page == nullptr) {
    return false;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  int index = leaf_page->KeyIndex(key, comparator_);
  if (index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    result->push_back(leaf_page->ValueAt(index));
    if (transaction != nullptr) {
      UnlockAndUnpin(transaction, READ);
    } else {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    }
    return true;
  }
  if (transaction != nullptr) {
    UnlockAndUnpin(transaction, READ);
  } else {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }
  return false;
}

// 找到叶子节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bustub::Transaction *transaction, bustub::Operation op)
    -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  // 为了找到正确的root_page 因为
  while (true) {
    if (curr_page == nullptr) {
      return nullptr;
    }
    if (op == READ) {
      curr_page->RLatch();
    } else {
      curr_page->WLatch();
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(curr_page);
    }
    if (root_page_id_ == curr_page->GetPageId()) {
      break;
    }
    if (op == READ) {
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }
    } else {
      UnlockAndUnpin(transaction, op);
    }
    // 更新当前节点页位置为根节点
    curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  }
  auto curr_page_node = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_page_node->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_page_node->Lookup(key, comparator_));
    // 如果是读操作 那么经过的节点都可以直接解锁 因为读操作不会对节点进行修改 因此也不会影响之前的节点
    if (op == READ) {
      next_page->RLatch();
      if (transaction != nullptr) {
        UnlockAndUnpin(transaction, op);
      } else {
        curr_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
      }
    } else {
      next_page->WLatch();
      if (IsSafe(next_page, op)) {
        UnlockAndUnpin(transaction, op);
      }
    }
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(next_page);
    }
    auto next_page_node = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page = next_page;
    curr_page_node = next_page_node;
  }
  return curr_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetMaxSize(bustub::BPlusTreePage *page) const -> int {
  return page->IsLeafPage() ? leaf_max_size_ - 1 : internal_max_size_;
}

// 判断对于插入删除是否安全
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(Page *page, Operation operation) -> bool {
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  // 对于插入
  if (operation == INSERT) {
    // 当前节点的大小小于最大大小时是安全的
    return node->GetSize() < GetMaxSize(node);
  }
  // 如果当前节点是根节点
  if (node->GetParentPageId() == INVALID_PAGE_ID) {
    // 如果当前根节点是叶子节点，则大小可以任意，即插入或删除任意数量的元素都是安全的
    if (node->IsLeafPage()) {
      return true;
    }
    // 否则必须大于2才是安全的
    return node->GetSize() > 2;
  }
  // 对于其他节点必须要当前节点的大小大于最小限制才是安全的
  return node->GetSize() > node->GetMinSize();
}

// 释放事务持有的所有页面资源，并将这些页面从事务的页面集合和已删除页面集合中移除。
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UnlockAndUnpin(bustub::Transaction *transaction, bustub::Operation op) -> void {
  if (transaction == nullptr) {
    return;
  }
  for (auto page : *transaction->GetPageSet()) {
    if (op == READ) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();
  for (auto page : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  Page *leaf_page = FindLeafPage(key, transaction, INSERT);
  std::cout << __FUNCTION__ << "[" << __LINE__ << "]" << "key=" << key <<" " << "value=" << value << std::endl;
  // 如果是空树那就需要对树进行加锁然后构建出节点
  while (leaf_page == nullptr) {
    latch_.lock();
    if (IsEmpty()) {
      page_id_t page_id;
      Page *page = buffer_pool_manager_->NewPage(&page_id);
      auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      root_page_id_ = page_id;
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    latch_.unlock();
    // 重新获取待插入的叶节点
    leaf_page = FindLeafPage(key, transaction, INSERT);
  }

  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = leaf_node->KeyIndex(key, comparator_);
  bool res = leaf_node->Insert(std::make_pair(key, value), index, comparator_);
  if (!res) {
    UnlockAndUnpin(transaction, INSERT);
    return false;
  }
  // 如果插入成功了,这里需要判断是否安全
  if (leaf_node->GetSize() == leaf_max_size_) {
    // 节点的最大数量达到max 分裂
    page_id_t brother_page_id;
    Page *brother_page = buffer_pool_manager_->NewPage(&brother_page_id);
    auto brother_node = reinterpret_cast<LeafPage *>(brother_page->GetData());
    brother_node->Init(brother_page_id, INVALID_PAGE_ID, leaf_max_size_);

    // 分裂 然后将后半部分划分给兄弟节点
    leaf_node->Split(brother_page);
    // 因为分裂出了一个兄弟节点 所以也需要对父节点插入
    InsertInParent(leaf_page, brother_node->KeyAt(0), brother_page, transaction);
    buffer_pool_manager_->UnpinPage(brother_page->GetPageId(), true);
    UnlockAndUnpin(transaction, INSERT);
  }
  for (int i = 0; i < leaf_node->GetSize(); ++i) {
    std::cout << leaf_node->ValueAt(i) << " ";
  }
  std::cout << std::endl;
  UnlockAndUnpin(transaction, INSERT);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInParent(bustub::Page *leaf_page, const KeyType &key, bustub::Page *brother_page,
                                    bustub::Transaction *transaction) -> void {
  auto tree_page = reinterpret_cast<BPlusTreePage *>(leaf_page->GetData());
  // 如果当前节点为最顶层，则需要创建一个新的根节点，整个b+t增高一层
  if (tree_page->GetParentPageId() == INVALID_PAGE_ID) {
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->SetValueAt(0, leaf_page->GetPageId());
    new_root->SetKeyAt(1, key);
    new_root->SetValueAt(1, brother_page->GetPageId());
    new_root->IncreaseSize(2);
    auto page_leaf_node = reinterpret_cast<BPlusTreePage *>(leaf_page->GetData());
    page_leaf_node->SetParentPageId(new_page_id);
    auto page_bother_node = reinterpret_cast<BPlusTreePage *>(brother_page->GetData());
    page_bother_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return;
  }
  page_id_t parent_id = tree_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  auto page_bother_node = reinterpret_cast<InternalPage *>(brother_page->GetData());
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(std::make_pair(key, brother_page->GetPageId()), comparator_);
    page_bother_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(parent_id, true);
    return;
  }
  page_id_t parent_bother_page_id;
  Page *page_parent_bother = buffer_pool_manager_->NewPage(&parent_bother_page_id);
  auto parent_bother_node = reinterpret_cast<InternalPage *>(page_parent_bother->GetData());
  parent_bother_node->Init(parent_bother_page_id, INVALID_PAGE_ID, internal_max_size_);
  parent_node->Split(key, brother_page, page_parent_bother, comparator_, buffer_pool_manager_);
  InsertInParent(parent_page, parent_bother_node->KeyAt(0), page_parent_bother, transaction);
  buffer_pool_manager_->UnpinPage(parent_bother_page_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  Page *leaf_page = FindLeafPage(key, transaction, DELETE);
  if (leaf_page == nullptr) {
    return;
  }
  DeleteEntry(leaf_page, key, transaction);
  // 在FindLeafPage的时候加了锁这里需要释放
  UnlockAndUnpin(transaction, DELETE);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntry(Page *&page, const KeyType &key, Transaction *transaction) -> void {
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (node->IsLeafPage()) {
    auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
    if (!leaf_node->Delete(key, comparator_)) {
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return;
    }
  } else {
    auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
    if (!inter_node->Delete(key, comparator_)) {
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      return;
    }
  }
  // 在当前页上成功将key对应的对删除
  // 当前节点为根节点
  if (root_page_id_ == node->GetPageId()) {
    // 如果当前节点是根节点 同时也是叶子节点 并且当前的size等于0让这棵树变成空树
    if (root_page_id_ == node->GetPageId() && node->IsLeafPage() && node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());
      return;
    }
    // 根节点，且为中间节点，size最少为2，若只为1，说明其只有一个孩子，孩子应该为新的根节点
    if (root_page_id_ == node->GetPageId() && node->IsRootPage() && node->GetSize() == 1) {
      auto inter_node = reinterpret_cast<InternalPage *>(page->GetData());
      root_page_id_ = inter_node->ValueAt(0);
      transaction->GetPageSet()->pop_back();
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(page->GetPageId());
      return;
    }
    // 将当前页面的锁定和内存缓存释放，并将其从事务的页面集合中移除，以便在之后的操作中能够正确地访问页面
    transaction->GetPageSet()->pop_back();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return;
  }
  //  如果节点不是根节点 并且小于minisize 就需要考虑合并或者借用
  if (node->GetSize() < node->GetMinSize()) {
    Page *brother_page;
    // 父节点中指向 node 的指针所对应的键值
    KeyType parent_key;
    bool ispre;

    // 这里是按照顺序将page装入PageSet的 所以-1应该就是当前的页面 -2就是父节点的页面
    auto parent_page = (*transaction->GetPageSet())[transaction->GetPageSet()->size() - 2];
    auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

    parent_node->GetBrotherPage(page->GetPageId(), brother_page, parent_key, ispre, buffer_pool_manager_);
    auto brother_node = reinterpret_cast<BPlusTreePage *>(brother_page->GetData());
    // 如果当前的叶子节点和兄弟节点都很小 相加并不会达到maxsize 就将他们合并
    if (brother_node->GetSize() + node->GetSize() <= GetMaxSize(node)) {
      // 如果不是当前节点的前一个节点
      // 如果兄弟节点在当前节点的后面 交换他们
      if (!ispre) {
        auto temp_page = page;
        page = brother_page;
        brother_page = temp_page;
        auto temp_node = node;
        node = brother_node;
        brother_node = temp_node;
      }
      if (node->IsRootPage()) {
        auto inter_brother_node = reinterpret_cast<InternalPage *>(brother_page->GetData());
        inter_brother_node->Merge(parent_key, page, buffer_pool_manager_);
        transaction->GetPageSet()->pop_back();
        brother_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(inter_brother_node->GetPageId(), true);
      } else {
        auto brother_leaf_node = reinterpret_cast<LeafPage *>(brother_page->GetData());
        auto curr_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
        auto next_page_id = curr_leaf_node->GetNextPageId();
        brother_leaf_node->Merge(page, buffer_pool_manager_);
        brother_leaf_node->SetNextPageId(next_page_id);
        transaction->GetPageSet()->pop_back();
        brother_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(brother_leaf_node->GetPageId(), true);
      }

      DeleteEntry(parent_page, parent_key, transaction);

    } else {
      // 借用brother的，只需要调整不需要收缩
      if (ispre) {  // 兄弟节点在左边的情况
        if (brother_node->IsRootPage()) {
          auto brother_inter_node = reinterpret_cast<InternalPage *>(brother_page->GetData());
          auto curr_inter_node = reinterpret_cast<InternalPage *>(page->GetData());

          page_id_t last_value = brother_inter_node->ValueAt(brother_inter_node->GetSize() - 1);
          KeyType last_key = brother_inter_node->KeyAt(brother_inter_node->GetSize() - 1);
          brother_inter_node->Delete(last_key, comparator_);

          brother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(brother_inter_node->GetPageId(), true);

          curr_inter_node->InsertFirst(parent_key, last_value);

          auto child_page = buffer_pool_manager_->FetchPage(last_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

          if (child_node->IsLeafPage()) {
            auto child_leaf_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            child_leaf_node->SetParentPageId(curr_inter_node->GetPageId());
          } else {
            auto child_inter_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            child_inter_node->SetParentPageId(curr_inter_node->GetPageId());
          }

          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

          auto parent_inter_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = parent_inter_node->KeyIndex(parent_key, comparator_);
          parent_inter_node->SetKeyAt(index, last_key);
        } else {
          auto brother_leaf_node = reinterpret_cast<LeafPage *>(brother_page->GetData());
          auto curr_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType last_value = brother_leaf_node->ValueAt(brother_leaf_node->GetSize() - 1);
          KeyType last_key = brother_leaf_node->KeyAt(brother_leaf_node->GetSize() - 1);

          brother_leaf_node->Delete(last_key, comparator_);
          curr_leaf_node->InsertFirst(last_key, last_value);

          brother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(brother_leaf_node->GetPageId(), true);

          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

          auto parent_inter_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = parent_inter_node->KeyIndex(parent_key, comparator_);
          parent_inter_node->SetKeyAt(index, last_key);
        }
      } else {  // 兄弟节点在右边的情况
        if (brother_node->IsRootPage()) {
          auto brother_inter_node = reinterpret_cast<InternalPage *>(brother_page->GetData());
          auto curr_inter_node = reinterpret_cast<InternalPage *>(page->GetData());

          page_id_t first_value = brother_inter_node->ValueAt(0);
          KeyType first_key = brother_inter_node->KeyAt(1);
          brother_inter_node->DeleteFirst();

          brother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(brother_page->GetPageId(), true);

          curr_inter_node->Insert(std::make_pair(parent_key, first_value), comparator_);
          auto child_page = buffer_pool_manager_->FetchPage(first_value);
          auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

          if (child_node->IsLeafPage()) {
            auto child_leaf_node = reinterpret_cast<LeafPage *>(child_page->GetData());
            child_leaf_node->SetParentPageId(curr_inter_node->GetPageId());
          } else {
            auto child_inter_node = reinterpret_cast<InternalPage *>(child_page->GetData());
            child_inter_node->SetParentPageId(curr_inter_node->GetPageId());
          }
          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
          auto parent_inter_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = parent_inter_node->KeyIndex(parent_key, comparator_);
          parent_inter_node->SetKeyAt(index, first_key);
        } else {
          auto brother_leaf_node = reinterpret_cast<LeafPage *>(brother_page->GetData());
          auto curr_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
          ValueType first_value = brother_leaf_node->ValueAt(0);
          KeyType first_key = brother_leaf_node->KeyAt(0);

          brother_leaf_node->Delete(first_key, comparator_);
          curr_leaf_node->InsertLast(first_key, first_value);

          brother_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(brother_page->GetPageId(), true);
          transaction->GetPageSet()->pop_back();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
          auto parent_inter_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
          int index = parent_inter_node->KeyIndex(parent_key, comparator_);
          parent_inter_node->SetKeyAt(index, brother_leaf_node->KeyAt(0));
        }
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_node = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_node->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_node->ValueAt(0));
    next_page->RLatch();
    auto next_node = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_node = next_node;
  }
  return INDEXITERATOR_TYPE(curr_page->GetPageId(), curr_page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *leaf_page = FindLeafPage(key, nullptr, READ);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index;
  for (index = 0; index < leaf_node->GetSize(); ++index) {
    if (comparator_(leaf_node->KeyAt(index), key) == 0) {
      break;
    }
  }
  if (index == leaf_node->GetSize()) {
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return End();
  }
  return INDEXITERATOR_TYPE(leaf_page->GetPageId(), leaf_page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  Page *curr_page = buffer_pool_manager_->FetchPage(root_page_id_);
  curr_page->RLatch();
  auto curr_inter_node = reinterpret_cast<InternalPage *>(curr_page->GetData());
  while (!curr_inter_node->IsLeafPage()) {
    Page *next_page = buffer_pool_manager_->FetchPage(curr_inter_node->ValueAt(curr_inter_node->GetSize() - 1));
    next_page->RLatch();
    auto next_inter_node = reinterpret_cast<InternalPage *>(next_page->GetData());
    curr_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
    curr_page = next_page;
    curr_inter_node = next_inter_node;
  }
  auto curr_leaf_node = reinterpret_cast<LeafPage *>(curr_page->GetData());
  page_id_t page_id = curr_page->GetPageId();
  curr_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(curr_page->GetPageId(), false);
  return INDEXITERATOR_TYPE(page_id, curr_page, curr_leaf_node->GetSize(), buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
