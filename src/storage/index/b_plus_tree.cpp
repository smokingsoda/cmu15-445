
#include <iostream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
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

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, page_id_t *page_id) -> bool {
  Page *root_page_with_page_type = this->buffer_pool_manager_->FetchPage(this->GetRootPageId());
  auto *root_page = reinterpret_cast<BPlusTreePage *>(root_page_with_page_type->GetData());
  if (root_page->IsLeafPage()) {
    *page_id = root_page->GetPageId();
    this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
    return true;
  }
  auto *root_page_internal = reinterpret_cast<InternalPage *>(root_page);
  auto index = root_page_internal->BisectPosition(key, this->comparator_);
  page_id_t target_page_id;
  if (index == root_page_internal->GetSize() - 1) {
    target_page_id = root_page_internal->ValueAt(index);
  } else {
    auto key_at_index_plus_one = root_page_internal->KeyAt(index + 1);
    if (this->comparator_(key_at_index_plus_one, key) == 0) {
      target_page_id = root_page_internal->ValueAt(index + 1);
    } else {
      target_page_id = root_page_internal->ValueAt(index);
    }
  }
  // Unpin root page
  this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
  Page *target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  while (!target_page->IsLeafPage()) {
    auto *target_page_internal = reinterpret_cast<InternalPage *>(target_page);
    index = target_page_internal->BisectPosition(key, this->comparator_);
    page_id_t new_target_page_id;
    if (index == target_page_internal->GetSize() - 1) {
      new_target_page_id = target_page_internal->ValueAt(index);
    } else {
      auto key_at_index_plus_one = target_page_internal->KeyAt(index + 1);
      if (this->comparator_(key_at_index_plus_one, key) == 0) {
        new_target_page_id = target_page_internal->ValueAt(index + 1);
      } else {
        new_target_page_id = target_page_internal->ValueAt(index);
      }
    }
    this->buffer_pool_manager_->UnpinPage(target_page_id, false);
    target_page_id = new_target_page_id;
    target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
    target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  }
  this->buffer_pool_manager_->UnpinPage(target_page_id, false);
  *page_id = target_page->GetPageId();
  return true;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  page_id_t target_page_id = INVALID_PAGE_ID;
  if (this->IsEmpty()) {
    return false;
  }
  BUSTUB_ASSERT((this->FindLeaf(key, &target_page_id)), "No");
  if (target_page_id == INVALID_PAGE_ID) {
    return false;
  }
  Page *target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page_general = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  auto *target_page_leaf = reinterpret_cast<LeafPage *>(target_page_general);
  ValueType value;
  auto index = target_page_leaf->BisectPosition(key, this->comparator_);
  if (index + 1 >= target_page_leaf->GetSize()) {
    this->buffer_pool_manager_->UnpinPage(target_page_id, false);
    return false;
  }
  if (this->comparator_(target_page_leaf->KeyAt(index + 1), key) == 0) {
    value = target_page_leaf->ValueAt(index + 1);
    result->push_back(value);
    this->buffer_pool_manager_->UnpinPage(target_page_id, false);
    return true;
  }
  this->buffer_pool_manager_->UnpinPage(target_page_id, false);
  return false;
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
  if (this->IsEmpty()) {
    Page *root_page_with_page_type = this->buffer_pool_manager_->NewPage(&this->root_page_id_);
    UpdateRootPageId(0);
    auto *root_page = reinterpret_cast<LeafPage *>(root_page_with_page_type->GetData());
    root_page->Init(this->GetRootPageId(), INVALID_PAGE_ID, this->leaf_max_size_);
    root_page->SetNextPageId(INVALID_PAGE_ID);
    root_page->InsertAt(0, key, value);
    // Unpin leaf pages
    this->buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  page_id_t target_page_id;
  BUSTUB_ASSERT(this->FindLeaf(key, &target_page_id), "No");
  Page *target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page_general = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  auto *target_page_leaf = reinterpret_cast<LeafPage *>(target_page_general);
  auto index = target_page_leaf->BisectPosition(key, this->comparator_);
  KeyType target_key = target_page_leaf->KeyAt(index + 1);
  if (this->comparator_(target_key, key) == 0) {
    this->buffer_pool_manager_->UnpinPage(target_page_id, true);
    return false;
  }
  target_page_leaf->InsertAt(index + 1, key, value);
  // Check size
  if (target_page_leaf->GetSize() >= target_page_leaf->GetMaxSize()) {
    page_id_t new_page_id;
    Page *new_page_with_page_type = this->buffer_pool_manager_->NewPage(&new_page_id);
    auto *new_page_general = reinterpret_cast<BPlusTreePage *>(new_page_with_page_type->GetData());
    auto *new_page_leaf = reinterpret_cast<LeafPage *>(new_page_general);
    new_page_leaf->Init(new_page_id, target_page_leaf->GetParentPageId(), target_page_leaf->GetMaxSize());
    // What if it is a root page? Should we use MinSize?
    if (!target_page_leaf->IsRootPage()) {
      new_page_leaf->RedistributeFrom(target_page_leaf, target_page_leaf->GetMinSize());
    } else {
      new_page_leaf->RedistributeFrom(target_page_leaf, target_page_leaf->GetSize() / 2);
    }
    auto old_next_page_id = target_page_leaf->GetNextPageId();
    target_page_leaf->SetNextPageId(new_page_id);
    new_page_leaf->SetNextPageId(old_next_page_id);
    KeyType insert_key = new_page_leaf->KeyAt(0);
    page_id_t insert_page_id = new_page_id;
    // Unpin leaf pages, put it down
    this->buffer_pool_manager_->UnpinPage(target_page_id, true);
    this->buffer_pool_manager_->UnpinPage(new_page_id, true);
    // Check if it is a root page
    if (target_page_leaf->IsRootPage()) {
      Page *root_page_with_page_type = this->buffer_pool_manager_->NewPage(&this->root_page_id_);
      UpdateRootPageId(0);
      auto *root_page = reinterpret_cast<InternalPage *>(root_page_with_page_type->GetData());
      root_page->Init(this->GetRootPageId(), INVALID_PAGE_ID, this->internal_max_size_);
      // Update pointers
      root_page->SetValueAt(0, target_page_id);
      root_page->SetKeyAt(1, insert_key);
      root_page->SetValueAt(1, insert_page_id);
      root_page->IncrementSize();
      root_page->UpdateChildrenPointers(this->buffer_pool_manager_);
      // Unpin leaf pages
      this->buffer_pool_manager_->UnpinPage(root_page_id_, true);
      return true;
    }
    // Now handle parent node
    auto parent_page_id = target_page_leaf->GetParentPageId();
    Page *parent_page_with_page_type = this->buffer_pool_manager_->FetchPage(parent_page_id);
    auto *parent_page_general = reinterpret_cast<BPlusTreePage *>(parent_page_with_page_type->GetData());
    auto *parent_page_internal = reinterpret_cast<InternalPage *>(parent_page_general);
    while (parent_page_internal->GetSize() >= parent_page_internal->GetMaxSize()) {
      // Insert the key into parent node
      index = parent_page_internal->BisectPosition(insert_key, this->comparator_);
      parent_page_internal->InsertAt(index + 1, insert_key, insert_page_id);
      // Split parent node
      // New a parent page
      page_id_t new_parent_page_id;
      Page *new_parent_page_with_page_type = this->buffer_pool_manager_->NewPage(&new_parent_page_id);
      auto *new_parent_page_general = reinterpret_cast<BPlusTreePage *>(new_parent_page_with_page_type->GetData());
      auto *new_parent_page_internal = reinterpret_cast<InternalPage *>(new_parent_page_general);
      new_parent_page_internal->Init(new_parent_page_id, parent_page_internal->GetParentPageId(),
                                     parent_page_internal->GetMaxSize());
      // Redistribute keys and values
      new_parent_page_internal->RedistributeFrom(parent_page_internal,
                                                 parent_page_internal->GetSize() / 2);  // Here is bug
      insert_key = new_parent_page_internal->Rearrange();
      insert_page_id = new_parent_page_id;
      // Update child pages pointers
      parent_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
      new_parent_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
      // Check if it is the root page
      if (parent_page_internal->IsRootPage()) {
        // Routine
        page_id_t new_root_page_id;
        Page *new_root_page_with_page_type = this->buffer_pool_manager_->NewPage(&new_root_page_id);
        auto *new_root_page_general = reinterpret_cast<BPlusTreePage *>(new_root_page_with_page_type->GetData());
        auto *new_root_page_internal = reinterpret_cast<InternalPage *>(new_root_page_general);
        // Init
        new_root_page_internal->Init(new_root_page_id, INVALID_PAGE_ID, parent_page_internal->GetMaxSize());
        // Insert the last key into root page
        new_root_page_internal->InsertAt(1, insert_key, new_parent_page_id);
        new_root_page_internal->SetValueAt(0, parent_page_id);
        new_root_page_internal->SetValueAt(1, new_parent_page_id);
        new_root_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
        this->root_page_id_ = new_root_page_id;
        UpdateRootPageId(0);
        // Unpin
        this->buffer_pool_manager_->UnpinPage(parent_page_id, true);
        this->buffer_pool_manager_->UnpinPage(new_parent_page_id, true);
        this->buffer_pool_manager_->UnpinPage(new_root_page_id, true);
        return true;
      }
      // Unpin parent pages
      auto old_parent_page_id = parent_page_id;
      // Change the variables
      parent_page_id = parent_page_internal->GetParentPageId();
      // Unpin parent pages
      this->buffer_pool_manager_->UnpinPage(old_parent_page_id, true);
      this->buffer_pool_manager_->UnpinPage(new_parent_page_id, true);
      parent_page_with_page_type = this->buffer_pool_manager_->FetchPage(parent_page_id);
      parent_page_general = reinterpret_cast<BPlusTreePage *>(parent_page_with_page_type->GetData());
      parent_page_internal = reinterpret_cast<InternalPage *>(parent_page_general);
    }
    index = parent_page_internal->BisectPosition(insert_key, this->comparator_);
    parent_page_internal->InsertAt(index + 1, insert_key, insert_page_id);
    this->buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return true;
  }
  // Insertion without overflow
  this->buffer_pool_manager_->UnpinPage(target_page_id, true);
  return true;

  this->buffer_pool_manager_->UnpinPage(target_page_id, false);
  return false;
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
  if (this->IsEmpty()) {
    return;
  }
  page_id_t target_page_id;
  BUSTUB_ASSERT(this->FindLeaf(key, &target_page_id), "No");
  Page *target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page_general = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  auto *target_page_leaf = reinterpret_cast<LeafPage *>(target_page_general);
  if (target_page_leaf->IsRootPage()) {
    // Do something for root page
    if (target_page_leaf->GetSize() == 1) {
      this->root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      this->buffer_pool_manager_->UnpinPage(target_page_id, false);
      this->buffer_pool_manager_->DeletePage(target_page_id);
      return;
    }
  }
  auto index = target_page_leaf->BisectPosition(key, this->comparator_);
  KeyType target_key = target_page_leaf->KeyAt(index + 1);
  if (this->comparator_(target_key, key) != 0) {
    this->buffer_pool_manager_->UnpinPage(target_page_id, false);
    return;
  }
  target_page_leaf->RemoveAt(index + 1);
  if (target_page_leaf->GetSize() >= target_page_leaf->GetMinSize()) {
    this->buffer_pool_manager_->UnpinPage(target_page_id, true);
    return;
  }
  // Handle underflow
  bool is_merge;
  bool is_right;
  page_id_t sibling_page_id;
  target_page_leaf->StealOrMerge(&is_merge, &is_right, this->buffer_pool_manager_, this->comparator_, &sibling_page_id,
                                 &index);
  Page *sibling_page_with_page_type = this->buffer_pool_manager_->FetchPage(sibling_page_id);
  auto *sibling_page_general = reinterpret_cast<BPlusTreePage *>(sibling_page_with_page_type->GetData());
  auto *sibling_page_leaf = reinterpret_cast<LeafPage *>(sibling_page_general);
  Page *parent_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_leaf->GetParentPageId());
  auto *parent_page_general = reinterpret_cast<BPlusTreePage *>(parent_page_with_page_type->GetData());
  auto *parent_page_internal = reinterpret_cast<InternalPage *>(parent_page_general);
  if (!is_merge) {
    KeyType stolen_key;
    ValueType stolen_value;
    if (is_right) {
      stolen_key = sibling_page_leaf->KeyAt(0);
      stolen_value = sibling_page_leaf->ValueAt(0);
      sibling_page_leaf->RemoveAt(0);
      target_page_leaf->InsertAt(target_page_leaf->GetSize(), stolen_key, stolen_value);
      parent_page_internal->SetKeyAt(index, sibling_page_leaf->KeyAt(0));
    } else {
      stolen_key = sibling_page_leaf->KeyAt(sibling_page_leaf->GetSize() - 1);
      stolen_value = sibling_page_leaf->ValueAt(sibling_page_leaf->GetSize() - 1);
      sibling_page_leaf->RemoveAt(sibling_page_leaf->GetSize() - 1);
      target_page_leaf->InsertAt(0, stolen_key, stolen_value);
      parent_page_internal->SetKeyAt(index + 1, target_page_leaf->KeyAt(0));
    }
    this->buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    this->buffer_pool_manager_->UnpinPage(target_page_id, true);
    this->buffer_pool_manager_->UnpinPage(parent_page_internal->GetPageId(), true);
    return;
  }
  // Merge
  LeafPage *new_leaf;
  page_id_t delete_page_id;
  if (is_right) {
    target_page_leaf->MergeWith(sibling_page_leaf, true);
    target_page_leaf->SetNextPageId(sibling_page_leaf->GetNextPageId());
    new_leaf = target_page_leaf;
    delete_page_id = sibling_page_id;
  } else {
    sibling_page_leaf->MergeWith(target_page_leaf, true);
    sibling_page_leaf->SetNextPageId(target_page_leaf->GetNextPageId());
    new_leaf = sibling_page_leaf;
    delete_page_id = target_page_id;
  }
  this->buffer_pool_manager_->UnpinPage(target_page_leaf->GetPageId(), true);
  this->buffer_pool_manager_->UnpinPage(sibling_page_leaf->GetPageId(), true);
  this->buffer_pool_manager_->DeletePage(delete_page_id);
  // Handle parent node
  if (is_right) {
    parent_page_internal->RemoveAt(index);
  } else {
    parent_page_internal->RemoveAt(index + 1);
  }

  while (parent_page_internal->GetSize() < parent_page_internal->GetMinSize()) {
    // First handle root page
    if (parent_page_internal->IsRootPage()) {
      this->root_page_id_ = parent_page_internal->ValueAt(0);
      Page *new_root_page_with_page_type = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
      auto *new_root_page_general = reinterpret_cast<BPlusTreePage *>(new_root_page_with_page_type->GetData());
      new_root_page_general->SetParentPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
      this->buffer_pool_manager_->UnpinPage(this->root_page_id_, true);
      this->buffer_pool_manager_->UnpinPage(parent_page_internal->GetPageId(), true);
      this->buffer_pool_manager_->DeletePage(parent_page_internal->GetPageId());
      return;
    }
    parent_page_internal->StealOrMerge(&is_merge, &is_right, this->buffer_pool_manager_, this->comparator_,
                                       &sibling_page_id, &index);
    sibling_page_with_page_type = this->buffer_pool_manager_->FetchPage(sibling_page_id);
    sibling_page_general = reinterpret_cast<BPlusTreePage *>(sibling_page_with_page_type->GetData());
    auto sibling_page_internal = reinterpret_cast<InternalPage *>(sibling_page_general);
    Page *grand_parent_page_with_page_type =
        this->buffer_pool_manager_->FetchPage(parent_page_internal->GetParentPageId());
    auto *grand_parent_page_general = reinterpret_cast<BPlusTreePage *>(grand_parent_page_with_page_type->GetData());
    auto *grand_parent_page_internal = reinterpret_cast<InternalPage *>(grand_parent_page_general);
    auto sibling_index = grand_parent_page_internal->BisectPosition(sibling_page_internal->KeyAt(1), this->comparator_);
    if (!is_merge) {
      KeyType stolen_key;
      page_id_t stolen_page_id;
      if (is_right) {
        stolen_key = grand_parent_page_internal->KeyAt(sibling_index);
        stolen_page_id = sibling_page_internal->ValueAt(0);
        parent_page_internal->InsertAt(parent_page_internal->GetSize(), stolen_key, stolen_page_id);
        grand_parent_page_internal->SetKeyAt(sibling_index, sibling_page_internal->KeyAt(1));
        sibling_page_internal->RemoveAt(0);
      } else {
        stolen_key = grand_parent_page_internal->KeyAt(sibling_index + 1);
        stolen_page_id = sibling_page_internal->ValueAt(sibling_page_internal->GetSize() - 1);
        parent_page_internal->InsertAt(0, stolen_key, stolen_page_id);
        parent_page_internal->SetKeyAt(1, stolen_key); // Here used to be a bug
        grand_parent_page_internal->SetKeyAt(sibling_index + 1,
                                             sibling_page_internal->KeyAt(sibling_page_internal->GetSize() - 1));
        sibling_page_internal->RemoveAt(sibling_page_internal->GetSize() - 1);
      }
      parent_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
      sibling_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
      grand_parent_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
      this->buffer_pool_manager_->UnpinPage(sibling_page_internal->GetParentPageId(), true);
      this->buffer_pool_manager_->UnpinPage(parent_page_internal->GetPageId(), true);
      this->buffer_pool_manager_->UnpinPage(grand_parent_page_internal->GetPageId(), true);
      return;
    }
    // Merge
    InternalPage *new_internal;
    page_id_t delete_internal_page_id;
    if (is_right) {
      auto it_key = grand_parent_page_internal->RemoveAt(sibling_index);
      auto it_page_id = sibling_page_internal->ValueAt(0);
      auto limit = sibling_page_internal->GetSize();
      for (int i = 0; i < limit; i++) {
        parent_page_internal->InsertAt(parent_page_internal->GetSize(), it_key, it_page_id);
        it_key = sibling_page_internal->KeyAt(i + 1);
        it_page_id = sibling_page_internal->ValueAt(i + 1);
      }
      new_internal = parent_page_internal;
      delete_internal_page_id = sibling_page_id;
    } else {
      auto it_key = grand_parent_page_internal->RemoveAt(sibling_index + 1);
      auto it_page_id = parent_page_internal->ValueAt(0);
      auto limit = parent_page_internal->GetSize();
      for (int i = 0; i < limit; i++) {
        sibling_page_internal->InsertAt(sibling_page_internal->GetSize(), it_key, it_page_id);
        it_key = parent_page_internal->KeyAt(i + 1);
        it_page_id = parent_page_internal->ValueAt(i + 1);  // Here used to be a bug
      }
      new_internal = sibling_page_internal;
      delete_internal_page_id = parent_page_internal->GetPageId();
    }
    new_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
    for (int i = 0; i < new_internal->GetSize(); i++) {
      auto child_page_id = new_internal->ValueAt(i);
      Page *child_page_with_page_type = this->buffer_pool_manager_->FetchPage(child_page_id);
      auto *child_page_general = reinterpret_cast<BPlusTreePage *>(child_page_with_page_type->GetData());
      if (child_page_general->IsLeafPage()) {
        auto *child_page_leaf = reinterpret_cast<LeafPage *>(child_page_general);
        if (i < new_internal->GetSize() - 1 && child_page_leaf->GetNextPageId() != new_internal->ValueAt(i + 1)) {
          child_page_leaf->SetNextPageId(new_internal->ValueAt(i + 1));
          this->buffer_pool_manager_->UnpinPage(child_page_id, true);
          continue;
        }
        this->buffer_pool_manager_->UnpinPage(child_page_id, false);
      }
    }
    grand_parent_page_internal->UpdateChildrenPointers(this->buffer_pool_manager_);
    this->buffer_pool_manager_->UnpinPage(parent_page_internal->GetPageId(), true);
    this->buffer_pool_manager_->UnpinPage(sibling_page_internal->GetPageId(), true);
    this->buffer_pool_manager_->DeletePage(delete_internal_page_id);
    parent_page_internal = grand_parent_page_internal;
  }
  this->buffer_pool_manager_->UnpinPage(parent_page_internal->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeftMostLeaf() -> page_id_t {
  auto root_page_with_page_type = this->buffer_pool_manager_->FetchPage(this->GetRootPageId());
  auto *root_page = reinterpret_cast<BPlusTreePage *>(root_page_with_page_type->GetData());
  if (root_page->IsLeafPage()) {
    auto root_page_id = root_page->GetPageId();
    this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
    return root_page_id;
  }
  auto *root_page_internal = reinterpret_cast<InternalPage *>(root_page);
  auto target_page_id = root_page_internal->ValueAt(0);
  this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
  auto target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  while (!target_page->IsLeafPage()) {
    auto *target_page_internal = reinterpret_cast<InternalPage *>(target_page);
    target_page_id = target_page_internal->ValueAt(0);
    this->buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
    target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  }
  auto ret = target_page->GetPageId();
  this->buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RightMostLeaf() -> page_id_t {
  auto root_page_with_page_type = this->buffer_pool_manager_->FetchPage(this->GetRootPageId());
  auto *root_page = reinterpret_cast<BPlusTreePage *>(root_page_with_page_type->GetData());
  if (root_page->IsLeafPage()) {
    auto root_page_id = root_page->GetPageId();
    this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
    return root_page_id;
  }
  auto *root_page_internal = reinterpret_cast<InternalPage *>(root_page);
  auto target_page_id = root_page_internal->ValueAt(root_page_internal->GetSize() - 1);
  this->buffer_pool_manager_->UnpinPage(this->GetRootPageId(), false);
  auto target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
  auto *target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  while (!target_page->IsLeafPage()) {
    auto *target_page_internal = reinterpret_cast<InternalPage *>(target_page);
    target_page_id = target_page_internal->ValueAt(target_page_internal->GetSize() - 1);
    this->buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
    target_page_with_page_type = this->buffer_pool_manager_->FetchPage(target_page_id);
    target_page = reinterpret_cast<BPlusTreePage *>(target_page_with_page_type->GetData());
  }
  auto ret = target_page->GetPageId();
  this->buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);
  return ret;
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
  auto left_most_page_id = this->LeftMostLeaf();
  return INDEXITERATOR_TYPE(left_most_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t page_id;
  this->FindLeaf(key, &page_id);
  auto *page = this->buffer_pool_manager_->FetchPage(page_id);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto index = leaf_page->BisectPosition(key, this->comparator_);
  this->buffer_pool_manager_->UnpinPage(page_id, false);
  return INDEXITERATOR_TYPE(page_id, buffer_pool_manager_, index + 1);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto right_most_page_id = this->RightMostLeaf();
  auto *page = this->buffer_pool_manager_->FetchPage(right_most_page_id);
  auto *leaf_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  auto size = leaf_page->GetSize();
  this->buffer_pool_manager_->UnpinPage(right_most_page_id, false);
  return INDEXITERATOR_TYPE(right_most_page_id, buffer_pool_manager_, size);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return this->root_page_id_; }

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
  out.flush();
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
