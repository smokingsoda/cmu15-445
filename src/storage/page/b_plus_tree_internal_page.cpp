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

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetSize(1);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  MappingType k_v = this->array_[index];
  return k_v.first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { this->array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const page_id_t &value) {
  this->array_[index].second = value;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  MappingType k_v = this->array_[index];
  return k_v.second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Bisect(KeyType const &key, page_id_t *page_id,
                                            KeyComparator const &comparator) const -> bool {
  auto index = this->BisectPosition(key, comparator);
  if (comparator(this->array_[index + 1].first, key) == 0) {
    *page_id = this->array_[index].second;
    return true;
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, KeyType const &key, page_id_t const &page_id) -> void {
  for (int i = this->GetSize() - 1; i >= index; i--) {
    this->array_[i + 1] = this->array_[i];
  }
  this->array_[index].first = key;
  this->array_[index].second = page_id;
  this->IncrementSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BisectPosition(KeyType const &key, KeyComparator const &comparator) const -> int {
  auto size = this->GetSize();
  auto l = 0;
  auto r = size;
  while (l + 1 < r) {
    auto mid = (l + r) / 2;
    if (comparator(this->array_[mid].first, key) < 0) {
      l = mid;
    } else {
      r = mid;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpdateChildrenPointers(BufferPoolManager *bpm) -> void {
  for (int i = 0; i < this->GetSize(); i++) {
    auto id = this->array_[i].second;
    Page *page_with_page_type = bpm->FetchPage(id);
    auto *page = reinterpret_cast<BPlusTreePage *>(page_with_page_type);
    bool is_dirty = false;
    if (page->GetParentPageId() != this->GetPageId()) {
      page->SetParentPageId(this->GetPageId());
      is_dirty = true;
    }
    bpm->UnpinPage(id, is_dirty);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IncrementSize() -> void { this->SetSize(this->GetSize() + 1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::DecrementSize() -> void { this->SetSize(this->GetSize() - 1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RedistributeFrom(
    BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *from_page, int index) -> void {
  auto limit = from_page->GetSize();
  for (int i = 1; i + index - 1 < limit; i++) {
    // It should be i + index - 1 here. Important!
    this->array_[i] = from_page->GetPairAt(i + index - 1);
    this->IncrementSize();
    from_page->DecrementSize();
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) -> KeyType {
  auto return_key = this->KeyAt(index);
  for (int i = index; i < this->GetSize() - 1; i++) {
    this->array_[i] = this->array_[i + 1];
  }
  this->DecrementSize();
  return return_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Rearrange() -> KeyType {
  auto return_key = this->array_[1].first;
  this->array_[0].second = this->array_[1].second;
  this->RemoveAt(1);
  return return_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPairAt(int index) const -> MappingType { return this->array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealOrMerge(bool *is_merge, bool *is_right, BufferPoolManager *bpm,
                                                  KeyComparator &cmp, page_id_t *sibling_page_id, int *sibling_index)
    -> void {
  page_id_t parent_id = this->GetParentPageId();
  Page *parent_page = bpm->FetchPage(parent_id);
  auto *parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  // int index = parent->BisectPosition(this->KeyAt(1), cmp);
  // if (index + 1 < parent->GetSize() && cmp(parent->KeyAt(index + 1), this->KeyAt(1)) == 0) {
  //   index = index + 1;
  // }
  int index = -1;
  for (int i = 0; i < parent->GetSize(); i++) {
    if (parent->ValueAt(i) == this->GetPageId()) {
      index = i;
      break;
    }
  }
  int left_index = index - 1;
  int right_index = index + 1;
  if (left_index < 0) {
    // Must do something from right
    *is_right = true;
    *sibling_index = right_index;
    Page *right_page = bpm->FetchPage(parent->ValueAt(right_index));
    auto *right = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(right_page->GetData());
    *sibling_page_id = right->GetPageId();
    if (right->GetSize() == right->GetMinSize()) {
      *is_merge = true;
    } else {
      *is_merge = false;
    }
    bpm->UnpinPage(parent_id, false);
    bpm->UnpinPage(right->GetPageId(), false);
    return;
  }
  if (right_index >= parent->GetSize()) {
    // Must do something from left
    *is_right = false;
    *sibling_index = left_index;
    Page *left_page = bpm->FetchPage(parent->ValueAt(left_index));
    auto *left = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(left_page->GetData());
    *sibling_page_id = left->GetPageId();
    if (left->GetSize() == left->GetMinSize()) {
      *is_merge = true;
    } else {
      *is_merge = false;
    }
    bpm->UnpinPage(parent_id, false);
    bpm->UnpinPage(left->GetPageId(), false);
    return;
  }
  Page *left_page = bpm->FetchPage(parent->ValueAt(left_index));
  Page *right_page = bpm->FetchPage(parent->ValueAt(right_index));
  auto *left = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(left_page->GetData());
  auto *right = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(right_page->GetData());
  if (left->GetSize() == left->GetMinSize() && right->GetSize() == right->GetMinSize()) {
    *is_merge = true;
    *is_right = true;
    *sibling_index = right_index;
    *sibling_page_id = right->GetPageId();
  } else {
    *is_merge = false;
    *is_right = (left->GetSize() == left->GetMinSize() || right->GetSize() > right->GetMinSize());
    if (*is_right) {
      *sibling_page_id = right->GetPageId();
      *sibling_index = right_index;
    } else {
      *sibling_page_id = left->GetPageId();
      *sibling_index = left_index;
    }
  }
  bpm->UnpinPage(parent_id, false);
  bpm->UnpinPage(left->GetPageId(), false);
  bpm->UnpinPage(right->GetPageId(), false);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
