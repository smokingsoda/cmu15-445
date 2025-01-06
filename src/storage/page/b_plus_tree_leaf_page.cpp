//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageId(page_id);
  this->SetSize(0);
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return this->next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { this->next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  MappingType k_v = this->array_[index];
  return k_v.first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  MappingType k_v = this->array_[index];
  return k_v.second;
}

/*
 * Bisect for leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Bisect(KeyType const &key, ValueType *value, KeyComparator const &comparator) const
    -> bool {
  // replace with your own code
  auto index = this->BisectPosition(key, comparator);
  // std::cout << this->array_[index + 1].first << std::endl;
  if (comparator(this->array_[index + 1].first, key) == 0) {
    *value = this->array_[index].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BisectPosition(KeyType const &key, KeyComparator const &comparator) const -> int {
  // replace with your own code
  auto size = GetSize();
  auto l = -1;
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAt(int index, KeyType const &key, ValueType const &value) -> void {
  for (int i = this->GetSize() - 1; i >= index; i--) {
    this->array_[i + 1] = this->array_[i];
  }
  this->array_[index].first = key;
  this->array_[index].second = value;
  this->IncrementSize();
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IncrementSize() -> void { this->SetSize(this->GetSize() + 1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::DecrementSize() -> void { this->SetSize(this->GetSize() - 1); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(int index) -> KeyType {
  auto return_key = this->KeyAt(index);
  for (int i = index; i < this->GetSize() - 1; i++) {
    this->array_[i] = this->array_[i + 1];
  }
  this->DecrementSize();
  return return_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RedistributeFrom(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *from_page,
                                                  int index) -> void {
  auto limit = from_page->GetSize();
  for (int i = 0; i + index < limit; i++) {
    this->array_[i] = from_page->GetPairAt(i + index);
    this->IncrementSize();
    from_page->DecrementSize();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MergeWith(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *from_page,
                                           bool is_right) -> void {
  auto limit = from_page->GetSize();
  auto offset = this->GetSize();
  if (is_right) {
    for (int i = 0; i < limit; i++) {
      this->array_[i + offset] = from_page->GetPairAt(i);
      this->IncrementSize();
      from_page->DecrementSize();
    }
    BUSTUB_ASSERT(this->GetSize() == limit + offset, "No");
    BUSTUB_ASSERT(from_page->GetSize() == 0, "No");
    return;
  }
  from_page->MergeWith(this, true);
  BUSTUB_ASSERT(this->GetSize() == 0, "No");
  BUSTUB_ASSERT(from_page->GetSize() == limit + offset, "No");
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealOrMerge(bool *is_merge, bool *is_right, BufferPoolManager *bpm,
                                              KeyComparator &cmp, page_id_t *sibling_page_id) -> void {
  page_id_t parent_id = this->GetParentPageId();
  Page *parent_page = bpm->FetchPage(parent_id);
  auto *parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
  int index = parent->BisectPosition(this->KeyAt(0), cmp);
  int left_index = index - 1;
  int right_index = index + 1;
  if (left_index < 0) {
    // Must do something from right
    *is_right = true;
    Page *right_page = bpm->FetchPage(parent->ValueAt(right_index));
    auto *right = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(right_page->GetData());
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
  Page *left_page = bpm->FetchPage(parent->ValueAt(left_index));
  Page *right_page = bpm->FetchPage(parent->ValueAt(right_index));
  auto *left = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(left_page->GetData());
  auto *right = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(right_page->GetData());
  if (left->GetSize() == left->GetMinSize() && right->GetSize() == right->GetMinSize()) {
    *is_merge = true;
    *is_right = true;
    *sibling_page_id = right->GetPageId();
  } else {
    *is_merge = false;
    *is_right = (left->GetSize() == left->GetMinSize() || right->GetSize() > right->GetMinSize());
    if (*is_right) {
      *sibling_page_id = right->GetPageId();
    } else {
      *sibling_page_id = left->GetPageId();
    }
  }
  bpm->UnpinPage(parent_id, false);
  bpm->UnpinPage(left->GetPageId(), false);
  bpm->UnpinPage(right->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPairAt(int index) const -> MappingType { return this->array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPairAt(int index) -> MappingType & { return this->array_[index]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
