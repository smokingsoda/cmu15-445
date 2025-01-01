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
#include <sstream>

#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

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
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
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

/*
 * Bisect for leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Bisect(KeyType const &key, ValueType *value,
                                        KeyComparator const &comparator) const -> bool {
  // replace with your own code
  auto index = this->BisectPosition(key, comparator);
  if (comparator(this->array_[index].first, key) == 0) {
    *value = this->array_[index].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::BisectPosition(KeyType const &key, KeyComparator const &comparator) const -> size_t {
  // replace with your own code
  auto size = GetSize();
  auto l = -1;
  auto r = size;
  while (l + 1 < r) {
    auto mid = (l + r) / 2;
    if (comparator(this->array_[mid].first, key) < 0) {
      l = mid;
    } else if (comparator(this->array_[mid].first, key) > 0) {
      r = mid;
    } else {
      return mid;
    }
  }
  return l;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKey(int index, KeyType *key_ptr) const -> void {
  BUSTUB_ASSERT((index) < this->GetSize(), "No");
  BUSTUB_ASSERT((index) >= 0, "No");
  *key_ptr = this->array_[index].first;
};
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RedistributeFrom(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *from_page,
                                                  int index) -> void {
  for (int i = 0; i + index < from_page->GetSize(); i++) {
    this->array_[i] = from_page->GetPairAt(i + index);
    this->IncrementSize();
    from_page->DecrementSize();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPairAt(int index) const -> MappingType { return this->array_[index]; }

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
