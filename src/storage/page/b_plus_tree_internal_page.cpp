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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::BisectPosition(KeyType const &key,
                                                    KeyComparator const &comparator) const -> size_t {
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
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(page_with_page_type);
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
  for (int i = 1; i + index < limit; i++) {
    this->array_[i] = from_page->GetPairAt(i + index);
    this->IncrementSize();
    from_page->DecrementSize();
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(int index) -> KeyType {
  auto return_key = this->KeyAt(index);
  for (int i = index; i < this->GetSize(); i--) {
    this->array_[i] = this->array_[i + 1];
  }
  this->DecrementSize();
  return return_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPairAt(int index) const -> MappingType { return this->array_[index]; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
