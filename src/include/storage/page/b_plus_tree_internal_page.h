//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const page_id_t &value);
  auto ValueAt(int index) const -> ValueType;
  auto Bisect(KeyType const &key, page_id_t *page_id, KeyComparator const &comparator) const -> bool;
  auto BisectPosition(KeyType const &key, KeyComparator const &comparator) const -> int;
  auto InsertAt(int index, KeyType const &key, page_id_t const &page_id) -> void;
  auto IncrementSize() -> void;
  auto DecrementSize() -> void;
  auto RedistributeFrom(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *from_page, int index) -> void;
  auto RemoveAt(int index) -> KeyType;
  auto UpdateChildrenPointers(BufferPoolManager *bmp) -> void;
  auto GetPairAt(int index) const -> MappingType;
  auto Rearrange() -> KeyType;
  auto StealOrMerge(bool *is_merge, bool *is_right, BufferPoolManager *bpm, KeyComparator &cmp,
                    page_id_t *sibling_page_id, int *sibling_index) -> void;

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
