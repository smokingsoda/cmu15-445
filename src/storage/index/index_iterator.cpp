/**
 * index_iterator.cpp
 */
#include <cassert>
#include <stdexcept>
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/index/index_iterator.h"

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_id, BufferPoolManager *bpm, int index) {
  this->leaf_id_ = leaf_id;
  this->bpm_ = bpm;
  this->index_ = index;
};

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  auto *page = this->bpm_->FetchPage(this->leaf_id_);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto flag = leaf_page->GetNextPageId() == INVALID_PAGE_ID && this->index_ == leaf_page->GetSize();
  this->bpm_->UnpinPage(leaf_id_, false);
  return flag;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  if (this->IsEnd()) {
    throw std::runtime_error("Out of bounds");
  }
  auto *page = this->bpm_->FetchPage(this->leaf_id_);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  auto &ret = leaf_page->GetPairAt(index_);
  this->bpm_->UnpinPage(leaf_id_, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (this->IsEnd()) {
    throw std::runtime_error("Out of bounds");
  }
  auto *page = this->bpm_->FetchPage(this->leaf_id_);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  if (this->index_ == leaf_page->GetSize()) {
    auto old_leaf_page_id = leaf_id_;
    this->leaf_id_ = leaf_page->GetNextPageId();
    this->index_ = 0;
    this->bpm_->UnpinPage(old_leaf_page_id, false);
    return *this;
  }
  this->index_ += 1;
  this->bpm_->UnpinPage(leaf_id_, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
