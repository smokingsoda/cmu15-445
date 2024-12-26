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
#include <cstddef>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"

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

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page;
  frame_id_t frame_id;
  page_id_t page_id_new;
  if (this->free_list_.empty()) {
    if (!this->replacer_->Evict(&frame_id)) {
      return nullptr;
    } else {
      page = &this->pages_[frame_id];
      this->page_table_->Remove(page->GetPageId());
    }
  } else {
    frame_id = this->free_list_.front();
    page = &this->pages_[frame_id];
    this->free_list_.pop_front();
  }
  if (page->IsDirty()) {
    this->disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  // Allocate new id
  page_id_new = this->AllocatePage();
  // Set clean
  page->is_dirty_ = false;
  // Set pin set 1
  page->pin_count_ = 1;
  // Set new id
  page->page_id_ = page_id_new;
  // Insert into pgtbl
  this->page_table_->Insert(page_id_new, frame_id);
  // Reset memory
  page->ResetMemory();
  // Return via ptr
  *page_id = page_id_new;
  // Add a record
  this->replacer_->RecordAccess(frame_id);
  // Set non-evitable
  this->replacer_->SetEvictable(frame_id, false);
  // Return page ptr
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(page_id < this->next_page_id_, "No");
  frame_id_t frame_id;
  Page *page;
  if (this->page_table_->Find(page_id, frame_id)) {
    page = &this->pages_[frame_id];
    return page;
  } else if (!this->free_list_.empty()) {
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
    page = &this->pages_[frame_id];
  } else if (this->replacer_->Evict(&frame_id)) {
    page = &this->pages_[frame_id];
    this->page_table_->Remove(page->GetPageId());
  } else {
    return nullptr;
  }
  if (page->IsDirty()) {
    this->disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }

  // Set clean
  page->is_dirty_ = false;
  // Set pin set 1
  page->pin_count_ = 1;
  // Set new id
  page->page_id_ = page_id;
  // Insert into pgtbl
  this->page_table_->Insert(page_id, frame_id);
  // Read_memory
  page->ResetMemory();
  this->disk_manager_->ReadPage(page->GetPageId(), page->GetData());
  // Add a record
  this->replacer_->RecordAccess(frame_id);
  // Set non-evitable
  this->replacer_->SetEvictable(frame_id, false);
  // Return
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!this->page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = &this->pages_[frame_id];
  if (page->GetPinCount() == 0) {
    return false;
  }
  page->pin_count_ -= 1;
  if (page->pin_count_ == 0) {
    this->replacer_->SetEvictable(frame_id, true);
  }
  page->is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (this->page_table_->Find(page_id, frame_id)) {
    Page *page = &this->pages_[frame_id];
    BUSTUB_ASSERT(page->GetPageId() != INVALID_PAGE_ID, "No");
    this->disk_manager_->WritePage(page->GetPageId(), page->GetData());
    page->is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < this->GetPoolSize(); ++i) {
    BUSTUB_ASSERT(this->FlushPgImp(this->pages_[i].GetPageId()), "No");
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!this->page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *page = &this->pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }
  this->page_table_->Remove(page->GetPageId());
  this->replacer_->Remove(frame_id);
  this->free_list_.push_back(frame_id);
  page->ResetMemory();
  this->DeallocatePage(page->GetPageId());
  page->page_id_ = INVALID_PAGE_ID;
  return false;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
