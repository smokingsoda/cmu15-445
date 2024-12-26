//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <iostream>
#include <limits>
#include <memory>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < num_frames; ++i) {
    this->map_[i] = std::make_shared<Frame>(i, false, false, k);
    // this->less_than_k_frames_.push_back(i);
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (this->curr_size_ <= 0) {
    return false;
  }
  size_t earlist_access = std::numeric_limits<size_t>::max();
  size_t max_difference = 0;
  frame_id_t result_less_than_k = 0;
  frame_id_t result_equal_k = 0;
  for (auto const &it : this->map_) {
    std::shared_ptr<Frame> target_frame = it.second;
    if (!target_frame->IsInit() || !target_frame->IsEvitable()) {
      continue;
    }
    if (target_frame->IsLessThanK() && target_frame->EarlistTimestamp() < earlist_access) {
      result_less_than_k = target_frame->GetId();
      earlist_access = target_frame->EarlistTimestamp();
      continue;
    }
    if (target_frame->IsLessThanK()) {
      continue;
    }
    size_t curr_difference = target_frame->DifferenceTimestampK();
    if (curr_difference > max_difference) {
      max_difference = curr_difference;
      result_equal_k = target_frame->GetId();
    }
  }
  std::shared_ptr<Frame> result_frame;
  if (earlist_access != std::numeric_limits<size_t>::max()) {
    *frame_id = result_less_than_k;
    result_frame = this->map_[result_less_than_k];
  } else {
    *frame_id = result_equal_k;
    result_frame = this->map_[result_equal_k];
  }
  result_frame->SetInit(false);
  result_frame->SetEvitable(false);
  result_frame->ClearHistory();
  // this->less_than_k_frames_.push_back(result);
  this->curr_size_ -= 1;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  std::shared_ptr<Frame> target_frame = this->map_[frame_id];
  // bool change_flag;
  if (!target_frame->IsInit()) {
    target_frame->SetInit(true);
    target_frame->AddAccess(this->current_timestamp_);
    target_frame->SetEvitable(false);
  } else {
    target_frame->AddAccess(this->current_timestamp_);
  }
  // if (change_flag) {
  //   this->less_than_k_frames_.remove(target_frame->GetId());
  // }
  this->current_timestamp_ += 1;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  std::shared_ptr<Frame> target_frame = this->map_[frame_id];
  BUSTUB_ASSERT(target_frame->IsInit(), "No");

  if (target_frame->IsEvitable() && !set_evictable) {
    target_frame->SetEvitable(set_evictable);
    this->curr_size_ -= 1;
  } else if (!target_frame->IsEvitable() && set_evictable) {
    target_frame->SetEvitable(set_evictable);
    this->curr_size_ += 1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  std::shared_ptr<Frame> target_frame = this->map_[frame_id];
  if (!target_frame->IsInit()) {
    return;
  }
  BUSTUB_ASSERT(target_frame->IsEvitable(), "No");
  target_frame->SetInit(false);
  target_frame->ClearHistory();
  target_frame->SetEvitable(false);
  // this->less_than_k_frames_.push_back(target_frame->GetId());
  this->curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
