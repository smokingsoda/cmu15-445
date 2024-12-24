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
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <deque>
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < num_frames; i++) {
    this->frame_id_deque_vector_.push_back(std::deque<size_t>());
    this->evitable_vector_.push_back(false);
    this->curr_size_ = 0;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (this->curr_size_ <= 0) {
    return false;
  }
  size_t max_difference = 0;
  size_t least_access = std::numeric_limits<size_t>::max();
  frame_id_t least_time_stamp_frame_id;
  int index = 0;
  for (const auto &deque : this->frame_id_deque_vector_) {
    BUSTUB_ASSERT(deque.size() > this->k_, "No");
    if (least_access < deque.back() && this->evitable_vector_[index]) {
      least_access = deque.back();
      least_time_stamp_frame_id = index;
    }
    if (deque.size() < this->k_ && this->evitable_vector_[index]) {
      max_difference = std::numeric_limits<size_t>::max();
      *frame_id = index;
      break;
    }
    if (deque[0] - deque[this->k_ - 1] < max_difference && this->evitable_vector_[index]) {
      max_difference = std::max(max_difference, deque[0] - deque[this->k_ - 1]);
      *frame_id = index;
    }
    index += 1;
  }
  if (max_difference != 0) {
    this->curr_size_ -= 1;
    this->frame_id_deque_vector_[*frame_id].clear();
    return true;
  } else if (least_access != std::numeric_limits<size_t>::max()) {
    this->curr_size_ -= 1;
    this->frame_id_deque_vector_[*frame_id].clear();
    *frame_id = least_time_stamp_frame_id;
    return true;
  } else {
    return false;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= static_cast<int>(this->frame_id_deque_vector_.size()), "No");
  this->current_timestamp_ += 1;
  auto target_deque = this->frame_id_deque_vector_[frame_id];
  BUSTUB_ASSERT(target_deque.size() > this->k_, "No");
  if (target_deque.size() == this->k_) {
    target_deque.pop_back();
    target_deque.push_front(this->current_timestamp_);
  } else if (target_deque.size() < this->k_) {
    target_deque.push_front(this->current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id >= static_cast<int>(this->frame_id_deque_vector_.size())) {
    return;
  }
  if (this->evitable_vector_[frame_id] && !set_evictable) {
    this->evitable_vector_[frame_id] = set_evictable;
    this->curr_size_ -= 1;
    return;
  }
  if (!this->evitable_vector_[frame_id] && set_evictable) {
    this->evitable_vector_[frame_id] = set_evictable;
    this->curr_size_ += 1;
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (frame_id > static_cast<int>(this->replacer_size_)) {
        return;
    }
    BUSTUB_ASSERT(!this->evitable_vector_[frame_id], "No")
    this->frame_id_deque_vector_[frame_id]
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
