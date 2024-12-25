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
#include <utility>
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  for (size_t i = 0; i < num_frames; i++) {
    this->is_evitable_.push_back(false);
    this->is_less_than_k_.push_back(true);
    this->non_evitable_.push_back(i);
    this->history_.push_back(std::deque<size_t>());
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (this->evitable_equal_k_.size() > 0) {
    size_t earlist_timestamp = std::numeric_limits<size_t>::max();
    frame_id_t target = this->evitable_less_than_k_.front();
    for (auto it = this->evitable_less_than_k_.begin(); it != this->evitable_less_than_k_.end(); ++it) {
      frame_id_t curr_frame_id = *it;
      if (this->history_[curr_frame_id].back() <= earlist_timestamp) {
        target = curr_frame_id;
        earlist_timestamp = this->history_[curr_frame_id].back();
      }
    }
    *frame_id = target;
    return true;
  } else if (this->evitable_equal_k_.size() > 0) {
    size_t max_difference = 0;
    frame_id_t target = this->evitable_equal_k_.front();
    for (auto it = this->evitable_equal_k_.begin(); it != this->evitable_equal_k_.end(); ++it) {
      frame_id_t curr_frame_id = *it;
      size_t curr_difference = this->history_[curr_frame_id].front() - this->history_[curr_frame_id].back();
      if (curr_difference >= max_difference) {
        target = curr_frame_id;
        max_difference = curr_difference;
      }
    }
    *frame_id = target;
    return true;
  } else {
    return false;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    if (this->is_less_than_k_[frame_id]) {
        this->history_[frame_id].push_front(this->current_timestamp_);
        if (this->history_[frame_id].size() >= this->k_) {
            this->is_less_than_k_[frame_id] = false;
            this->evitable_less_than_k_.remove(frame_id);
            this->evitable_equal_k_.push_back(frame_id);
        }
    } else if (!this->is_less_than_k_[frame_id]) {
        this->history_[frame_id].push_front(this->current_timestamp_);
        this->history_[frame_id].pop_back();
    } else {
        BUSTUB_ASSERT(true, "No");
    }
    this->current_timestamp_ += 1;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id >= static_cast<frame_id_t>(this->replacer_size_), "No");
    if (this->is_evitable_[frame_id] && !set_evictable) {
        if (this->is_less_than_k_[frame_id]) {
            this->evitable_less_than_k_.remove(frame_id);
            this->non_evitable_.push_back(frame_id);
        } else {
            this->evitable_equal_k_.remove(frame_id);
            this->non_evitable_.push_back(frame_id);
        }
    } else if (!this->is_evitable_[frame_id] && set_evictable) {
        if (this->history_[frame_id].size() < this->k_) {
            this->non_evitable_.remove(frame_id);
            this->evitable_less_than_k_.push_back(frame_id);
        } else {
            this->non_evitable_.remove(frame_id);
            this->evitable_equal_k_.push_back(frame_id);
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    if (frame_id > static_cast<frame_id_t>(this->replacer_size_)) {
        return;
    }
    BUSTUB_ASSERT(!this->is_evitable_[frame_id], "No");
    this->is_evitable_[frame_id] = false;
    if (this->is_less_than_k_[frame_id]) {
        this->evitable_less_than_k_.remove(frame_id);
        this->non_evitable_.push_back(frame_id);
    } else {
        this->evitable_equal_k_.remove(frame_id);
        this->non_evitable_.push_back(frame_id);
    }
}

auto LRUKReplacer::Size() -> size_t { return this->replacer_size_ - this->non_evitable_.size(); }

}  // namespace bustub
