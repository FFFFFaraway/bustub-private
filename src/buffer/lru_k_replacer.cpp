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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  data_ = std::vector<std::unique_ptr<LRUEntry>>(replacer_size_);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  frame_id_t select = -1;
  for (size_t i = 0; i < data_.size(); i++) {
    if (data_[i] && data_[i]->evictable_) {
      if (select == -1) {
        select = i;
        continue;
      }
      // When do we assign the i as the select evict frame id?
      // 1. both accessed k times, but frame[i] has older time than current selected
      // 2. frame[i] no k times, but selected has k times
      // 3. both no k times, and frame[i] has older time, LRU, only consider front
      if ((data_[select]->t_.size() == k_ && data_[i]->t_.size() == k_ &&
           data_[i]->t_.back() < data_[select]->t_.back()) ||
          (data_[select]->t_.size() == k_ && data_[i]->t_.size() < k_) ||
          (data_[select]->t_.size() < k_ && data_[i]->t_.size() < k_ &&
           data_[i]->t_.front() < data_[select]->t_.front())) {
        select = i;
      }
    }
  }
  RemoveInternal(select);
  *frame_id = select;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "frame id is invalid");
  if (data_[frame_id] == nullptr) {
    data_[frame_id] = std::make_unique<LRUEntry>();
    data_[frame_id]->t_ = std::list<time_t>();
    data_[frame_id]->evictable_ = false;
  }
  data_[frame_id]->t_.push_front(current_timestamp_++);
  if (data_[frame_id]->t_.size() > k_) {
    data_[frame_id]->t_.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "frame id is invalid");
  if (!data_[frame_id]->evictable_ && set_evictable) {
    curr_size_++;
    data_[frame_id]->evictable_ = set_evictable;
  }
  if (data_[frame_id]->evictable_ && !set_evictable) {
    curr_size_--;
    data_[frame_id]->evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  RemoveInternal(frame_id);
}

void LRUKReplacer::RemoveInternal(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) < replacer_size_, "frame id is invalid");
  if (data_[frame_id] == nullptr) {
    return;
  }
  BUSTUB_ASSERT(data_[frame_id]->evictable_, "remove is called on a non-evictable frame");
  data_[frame_id].reset();
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
