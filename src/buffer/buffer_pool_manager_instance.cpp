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

#include "common/exception.h"
#include "common/macros.h"

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
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f = -1;
  auto new_page = AllocatePage();

  if (!free_list_.empty()) {
    f = free_list_.front();
    free_list_.pop_front();
  } else {
    auto can_evict = replacer_->Evict(&f);
    if (!can_evict) {
      return nullptr;
    }
    page_table_->Remove(pages_[f].GetPageId());
    if (pages_[f].IsDirty()) {
      disk_manager_->WritePage(pages_[f].GetPageId(), pages_[f].GetData());
    }
  }
  pages_[f].ResetMemory();
  page_table_->Insert(new_page, f);
  disk_manager_->ReadPage(new_page, pages_[f].data_);
  pages_[f].page_id_ = new_page;
  pages_[f].is_dirty_ = false;
  pages_[f].pin_count_ = 1;

  replacer_->RecordAccess(f);
  replacer_->SetEvictable(f, false);
  *page_id = new_page;
  return &pages_[f];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f;
  auto find = page_table_->Find(page_id, f);
  if (!find) {
    if (!free_list_.empty()) {
      f = free_list_.front();
      free_list_.pop_front();
    } else {
      auto can_evict = replacer_->Evict(&f);
      if (!can_evict) {
        return nullptr;
      }
      page_table_->Remove(pages_[f].GetPageId());
      if (pages_[f].IsDirty()) {
        disk_manager_->WritePage(pages_[f].GetPageId(), pages_[f].GetData());
      }
    }
    pages_[f].ResetMemory();
    page_table_->Insert(page_id, f);
    disk_manager_->ReadPage(page_id, pages_[f].data_);
    pages_[f].page_id_ = page_id;
    pages_[f].is_dirty_ = false;
    pages_[f].pin_count_ = 0;
  }
  pages_[f].pin_count_++;
  replacer_->RecordAccess(f);
  replacer_->SetEvictable(f, false);
  return &pages_[f];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f;
  auto find = page_table_->Find(page_id, f);
  if (!find) {
    return false;
  }
  if (pages_[f].GetPinCount() == 0) {
    return false;
  }
  pages_[f].pin_count_--;
  if (pages_[f].GetPinCount() == 0) {
    replacer_->SetEvictable(f, true);
  }
  if (is_dirty) {
    pages_[f].is_dirty_ = is_dirty;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f;
  auto find = page_table_->Find(page_id, f);
  if (!find) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[f].GetData());
  pages_[f].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t f = 0; f < pool_size_; f++) {
    disk_manager_->WritePage(pages_[f].GetPageId(), pages_[f].GetData());
    pages_[f].is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t f;
  auto find = page_table_->Find(page_id, f);
  if (!find) {
    return true;
  }
  if (pages_[f].GetPinCount() > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(f);
  pages_[f].ResetMemory();
  pages_[f].page_id_ = INVALID_PAGE_ID;
  pages_[f].is_dirty_ = false;
  pages_[f].pin_count_ = 0;
  free_list_.push_back(f);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
