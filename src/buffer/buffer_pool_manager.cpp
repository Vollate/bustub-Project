//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cmath>
#include <iostream>
#include <mutex>  // NOLINT

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t evicted_frame;
  Page *page = nullptr;
  std::lock_guard guard{latch_};
  if (free_list_.empty()) {  // use lru-k evict
    if (replacer_->Evict(&evicted_frame)) {
      page = pages_ + evicted_frame;
      if (page->IsDirty()) {
        WriteBackPage(page);
        page->is_dirty_ = false;
      }
    } else {
      page_id = nullptr;
      return nullptr;
    }
  } else {  // use free list
    evicted_frame = free_list_.front();
    free_list_.pop_front();
    page = pages_ + evicted_frame;
    page->is_dirty_ = false;
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = evicted_frame;
  page->page_id_ = *page_id;
  FetchFromDisk(page);
  PinFrame(evicted_frame);
  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard guard{latch_};
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page_id == page->GetPageId()) {  // in buffer
    return pages_ + frame_id;
  }
  if (!free_list_.empty()) {  // use free list
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
    page_table_[page_id] = frame_id;
    if (page->IsDirty()) {
      WriteBackPage(page_id);
      page->is_dirty_ = false;
    }
    page->page_id_ = page_id;
  } else if (replacer_->Evict(&frame_id)) {  // evict from replacer
    page = pages_ + frame_id;
    page_table_[page_id] = frame_id;
    if (page->IsDirty()) {
      WriteBackPage(frame_id);
      page->is_dirty_ = false;
    }
    page->page_id_ = page_id;
  } else {
    return nullptr;
  }
  FetchFromDisk(page);
  PinFrame(frame_id);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard guard{latch_};
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page->GetPageId() != page_id || page->pin_count_ == 0) {
    return false;
  }
  page->is_dirty_ = is_dirty;
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard guard{latch_};
  frame_id_t frame_id = page_table_[page_id];
  Page *page = pages_ + frame_id;
  if (page_id != page->GetPageId() || page_id == INVALID_PAGE_ID) {
    return false;
  }
  WriteBackPage(page_id);
  (pages_ + page_table_[page_id])->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard guard{latch_};
  Page *page = pages_;
  for (size_t i = 0; i < pool_size_; ++i, ++page) {
    if (page->page_id_ != INVALID_PAGE_ID) {
      WriteBackFrame(i);
      page->is_dirty_ = false;
      page->page_id_ = INVALID_PAGE_ID;
      free_list_.push_front(i);
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  frame_id_t frame = page_table_[page_id];
  Page *page = pages_ + frame;
  // not exist
  if (page->GetPageId() != page_id || page_id != (pages_ + frame)->GetPageId()) {
    return true;
  }
  // can't be deleted
  if (page->pin_count_ != 0) {
    return false;
  }
  // remove
  page->page_id_ = INVALID_PAGE_ID;
  replacer_->Remove(frame);
  free_list_.push_front(frame);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

void BufferPoolManager::WriteBackFrame(const frame_id_t frame_id) {
  disk_manager_->WritePage((pages_ + frame_id)->GetPageId(), (pages_ + frame_id)->data_);
}

void BufferPoolManager::WriteBackPage(const page_id_t page_id) {
  disk_manager_->WritePage(page_id, (pages_ + page_table_[page_id])->data_);
}

void BufferPoolManager::WriteBackPage(Page *page) { disk_manager_->WritePage(page->page_id_, page->data_); }

void BufferPoolManager::FetchFromDisk(Page *page) { disk_manager_->ReadPage(page->page_id_, page->data_); }

void BufferPoolManager::PinFrame(const frame_id_t frame_id) {
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  ++(pages_ + frame_id)->pin_count_;
}

}  // namespace bustub
