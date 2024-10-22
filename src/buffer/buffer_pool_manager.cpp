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
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
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
  std::scoped_lock<std::mutex> lock(latch_);
  // 为新的page分配page_id
  auto new_page_id = AllocatePage();
  frame_id_t frame_id = -1;
  // 如果free_list为空，则从replacer中evict一个frame
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 如果page是脏页，将page写回disk
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
      future.get();
    }
    // 更新page_table_
    page_table_.erase(pages_[frame_id].GetPageId());
  } else {
    // 从free_list中取出一个frame_id
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  // 更新page_table_
  page_table_[new_page_id] = frame_id;

  // 初始化page
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();

  // 更新replacer_
  replacer_->RecordAccess(frame_id);
  // 设置frame不可驱逐
  replacer_->SetEvictable(frame_id, false);

  *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 加锁
  std::lock_guard lock(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  // 如果page在buffer pool中，直接返回Page
  if (page_table_.find(page_id) != page_table_.end()) {
    // 获取page_id对应的frame_id
    frame_id_t frame_id = page_table_[page_id];
    // 更新访问历史
    replacer_->RecordAccess(frame_id, access_type);
    // 设置frame不可驱逐
    replacer_->SetEvictable(frame_id, false);
    // 更新pin_count
    ++pages_[frame_id].pin_count_;
    return &pages_[frame_id];
  }
  frame_id_t frame_id = -1;
  // 如果free_list为空，则从replacer中evict一个frame
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 如果page是脏页，将page写回disk
    if (pages_[frame_id].IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
      future.get();
    }
    // 更新page_table_
    page_table_.erase(pages_[frame_id].GetPageId());
  } else {
    // 从free_list中取出一个frame_id
    frame_id = free_list_.back();
    free_list_.pop_back();
  }

  // 更新page_table_
  page_table_[page_id] = frame_id;

  // 初始化page
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // 从disk中读取page到buffer pool中
  auto read_promise = disk_scheduler_->CreatePromise();
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), page_id, std::move(read_promise)});
  read_future.get();

  // 更新replacer_
  replacer_->RecordAccess(frame_id, access_type);
  // 设置frame不可驱逐
  replacer_->SetEvictable(frame_id, false);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // 加锁
  std::scoped_lock lock(latch_);
  // 如果page_id无效，或者page不在buffer pool中，返回false
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  // 获取frame_id
  frame_id_t frame_id = page_table_[page_id];
  // 更新page的dirty标志
  pages_[frame_id].is_dirty_ = pages_[frame_id].is_dirty_ || is_dirty;

  // 如果pin_count == 0，返回false
  if (pages_[frame_id].GetPinCount() == 0) {
    return false;
  }

  // 如果pin_count > 0，更新pin_count
  --pages_[frame_id].pin_count_;
  // 如果pin_count == 0, 将replace_中对应的frame设置为evictable
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // 加锁
  std::scoped_lock lock(latch_);
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // 获取frame_id
  frame_id_t frame_id = page_table_[page_id];
  // 将page写回disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), page_id, std::move(promise)});
  future.get();
  // 重置page的dirty标志
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  // 加锁
  std::scoped_lock lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    // 获取page_id
    page_id_t page_id = pages_[i].GetPageId();
    // 如果page有效，且dirty，将page写回disk
    if (page_id != INVALID_PAGE_ID) {
      // 将page写回disk
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[i].GetData(), page_id, std::move(promise)});
      future.get();
      // 重置page的dirty标志
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 加锁
  std::scoped_lock lock(latch_);
  // 如果page_id无效，或者page不在buffer pool中，返回true
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  // 获取frame_id
  frame_id_t frame_id = page_table_[page_id];
  // 如果page是pinned，返回false
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // 如果page是脏页，将page写回disk
  if (pages_[frame_id].IsDirty()) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), page_id, std::move(promise)});
    future.get();
  }

  // 重置page元数据
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  // 删除page_table_中的page_id映射
  page_table_.erase(page_id);
  // 将frame_id加入free_list
  free_list_.push_back(frame_id);
  // remove the frame from the replacer
  replacer_->Remove(frame_id);
  // 释放page_id
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
