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

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

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
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (free_list_.empty()) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    // 删除旧的page
    pages_[frame_id].RLatch();
    page_id_t old_page_id = pages_[frame_id].GetPageId();
    pages_[frame_id].RUnlatch();
    DeletePage(old_page_id);
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  // 为新的page分配page_id
  page_id_t new_page_id = AllocatePage();
  // 更新page_table_
  page_table_[new_page_id] = frame_id;
  // 将page_id写入page中
  pages_[frame_id].WLatch();
  pages_[frame_id].page_id_ = new_page_id;
  pages_[frame_id].WUnlatch();

  *page_id = new_page_id;
  return &pages_[frame_id];

  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // 如果page在buffer pool中，直接返回Page
  if (page_table_.find(page_id) != page_table_.end()) {
    // 获取page_id对应的frame_id
    frame_id_t frame_id = page_table_[page_id];
    // 更新访问历史
    replacer_->RecordAccess(frame_id, access_type);
    return &pages_[frame_id];
  }

  // 如果page不在buffer pool中，且free_list为空，返回nullptr
  if (page_table_.find(page_id) == page_table_.end() && free_list_.empty()) {
    return nullptr;
  }

  // 如果page不在buffer pool中，且free_list不为空，从free_list中取出一个frame_id
  frame_id_t new_frame_id = free_list_.front();
  free_list_.pop_front();
  if (pages_[page_id].IsDirty()) {
    // 读取old_page的数据和page_id
    char old_data[BUSTUB_PAGE_SIZE] = {0};
    pages_[new_frame_id].RLatch();
    memcpy(old_data, pages_[new_frame_id].GetData(), BUSTUB_PAGE_SIZE);
    auto old_page_id = pages_[new_frame_id].GetPageId();
    pages_[new_frame_id].RUnlatch();

    // 如果page_id有效，则将data写回disk
    if (old_page_id != INVALID_PAGE_ID) {
      auto write_promise = disk_scheduler_->CreatePromise();
      auto write_future = write_promise.get_future();
      disk_scheduler_->Schedule({true, old_data, old_page_id, std::move(write_promise)});
      write_future.get();
    }
  }

  // 从disk中读取page到buffer pool中
  auto read_promise = disk_scheduler_->CreatePromise();
  auto read_future = read_promise.get_future();
  char data[BUSTUB_PAGE_SIZE] = {0};
  disk_scheduler_->Schedule({false, data, page_id, std::move(read_promise)});
  read_future.get();

  // 更新pages_
  pages_[new_frame_id].WLatch();
  memcpy(pages_[new_frame_id].GetData(), data, BUSTUB_PAGE_SIZE);
  pages_[new_frame_id].page_id_ = page_id;
  pages_[new_frame_id].is_dirty_ = false;
  pages_[new_frame_id].WUnlatch();

  // 更新page_table_
  page_table_[page_id] = new_frame_id;
  // 更新replacer_
  replacer_->RecordAccess(new_frame_id, access_type);
  return &pages_[new_frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // 如果page不在buffer pool中，返回false
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  bool flag = false;
  // 获取pin_count
  pages_[frame_id].RLatch();
  flag = pages_[frame_id].GetPinCount() <= 0;
  pages_[frame_id].RUnlatch();

  // 如果pin_count <= 0，返回false
  if (flag) {
    return false;
  }

  // 如果pin_count > 0，更新pin_count
  pages_[frame_id].WLatch();
  pages_[frame_id].pin_count_--;
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }
  pages_[frame_id].WUnlatch();

  // 如果pin_count == 0, 将replace_中对应的frame设置为evictable
  pages_[frame_id].RLatch();
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].RUnlatch();

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // 如果page_id无效，返回false
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // 如果page不在buffer pool中，返回false
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  // 获取frame_id
  frame_id_t frame_id = page_table_[page_id];
  // 获取page的数据
  pages_[frame_id].WLatch();
  char data[BUSTUB_PAGE_SIZE] = {0};
  memcpy(data, pages_[frame_id].GetData(), BUSTUB_PAGE_SIZE);
  // unset dirty flag
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();

  // 将page写回disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, data, page_id, std::move(promise)});
  future.get();

  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (size_t i = 0; i < pool_size_; ++i) {
    // 获取page的is_dirty和page_id
    pages_[i].RLatch();
    auto is_dirty = pages_[i].IsDirty();
    auto page_id = pages_[i].GetPageId();
    pages_[i].RUnlatch();

    // 如果page有效，且dirty，将page写回disk
    if (page_id != INVALID_PAGE_ID && is_dirty) {
      FlushPage(page_id);
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 如果page_id不在buffer pool中，返回true
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  bool flag = false;
  bool is_dirty = false;
  // 获取pin_count和is_dirty
  pages_[frame_id].RLatch();
  flag = pages_[frame_id].GetPinCount() > 0;
  is_dirty = pages_[frame_id].IsDirty();
  pages_[frame_id].RUnlatch();

  // 如果pin_count > 0，返回false
  if (flag) {
    return false;
  }

  // reset the page's memory and metadata
  if (is_dirty) {
    FlushPage(page_id);
  }
  pages_[frame_id].WLatch();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();
  DeallocatePage(page_id);

  // remove the frame from the replacer
  replacer_->Remove(frame_id);
  // 将frame_id加入free_list
  free_list_.push_back(frame_id);

  return false;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
