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
#include <memory>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  node_store_ = new LRUKNode[num_frames];
}

LRUKReplacer::~LRUKReplacer() { delete[] node_store_; }

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  // 优先从history队列evict frame
  for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
    if (node_store_[*it].is_evictable_) {
      *frame_id = *it;
      history_list_.erase(it);
      --curr_size_;
      node_store_[*frame_id].history_.clear();
      node_store_[*frame_id].is_evictable_ = false;
      return true;
    }
  }

  // lru队列evict frame
  auto min_time = current_timestamp_;
  for (auto it : lru_list_) {
    if (node_store_[it].is_evictable_ && node_store_[it].history_.front() < min_time) {
      min_time = node_store_[it].history_.front();
      *frame_id = it;
    }
  }
  --curr_size_;
  lru_list_.remove(*frame_id);
  node_store_[*frame_id].history_.clear();
  node_store_[*frame_id].is_evictable_ = false;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // 判断frame_id是否合法
  if (frame_id < 0 || frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }

  // 添加历史记录
  node_store_[frame_id].history_.push_back(current_timestamp_);
  // 更新时间戳
  ++current_timestamp_;

  // 新加入记录
  if (node_store_[frame_id].history_.size() == 1) {
    if (access_type == AccessType::Scan) {
      history_list_.push_front(frame_id);
    } else {
      history_list_.push_back(frame_id);
    }
  }

  // 记录达到k次，加入lru队列
  if (node_store_[frame_id].history_.size() == k_) {
    for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
      if (*it == frame_id) {
        history_list_.erase(it);
        break;
      }
    }
    lru_list_.push_back(frame_id);
  }

  // 本来就在lru队列中，更新时间戳
  if (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // 判断frame_id是否合法
  if (frame_id < 0 || frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }

  if (!node_store_[frame_id].is_evictable_ && set_evictable) {
    // 原先不可驱逐，现在可驱逐
    ++curr_size_;
  } else if (node_store_[frame_id].is_evictable_ && !set_evictable) {
    // 原先可驱逐，现在不可驱逐
    --curr_size_;
  }
  node_store_[frame_id].is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // 判断frame_id是否合法
  if (frame_id < 0 || frame_id > static_cast<frame_id_t>(replacer_size_)) {
    return;
  }
  // 判断frame_id是否可驱逐
  if (!node_store_[frame_id].is_evictable_) {
    return;
  }
  // 删除节点
  if (node_store_[frame_id].history_.size() == k_) {
    for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
      if (*it == frame_id) {
        lru_list_.erase(it);
        break;
      }
    }
  } else {
    for (auto it = history_list_.begin(); it != history_list_.end(); it++) {
      if (*it == frame_id) {
        history_list_.erase(it);
        break;
      }
    }
  }
  node_store_[frame_id].history_.clear();
  node_store_[frame_id].is_evictable_ = false;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
