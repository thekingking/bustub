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
  node_store_ = std::vector<LRUKNode>(num_frames);
  for (size_t i = 0; i < num_frames; ++i) {
    node_store_[i].frame_id_ = static_cast<frame_id_t>(i);
  }
}

LRUKReplacer::~LRUKReplacer() = default;

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  // 定义一个 lambda 来处理Evict逻辑
  // 策略: 从队尾开始找第一个 is_evictable 的节点
  auto evict_from_list = [&](std::list<frame_id_t> &list) -> bool {
    // 使用反向迭代器，从尾部开始扫描
    for (auto it = list.rbegin(); it != list.rend(); ++it) {
      if (node_store_[*it].is_evictable_) {
        *frame_id = *it;

        // 转换为正向迭代器并删除
        node_store_[*frame_id].history_.clear();
        node_store_[*frame_id].is_evictable_ = false;
        --curr_size_;

        list.erase(node_store_[*frame_id].pos_);
        return true;
      }
    }
    return false;
  };

  // 1. 优先驱逐 History 队列 (FIFO 淘汰)
  if (evict_from_list(history_list_)) {
    return true;
  }

  // 2. 其次驱逐 Cache 队列 (LRU 淘汰)
  if (evict_from_list(cache_list_)) {
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // 判断frame_id是否合法
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
  }

  auto &node = node_store_[frame_id];
  node.history_.push_back(current_timestamp_);
  current_timestamp_++;

  // Case 1: 新页面（第一次访问）
  if (node.history_.size() == 1) {
    history_list_.push_front(frame_id);
    node.pos_ = history_list_.begin();
  }
  // Case 2: 达到 k 次访问 -> 晋升 Cache 队列
  else if (node.history_.size() == k_) {
    history_list_.erase(node.pos_);
    cache_list_.push_front(frame_id);
    node.pos_ = cache_list_.begin();
  }
  // Case 3: 已在 Cache 队列 -> 2Q策略更新
  else if (node.history_.size() > k_) {
    node.history_.pop_front();
    cache_list_.erase(node.pos_);
    cache_list_.push_front(frame_id);
    node.pos_ = cache_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // 加锁
  std::lock_guard<std::mutex> lock(latch_);
  // 判断frame_id是否合法
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
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
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame id out of range");
  }

  auto &node = node_store_[frame_id];
  // 只有没有任何历史记录的节点才不需要处理 (或者已经被 Evict/Remove)
  if (node.history_.empty()) {
    return;
  }

  if (!node.is_evictable_) {
    throw Exception(ExceptionType::INVALID, "Can't remove non-evictable frame");
  }

  // 根据访问次数判断在哪个队列
  if (node.history_.size() < k_) {
    history_list_.erase(node.pos_);
  } else {
    cache_list_.erase(node.pos_);
  }

  // 重置状态
  curr_size_--;
  node.history_.clear();
  node.is_evictable_ = false;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
