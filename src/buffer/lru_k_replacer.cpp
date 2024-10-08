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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    // 加锁
    std::lock_guard<std::mutex> lock(latch_);

    // 优先从history队列evict frame
    for (auto it = history_list.begin(); it != history_list.end(); it++) {
        if (node_store_[*it]->is_evictable_) {
            *frame_id = *it;
            history_list.erase(it);
            --curr_size_;
            node_store_[*frame_id]->history_.clear();
            return true;
        }
    }

    // lru队列evict frame
    if (curr_size_ > 0) {
        auto min_time = current_timestamp_;
        for (auto it = lru_list.begin(); it != lru_list.end(); it++) {
            if (node_store_[*it]->is_evictable_ && node_store_[*it]->history_.front() < min_time) {
                min_time = node_store_[*it]->history_.front();
                *frame_id = *it;
            }
        }
        --curr_size_;
        lru_list.remove(*frame_id);
        node_store_[*frame_id]->history_.clear();
        return true;
    }

    return false;
}


void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    // 加锁
    std::lock_guard<std::mutex> lock(latch_);

    // 判断frame_id是否合法
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        throw Exception("frame_id is invalid");
    }
    // 获取frame_id指定的节点
    auto it = node_store_.find(frame_id);
    std::shared_ptr<LRUKNode> node = nullptr;
    // 如果节点不存在，创建新节点
    if (it == node_store_.end()) {
        node = std::make_shared<LRUKNode>();
        node_store_[frame_id] = node;
    } else {
        node = it->second;
    }

    // 添加历史记录
    node->history_.push_back(current_timestamp_);
    // 更新时间戳
    ++current_timestamp_;

    // 新加入记录
    if (node->history_.size() == 1) {
        if (curr_size_ == max_size_) {
            frame_id_t frame;
            Evict(&frame);
        }
        node->is_evictable_ = true;
        ++curr_size_;
        history_list.push_back(frame_id);
        // 向node_store中添加节点
        node_store_[frame_id] = node;
    }

    // 记录达到k次，加入lru队列
    if (node->history_.size() == k_) {
        for (auto it = history_list.begin(); it != history_list.end(); it++) {
            if (*it == frame_id) {
                history_list.erase(it);
                break;
            }
        }
        lru_list.push_back(frame_id);
    }

    // 本来就在lru队列中，更新时间戳
    if (node->history_.size() > k_) {
        node->history_.pop_front();
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    // 加锁
    std::lock_guard<std::mutex> lock(latch_);
    // 判断frame_id是否合法
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        throw Exception("frame_id is invalid");
    }

    // 获取frame_id指定的节点
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        throw Exception("frame_id not found");
    }
    auto node = it->second;

    if (!node->is_evictable_ && set_evictable) {
        // 原先不可驱逐，现在可驱逐
        ++max_size_;
        ++curr_size_;
    } else if (node->is_evictable_ && !set_evictable) {
        // 原先可驱逐，现在不可驱逐
        --max_size_;
        --curr_size_;
    }
    node->is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    // 加锁
    std::lock_guard<std::mutex> lock(latch_);
    // 判断frame_id是否合法
    if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
        throw Exception("frame_id is invalid");
    }
    if (node_store_.find(frame_id) == node_store_.end()) {
        return;
    }
    // 判断frame_id是否可驱逐
    if (!node_store_[frame_id]->is_evictable_) {
        throw Exception("frame_id is not evictable");
    }

    // 删除节点
    auto node = node_store_[frame_id];
    if (node->history_.size() == k_) {
        for (auto it = lru_list.begin(); it != lru_list.end(); it++) {
            if (*it == frame_id) {
                lru_list.erase(it);
                break;
            }
        }
    } else {
        for (auto it = history_list.begin(); it != history_list.end(); it++) {
            if (*it == frame_id) {
                history_list.erase(it);
                break;
            }
        }
    }
    node_store_[frame_id]->history_.clear();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
