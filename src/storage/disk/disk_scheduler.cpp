//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <chrono>
#include <mutex>
#include <optional>
#include <thread>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // Spawn the background thread
  for (size_t i = 0; i < min_threads_; i++) {
    workers_.emplace_back(&DiskScheduler::NewThread, this);
  }
}

DiskScheduler::~DiskScheduler() {
  {
    // 加锁
    std::unique_lock<std::mutex> lock(queue_mutex_);
    // 停止线程池
    stop_ = true;
  }
  // 唤醒所有线程
  condition_.notify_all();
  // 等待所有线程结束
  for (auto &worker : workers_) {
    worker.join();
  }
}

void DiskScheduler::NewThread() {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      // 等待任务队列不为空或者stop_为true
      condition_.wait(lock, [this] { return !tasks_.empty() || stop_; });
      // 如果stop_为true，退出线程
      if (stop_ && tasks_.empty()) {
        return;
      }
      // 取出任务
      task = std::move(tasks_.front());
      tasks_.pop();
    }
    // 执行任务
    task();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // 将disk_request放入channel队列中
  auto request = std::make_shared<DiskRequest>(std::move(r));
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    if (stop_) {
      throw Exception("Enqueue on stopped ThreadPool");
    }
    tasks_.emplace([this, request = std::move(request)]() mutable {
      // 执行磁盘请求
      StartWorkerThread(std::move(request));
    });
    if (workers_.size() < max_threads_ && tasks_.size() > thread_condition_) {
      workers_.emplace_back(&DiskScheduler::NewThread, this);
    }
  }
  condition_.notify_one();
}

void DiskScheduler::StartWorkerThread(std::shared_ptr<DiskRequest> request) {
  // 执行读写操作
  if (request->is_write_) {
    disk_manager_->WritePage(request->page_id_, request->data_);
  } else {
    disk_manager_->ReadPage(request->page_id_, request->data_);
  }
  // Signal the issuer that the request has been completed
  request->callback_.set_value(true);
}
}  // namespace bustub
