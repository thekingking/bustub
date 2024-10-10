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
#include <optional>
#include <thread>
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // 将disk_request放入channel队列中
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> r;
  // 从channel队列中取出disk_request并执行
  while ((r = request_queue_.Get()) != std::nullopt) {
    // 创建线程执行disk_request
    std::thread t(&DiskScheduler::ProcessDiskRequest, this, std::move(r.value()));
    t.join();
  }
}

void DiskScheduler::ProcessDiskRequest(DiskRequest request) {
  // 执行读写操作
  if (request.is_write_) {
    disk_manager_->WritePage(request.page_id_, request.data_);
  } else {
    disk_manager_->ReadPage(request.page_id_, request.data_);
  }

  // Signal the issuer that the request has been completed
  request.callback_.set_value(true);
}

}  // namespace bustub
