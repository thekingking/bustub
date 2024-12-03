#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  // TODO(fall2023): implement me!
  if (current_reads_.empty()) {
    watermark_ = read_ts;
  }
  ++current_reads_[read_ts];
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  auto iter = current_reads_.find(read_ts);
  if (iter == current_reads_.end()) {
    throw Exception("read ts not found");
  }
  if (iter->second == 1) {
    current_reads_.erase(iter);
  } else {
    --iter->second;
  }
  if (read_ts == watermark_ && !current_reads_.empty()) {
    while (watermark_ < commit_ts_ && current_reads_.find(watermark_) == current_reads_.end()) {
      ++watermark_;
    }
  }
}

}  // namespace bustub
