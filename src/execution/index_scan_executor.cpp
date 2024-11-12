//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();

  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  auto key_schema = index_info->key_schema_;
  auto value = plan_->pred_key_->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &key_schema);

  htable->ScanKey(index_key, &result_rids_, exec_ctx_->GetTransaction());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_scanned_) {
    return false;
  }
  has_scanned_ = true;
  if (result_rids_.empty()) {
    return false;
  }
  auto meta = table_heap_->GetTuple(result_rids_[0]).first;
  if (meta.is_deleted_) {
    return false;
  }
  *rid = result_rids_[0];
  *tuple = table_heap_->GetTuple(*rid).second;
  return true;
}

}  // namespace bustub
