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
#include <vector>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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

  // 获取对应索引的所有rid
  htable->ScanKey(index_key, &result_rids_, exec_ctx_->GetTransaction());
  has_scanned_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_scanned_) {
    return false;
  }
  has_scanned_ = true;
  if (result_rids_.empty()) {
    return false;
  }
  // 初始化基本信息
  auto schema = plan_->OutputSchema();
  auto tuple_meta = table_heap_->GetTuple(result_rids_[0]).first;
  bool is_deleted = tuple_meta.is_deleted_;
  auto txn = exec_ctx_->GetTransaction();
  // 获取原数据
  *rid = result_rids_[0];
  *tuple = table_heap_->GetTuple(*rid).second;

  // 事务执行，判断是否需要undo
  if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId()) {
    std::vector<UndoLog> undo_logs;
    std::optional<UndoLink> undo_link;
    std::optional<UndoLog> undo_log;

    undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(result_rids_[0]);
    auto min_ts = tuple_meta.ts_;
    while (undo_link.has_value()) {
      undo_log = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(*undo_link);
      if (!undo_log.has_value()) {
        break;
      }
      min_ts = undo_log->ts_;
      undo_logs.push_back(*undo_log);
      if (min_ts <= txn->GetReadTs()) {
        break;
      }
      undo_link = undo_log->prev_version_;
    }
    if (min_ts > txn->GetReadTs()) {
      return false;
    }
    auto new_tuple = ReconstructTuple(&schema, *tuple, tuple_meta, undo_logs);
    // 如果重构失败，直接退出
    if (!new_tuple.has_value()) {
      return false;
    }
    is_deleted = false;
    *tuple = *new_tuple;
  }
  return !is_deleted;
}

}  // namespace bustub
