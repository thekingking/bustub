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
#include "fmt/core.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_scanned_) {
    return false;
  }
  has_scanned_ = true;

  // 根据索引获取对应tuple
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto key_schema = index_info->key_schema_;
  auto value = plan_->pred_key_->val_;
  std::vector<Value> values{value};
  Tuple index_key(values, &key_schema);
  std::vector<RID> rids;
  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  htable->ScanKey(index_key, &rids, exec_ctx_->GetTransaction());
  if (rids.empty()) {
    return false;
  }

  // 初始化基本信息
  auto schema = plan_->OutputSchema();
  auto txn = exec_ctx_->GetTransaction();
  auto tuple_meta = table_info->table_->GetTuple(rids[0]).first;
  bool is_deleted = tuple_meta.is_deleted_;

  // 获取原数据
  *rid = rids[0];
  *tuple = table_info->table_->GetTuple(*rid).second;

  auto tuple_ts = tuple_meta.ts_;
  auto txn_ts = txn->GetReadTs();

  // 事务执行，判断是否需要undo
  if (tuple_ts > txn_ts && tuple_meta.ts_ != txn->GetTransactionId()) {
    // 回退的版本记录
    std::vector<UndoLog> undo_logs;
    // 循环获取undo_log，直到undo_log的timestamp小于等于txn的timestamp
    UndoLink undo_link = txn_manager->GetUndoLink(*rid).value_or(UndoLink{});
    std::optional<UndoLog> optional_undo_log = txn_manager->GetUndoLogOptional(undo_link);
    while (optional_undo_log.has_value()) {
      undo_logs.push_back(*optional_undo_log);
      tuple_ts = optional_undo_log->ts_;
      undo_link = optional_undo_log->prev_version_;
      if (tuple_ts <= txn_ts) {
        break;
      }
      optional_undo_log = txn_manager->GetUndoLogOptional(undo_link);
    }

    if (tuple_ts > txn_ts) {
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
