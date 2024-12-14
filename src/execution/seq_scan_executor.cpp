//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <memory>
#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_iterator_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = plan_->OutputSchema();
  while (!table_iterator_->IsEnd()) {
    // 从TableHeap中获取数据
    auto [tuple_meta, tuple_data] = table_iterator_->GetTuple();
    *rid = tuple_data.GetRid();
    *tuple = tuple_data;
    // 更新table_iterator_
    ++(*table_iterator_);

    // 获取tuple的timestamp和当前txn的timestamp
    auto txn = exec_ctx_->GetTransaction();
    auto tuple_ts = tuple_meta.ts_;
    auto txn_ts = txn->GetReadTs();
    bool is_deleted = tuple_meta.is_deleted_;

    // 如果tuple的timestamp大于txn的timestamp
    // 1. 如果tuple_ts >
    // TXN_START_ID，说明正在进行update，判断是否是当前txn正在进行update，如果不是当前txn的update，需要undo
    // 2. 如果tuple_ts < TXN_START_ID，直接undo，直到没有undo_log或者undo_log的timestamp大于txn的timestamp
    if (tuple_ts > txn_ts && tuple_ts != txn->GetTransactionId()) {
      std::vector<UndoLog> undo_logs;
      std::optional<UndoLink> undo_link;
      std::optional<UndoLog> undo_log;
      // 循环获取undo_log，直到undo_log的timestamp小于等于txn的timestamp
      undo_link = exec_ctx_->GetTransactionManager()->GetUndoLink(*rid);
      auto min_ts = tuple_ts;
      while (undo_link.has_value()) {
        undo_log = exec_ctx_->GetTransactionManager()->GetUndoLogOptional(*undo_link);
        if (!undo_log.has_value()) {
          break;
        }
        min_ts = undo_log->ts_;
        undo_logs.push_back(*undo_log);
        if (min_ts <= txn_ts) {
          break;
        }
        undo_link = undo_log->prev_version_;
      }
      if (min_ts > txn_ts) {
        continue;
      }
      // 重构tuple
      auto new_tuple = bustub::ReconstructTuple(&schema, *tuple, tuple_meta, undo_logs);
      // 如果重构失败，继续下一个tuple
      if (!new_tuple.has_value()) {
        continue;
      }
      is_deleted = false;
      *tuple = *new_tuple;
    }
    if (!is_deleted && !(plan_->filter_predicate_ != nullptr &&
                         !plan_->filter_predicate_->Evaluate(&tuple_data, schema).GetAs<bool>())) {
      return true;
    }
  }
  return false;
}

}  // namespace bustub
