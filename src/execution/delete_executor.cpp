//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>
#include <optional>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // Initialize the plan node
  plan_ = plan;
  // Initialize the child executor
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  // Mark as deleted
  has_deleted_ = true;
  // Initialize
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    auto tuple_meta = table_info->table_->GetTupleMeta(*rid);
    auto txn = exec_ctx_->GetTransaction();
    auto txn_manager = exec_ctx_->GetTransactionManager();
    // 判断是否是当前事务已在操作的tuple
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // 更新事务的undo log
      std::vector<bool> modified_fields = std::vector<bool>(schema.GetColumnCount(), true);
      auto pre_link = txn_manager->GetUndoLink(*rid);
      auto undo_link = txn->AppendUndoLog(UndoLog{false, modified_fields, *tuple, tuple_meta.ts_, *pre_link});
      txn->AppendWriteSet(plan_->GetTableOid(), *rid);
      txn_manager->UpdateUndoLink(*rid, undo_link, nullptr);
    } else if (tuple_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    } else {
      auto old_link = txn_manager->GetUndoLink(*rid);
      if (old_link.has_value()) {
        // 最开始执行的不是插入操作
        auto old_undo_log = txn->GetUndoLog(old_link->prev_log_idx_);
        auto base_tuple = ReconstructTuple(&schema, *tuple, tuple_meta, {old_undo_log});
        txn->ModifyUndoLog(old_link->prev_log_idx_,
                           UndoLog{false, std::vector<bool>(schema.GetColumnCount(), true), base_tuple.value(),
                                   old_undo_log.ts_, old_undo_log.prev_version_});
      }
    }

    // Delete the tuple from the table
    table_info->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, *rid);
    // Delete the tuple from the indexes
    for (auto &index_info : indexes) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  // Return the number of deleted tuples
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
