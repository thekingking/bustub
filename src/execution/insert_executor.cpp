//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <optional>

#include "catalog/schema.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // Initialize the plan node
  plan_ = plan;
  // Initialize the child executor
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  // Initialize the child executor
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  // 标记已经插入过
  has_inserted_ = true;
  // 初始化
  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  auto txn = exec_ctx_->GetTransaction();

  // 从child_executor中获取待插入的tuple
  while (child_executor_->Next(tuple, rid)) {
    std::vector<RID> rids;
    // 判断是否存在索引冲突
    for (auto &index_info : indexes) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->ScanKey(key, &rids, txn);
      if (!rids.empty()) {
        auto tuple_meta = table_info->table_->GetTupleMeta(rids[0]);
        if (!tuple_meta.is_deleted_ || (tuple_meta.is_deleted_ && tuple_meta.ts_ > txn->GetReadTs() &&
                                        tuple_meta.ts_ != txn->GetTransactionId())) {
          txn->SetTainted();
          throw ExecutionException("write-write conflict");
        }
      }
    }
    if (rids.empty()) {
      // 索引不存在，插入新的tuple，并在关联的索引中插入新的索引项
      std::optional<RID> new_rid_optional =
          table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, *tuple);
      if (!new_rid_optional.has_value()) {
        continue;
      }
      RID new_rid = new_rid_optional.value();

      exec_ctx_->GetTransactionManager()->UpdateVersionLink(*rid, std::nullopt, nullptr);
      txn->AppendWriteSet(table_info->oid_, new_rid);

      // 在关联的索引中插入新的索引项
      for (auto &index_info : indexes) {
        // 将tuple转换为key
        auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        auto res = index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
        if (!res) {
          txn->SetTainted();
          throw ExecutionException("write-write conflict");
        }
      }
    } else {
      // 索引存在，但是被删除了，更新tuple的元数据
      auto tuple_meta = table_info->table_->GetTupleMeta(rids[0]);
      if (tuple_meta.ts_ != txn->GetTransactionId()) {
        // 原先tuple被删除，且已经提交
        auto pre_link = exec_ctx_->GetTransactionManager()->GetUndoLink(rids[0]);
        UndoLink undo_link = txn->AppendUndoLog(UndoLog{tuple_meta.is_deleted_,
                                                        std::vector<bool>(schema.GetColumnCount(), false),
                                                        {},
                                                        tuple_meta.ts_,
                                                        pre_link.value_or(UndoLink{})});
        table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, rids[0]);
        exec_ctx_->GetTransactionManager()->UpdateUndoLink(rids[0], undo_link, nullptr);
        txn->AppendWriteSet(table_info->oid_, rids[0]);
      }
      // 如果tuple已经被删除，直接更新tuple的元数据
      table_info->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, *tuple, rids[0]);
    }
    ++count;
  }
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
