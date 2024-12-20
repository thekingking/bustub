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
#include "execution/executors/insert_executor.h"
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
    // Insert the tuple into the table
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
    ++count;
  }
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
