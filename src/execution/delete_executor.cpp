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

#include <memory>

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
    // Delete the tuple from the table
    table_info->table_->UpdateTupleMeta({0, true}, *rid);
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
