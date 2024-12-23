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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->GetTableOid();

  // 从child_executor中获取待插入的tuple
  while (child_executor_->Next(tuple, rid)) {
    InsertTuple(txn, txn_mgr, catalog, table_oid, *tuple);
    ++count;
  }
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
