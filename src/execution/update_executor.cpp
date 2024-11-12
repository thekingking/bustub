//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  // 标记已经更新过
  has_updated_ = true;
  // 初始化
  int count = 0;
  auto schema = table_info_->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple old_tuple;
  RID old_rid;
  // 遍历更新tuple
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 将原先的tuple设置为删除状态
    table_info_->table_->UpdateTupleMeta({0, true}, old_rid);

    // 从expression中获取update后的值
    std::vector<Value> values;
    values.reserve(plan_->target_expressions_.size());
    for (auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&old_tuple, schema));
    }

    // 生成新的tuple并插入到表中
    auto new_tuple = Tuple{values, &schema};
    auto new_rid_optional = table_info_->table_->InsertTuple({0, false}, new_tuple);
    auto new_rid = new_rid_optional.value();

    // 更新索引
    for (auto &index_info : indexes) {
      auto key_schema = index_info->key_schema_;
      auto attrs = index_info->index_->GetKeyAttrs();
      auto old_key = old_tuple.KeyFromTuple(schema, key_schema, attrs);
      auto new_key = new_tuple.KeyFromTuple(schema, key_schema, attrs);
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }
  // 返回更新的tuple数目
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}
}  // namespace bustub
