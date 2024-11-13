//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  
}

void HashJoinExecutor::Init() { 
  left_child_->Init();
  right_child_->Init();
  RID rid{};
  Tuple tuple{};
  auto right_expressions = plan_->RightJoinKeyExpressions();
  auto schema = right_child_->GetOutputSchema();
  // 将右表中的数据存入哈希表中
  while (right_child_->Next(&tuple, &rid)) {
    auto key = MakeHashJoinKey(&tuple, right_expressions, schema);
    hash_table_[key].push_back(MakeHashJoinValue(&tuple, schema));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // 从保存的结果中取出一个tuple
  if (!results_.empty()) {
    *tuple = results_.front();
    results_.pop_front();
    return true;
  }
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();
  auto left_expressions = plan_->LeftJoinKeyExpressions();
  while (left_child_->Next(tuple, rid)) {
    // 将左表中一个tuple转换为key
    auto key = MakeHashJoinKey(tuple, left_expressions, left_schema);
    // 如果在哈希表中找到了对应的key
    if (hash_table_.find(key) != hash_table_.end()) {
      // 将满足条件的tuple加入到results_中
      for (auto &value : hash_table_[key]) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
          values.push_back(tuple->GetValue(&left_schema, i));
        }
        for (auto &val : value.values_) {
          values.push_back(val);
        }
        *tuple = Tuple(values, &GetOutputSchema());
        results_.push_back(*tuple);
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      // 如果右表中没有满足条件的tuple，且是left join，将左表中的tuple加入到results_中
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        values.push_back(tuple->GetValue(&left_schema, i));
      }
      for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); ++idx) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      results_.push_back(*tuple);
    }
    // 如果results_中有元组，取出一个返回
    if (!results_.empty()) {
      *tuple = results_.front();
      results_.pop_front();
      return true;
    }
  }
  return false; 
}

}  // namespace bustub
