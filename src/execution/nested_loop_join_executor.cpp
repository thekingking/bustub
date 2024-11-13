//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <sys/types.h>
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  left_done_ = !left_executor_->Next(&left_tuple_, &rid);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID right_rid;
  Tuple right_tuple;
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  // 判断左侧是否还有元组
  while (!left_done_) {
    // 从右侧table中取tuple
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        right_done_ = true;
        std::vector<Value> values;
        for (uint32_t idx = 0; idx < left_schema.GetColumnCount(); ++idx) {
          values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
        }
        for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); ++idx) {
          values.emplace_back(right_tuple.GetValue(&right_schema, idx));
        }
        *tuple = Tuple(values, &plan_->OutputSchema());
        std::cout << tuple->ToString(&plan_->OutputSchema()) << std::endl;
        return true;
      }
    }
    // 右侧中没有符合条件的元组，join为left join，需要输出左侧元组
    if (!right_done_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_schema.GetColumnCount(); ++idx) {
        values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
      }
      for (uint32_t idx = 0; idx < right_schema.GetColumnCount(); ++idx) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &plan_->OutputSchema());
      left_done_ = !left_executor_->Next(&left_tuple_, rid);
      right_executor_->Init();
      right_done_ = false;
      return true;
    }
    // 右侧遍历完，重置右侧，左侧取下一个元组
    left_done_ = !left_executor_->Next(&left_tuple_, rid);
    right_executor_->Init();
    right_done_ = false;
  }
  return false;
}
}  // namespace bustub
