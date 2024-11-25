#include "execution/executors/window_function_executor.h"
#include <cstdint>
#include <memory>
#include "catalog/schema.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  std::vector<Tuple> tuples{};
  RID rid{};
  Tuple tuple{};
  // 从child_executor获取所有的tuple
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(tuple);
  }
  if (plan_->window_functions_.empty()) {
    return;
  }
  auto schema = child_executor_->GetOutputSchema();
  bool has_order_by = false;
  // 对tuples进行排序，由于所有排序相同所以只进行一次排序
  for (const auto &window : plan_->window_functions_) {
    auto order_by = window.second.order_by_;
    if (!order_by.empty()) {
      has_order_by = true;
      std::sort(tuples.begin(), tuples.end(), [this, &order_by](const Tuple &lhs, const Tuple &rhs) {
        for (const auto &order_by : order_by) {
          auto order_by_type = order_by.first;
          auto expr = order_by.second.get();
          auto lhs_val = expr->Evaluate(&lhs, plan_->OutputSchema());
          auto rhs_val = expr->Evaluate(&rhs, plan_->OutputSchema());
          if (lhs_val.CompareLessThan(rhs_val) == CmpBool::CmpTrue) {
            return order_by_type != OrderByType::DESC;
          }
          if (lhs_val.CompareGreaterThan(rhs_val) == CmpBool::CmpTrue) {
            return order_by_type == OrderByType::DESC;
          }
        }
        return true;
      });
      break;
    }
  }
  if (!has_order_by) {
    // 为每个window function创建一个hash table
    std::vector<SimpleWindowFunctionHashTable> ahts;
    // 对每个window function进行处理，每个window_function分开处理
    uint32_t i = plan_->columns_.size() - plan_->window_functions_.size();
    auto len = plan_->columns_.size();
    while (i < len) {
      auto window_function = plan_->window_functions_.at(i);

      auto window_function_type = window_function.type_;
      auto partition_by = window_function.partition_by_;
      auto function = window_function.function_;
      auto order_by = window_function.order_by_;
      SimpleWindowFunctionHashTable aht =
          (window_function_type == WindowFunctionType::Rank)
              ? SimpleWindowFunctionHashTable{order_by[0].second, window_function_type, partition_by, schema}
              : SimpleWindowFunctionHashTable{function, window_function_type, partition_by, schema};
      for (const auto &tuple : tuples) {
        aht.InsertCombine(tuple);
      }
      ahts.emplace_back(aht);
      ++i;
    }
    // 计算window function的最终结果，将结果存入results_
    for (auto &tuple : tuples) {
      std::vector<Value> values;
      auto iter = ahts.begin();
      for (auto &expr : plan_->columns_) {
        // 如果是普通的column，直接取值
        auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr);
        if (column_expr->GetColIdx() != static_cast<uint32_t>(-1)) {
          values.emplace_back(tuple.GetValue(&schema, column_expr->GetColIdx()));
        } else {
          auto aht = *iter;
          values.emplace_back(aht.Find(tuple));
          ++iter;
        }
      }
      Tuple res = Tuple(values, &plan_->OutputSchema());
      results_.emplace_back(res);
    }
  } else {
    // 为每个window function创建一个hash table
    std::vector<SimpleWindowFunctionHashTable> ahts;
    uint32_t i = plan_->columns_.size() - plan_->window_functions_.size();
    auto len = plan_->columns_.size();
    while (i < len) {
      auto window_function = plan_->window_functions_.at(i);

      auto window_function_type = window_function.type_;
      auto partition_by = window_function.partition_by_;
      auto function = window_function.function_;
      auto order_by = window_function.order_by_;

      SimpleWindowFunctionHashTable aht =
          (window_function_type == WindowFunctionType::Rank)
              ? SimpleWindowFunctionHashTable{order_by[0].second, window_function_type, partition_by, schema}
              : SimpleWindowFunctionHashTable{function, window_function_type, partition_by, schema};
      ahts.emplace_back(aht);
      ++i;
    }
    for (auto &tuple : tuples) {
      std::vector<Value> values;
      int j = 0;
      for (auto &expr : plan_->columns_) {
        // 如果是普通的column，直接取值
        auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(expr);
        if (column_expr->GetColIdx() != static_cast<uint32_t>(-1)) {
          values.emplace_back(tuple.GetValue(&schema, column_expr->GetColIdx()));
        } else {
          values.emplace_back(ahts[j].InsertCombine(tuple));
          ++j;
        }
      }
      Tuple res = Tuple(values, &plan_->OutputSchema());
      results_.emplace_back(res);
    }
  }

  curr_pos_ = 0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (curr_pos_ >= results_.size()) {
    return false;
  }
  *tuple = results_[curr_pos_];
  ++curr_pos_;
  return true;
}
}  // namespace bustub
