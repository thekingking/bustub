//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

class SimpleWindowFunctionHashTable {
  friend class WindowFunctionExecutor;

 public:
  /**
   * Construct a new SimpleWindowFunctionHashTable instance.
   * @param win_exprs the window_function expressions
   * @param win_types the types of window_function
   */
  explicit SimpleWindowFunctionHashTable(AbstractExpressionRef function, const WindowFunctionType win_type,
                                         std::vector<AbstractExpressionRef> partition_by, Schema schema)
      : win_type_(win_type),
        partition_by_(std::move(partition_by)),
        function_(std::move(function)),
        schema_(std::move(schema)) {}

  /** @return The initial window_function value for this window_function executor */
  auto GenerateInitialWindowAggregateValue() -> Value {
    Value val;
    switch (win_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        val = ValueFactory::GetIntegerValue(0);
        break;
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
      case WindowFunctionType::Rank:
        // Others starts at null.
        val = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        break;
    }
    return {val};
  }

  /**
   * Combines the input into the window_function result.
   * @param[out] result The output window_function value
   * @param input The input value
   */
  auto CombineWindowAggregateValues(Value *result, const Value &input) -> Value {
    switch (win_type_) {
      case WindowFunctionType::CountStarAggregate:
        *result = result->Add(Value(TypeId::INTEGER, 1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!input.IsNull()) {
          if (result->IsNull()) {
            *result = ValueFactory::GetIntegerValue(1);
          } else {
            *result = result->Add(Value(TypeId::INTEGER, 1));
          }
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (!input.IsNull()) {
          if (result->IsNull()) {
            *result = input;
          } else {
            *result = result->Add(input);
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!input.IsNull() && (result->IsNull() || result->CompareGreaterThan(input) == CmpBool::CmpTrue)) {
          *result = input;
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (!input.IsNull() && (result->IsNull() || result->CompareLessThan(input) == CmpBool::CmpTrue)) {
          *result = input;
        }
        break;
      case WindowFunctionType::Rank:
        ++rank_count_;
        if (result->GetAs<int32_t>() != input.GetAs<int32_t>()) {
          *result = input;
          last_rank_count_ = rank_count_;
        }
        return ValueFactory::GetIntegerValue(last_rank_count_);
    }
    return *result;
  }

  /**
   * Inserts a value into the hash table and then combines it with the current window_function.
   * @param key the key to be inserted
   * @param val the value to be inserted
   */
  auto InsertCombine(const Tuple &tuple) -> Value {
    auto win_key = MakeAggregateKey(&tuple);
    auto win_val = function_->Evaluate(&tuple, schema_);
    // If the key does not exist, insert it with the initial value.
    if (ht_.count(win_key) == 0) {
      ht_.insert({win_key, GenerateInitialWindowAggregateValue()});
    }
    return CombineWindowAggregateValues(&ht_[win_key], win_val);
  }

  auto Find(Tuple &tuple) -> Value {
    auto win_key = MakeAggregateKey(&tuple);
    return ht_.find(win_key)->second;
  }

  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    std::vector<Value> keys;
    for (const auto &expr : partition_by_) {
      keys.emplace_back(expr->Evaluate(tuple, schema_));
    }
    return {keys};
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from window_function keys to window_function values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The types of window_function that we have */
  const WindowFunctionType win_type_;

  std::vector<AbstractExpressionRef> partition_by_;

  AbstractExpressionRef function_;

  Schema schema_;

  uint32_t rank_count_{0};
  uint32_t last_rank_count_{0};
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  /** The results of window function */
  std::vector<Tuple> results_;

  /** The current position in the window function tuples */
  size_t curr_pos_{0};
};
}  // namespace bustub
