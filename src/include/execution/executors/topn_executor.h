//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

  struct Compare {
    explicit Compare(const TopNPlanNode *plan) : plan_(plan) {}
    auto operator()(const Tuple &lhs, const Tuple &rhs) const -> bool {
      for (const auto &order_by : plan_->GetOrderBy()) {
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
    }

    const TopNPlanNode *plan_;
  };

 private:
  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The tuples produced by the child executor */
  std::vector<Tuple> top_tuples_;
  /** The current position in the sorted tuples */
  size_t curr_pos_{0};
};
}  // namespace bustub
