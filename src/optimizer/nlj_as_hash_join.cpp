#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto ConvertPredicateToJoinKey(const AbstractExpressionRef &predicate,
                               std::vector<AbstractExpressionRef> &left_key_expressions,
                               std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(predicate);
  if (logic_expr != nullptr) {
    return ConvertPredicateToJoinKey(logic_expr->GetChildAt(0), left_key_expressions, right_key_expressions) &&
           ConvertPredicateToJoinKey(logic_expr->GetChildAt(1), left_key_expressions, right_key_expressions);
  }
  auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
  if (comparison_expr == nullptr || comparison_expr->comp_type_ != ComparisonType::Equal) {
    return false;
  }
  auto left_expr = comparison_expr->GetChildAt(0);
  auto right_expr = comparison_expr->GetChildAt(1);
  auto left_column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(left_expr);
  auto right_column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(right_expr);
  if (left_column_expr == nullptr || right_column_expr == nullptr) {
    return false;
  }
  if (left_column_expr->GetTupleIdx() == 0 && right_column_expr->GetTupleIdx() == 1) {
    left_key_expressions.push_back(left_column_expr);
    right_key_expressions.push_back(right_column_expr);
  } else if (left_column_expr->GetTupleIdx() == 1 && right_column_expr->GetTupleIdx() == 0) {
    left_key_expressions.push_back(right_column_expr);
    right_key_expressions.push_back(left_column_expr);
  } else {
    return false;
  }
  return true;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto predicate = nlj_plan.Predicate();
    if (predicate != nullptr) {
      std::vector<AbstractExpressionRef> left_key_expressions;
      std::vector<AbstractExpressionRef> right_key_expressions;
      if (ConvertPredicateToJoinKey(predicate, left_key_expressions, right_key_expressions)) {
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_key_expressions, right_key_expressions,
                                                  nlj_plan.GetJoinType());
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
