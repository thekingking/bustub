#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<LimitPlanNode &>(*optimized_plan);
    auto child = limit_plan.GetChildAt(0);
    if (child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child);
      return std::make_shared<TopNPlanNode>(sort_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
