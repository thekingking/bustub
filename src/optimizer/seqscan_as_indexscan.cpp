#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  // 递归执行优化
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(children);

  // 如果是 SeqScanPlanNode
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<SeqScanPlanNode &>(*optimized_plan);
    auto predicate = seq_scan_plan.filter_predicate_;
    if (predicate != nullptr) {
      auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
      // 如果Expression是ComparisonExpression，且比较类型是Equal
      if (comparison_expr != nullptr && comparison_expr->comp_type_ == ComparisonType::Equal) {
        // 将左侧的ColumnValueExpression和右侧的ConstantValueExpression提取出来
        auto column_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(0));
        if (column_expr != nullptr) {
          // 获取ColumnValueExpression的列索引和表名
          auto column_idx = column_expr->GetColIdx();
          auto table_name = seq_scan_plan.table_name_;
          auto indexes = catalog_.GetTableIndexes(table_name);
          // 遍历所有索引，找到包含该列的索引，如果不存在则返回原始的SeqScanPlanNode
          for (auto &index_info : indexes) {
            auto attrs = index_info->index_->GetKeyAttrs();
            // 如果索引包含该列
            if (std::find(attrs.begin(), attrs.end(), column_idx) != attrs.end()) {
              auto constant_expr = std::dynamic_pointer_cast<ConstantValueExpression>(comparison_expr->GetChildAt(1));
              if (constant_expr != nullptr) {
                return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_,
                                                           index_info->index_oid_, seq_scan_plan.filter_predicate_,
                                                           constant_expr.get());
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
