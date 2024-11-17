#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() { 
    child_executor_->Init();
    tuples_.clear();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
        tuples_.emplace_back(tuple);
    }
    std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &lhs, const Tuple &rhs) {
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
    });
    curr_pos_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (curr_pos_ >= tuples_.size()) {
        return false;
    }
    *tuple = tuples_[curr_pos_];
    ++curr_pos_;
    return true; 
}
}  // namespace bustub
