#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto top_entries = std::priority_queue<Tuple, std::vector<Tuple>, Compare>(Compare(plan_));
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    top_entries.push(tuple);
    if (top_entries.size() > plan_->GetN()) {
      top_entries.pop();
    }
  }
  top_tuples_.clear();
  top_tuples_.reserve(top_entries.size());
  while (!top_entries.empty()) {
    top_tuples_.emplace_back(top_entries.top());
    top_entries.pop();
  }
  std::reverse(top_tuples_.begin(), top_tuples_.end());
  curr_pos_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (curr_pos_ >= top_tuples_.size()) {
    return false;
  }
  *tuple = top_tuples_[curr_pos_];
  ++curr_pos_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return top_tuples_.size(); };

}  // namespace bustub
