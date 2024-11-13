//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  child_executor_->Init();
  aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  Tuple tuple{};
  RID rid{};
  while (child_executor_->Next(&tuple, &rid)) {
    aht_->InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*aht_iterator_ != aht_->End()) {
    has_executed_ = true;
    auto key = aht_iterator_->Key();
    auto value = aht_iterator_->Val();
    std::vector<Value> values;
    values.reserve(key.group_bys_.size() + value.aggregates_.size());
    for (const auto &group_by : key.group_bys_) {
      values.push_back(group_by);
    }
    for (const auto &aggregate : value.aggregates_) {
      values.push_back(aggregate);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    ++(*aht_iterator_);
    return true;
  }
  if (!has_executed_ && plan_->group_bys_.empty()) {
    has_executed_ = true;
    std::vector<Value> values;
    for (auto &value : aht_->GenerateInitialAggregateValue().aggregates_) {
      values.emplace_back(value);
    }
    *tuple = Tuple(values, &plan_->OutputSchema());
    return true;
  }
  has_executed_ = true;
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
