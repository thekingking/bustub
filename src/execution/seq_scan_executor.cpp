//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <cstddef>
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_iterator_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = plan_->OutputSchema();
  while (!table_iterator_->IsEnd()) {
    auto [tuple_meta, tuple_data] = table_iterator_->GetTuple();
    ++(*table_iterator_);
    if (!tuple_meta.is_deleted_ && !(plan_->filter_predicate_ != nullptr &&
                                     !plan_->filter_predicate_->Evaluate(&tuple_data, schema).GetAs<bool>())) {
      *tuple = tuple_data;
      *rid = tuple_data.GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
