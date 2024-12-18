//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstdint>
#include <memory>
#include <vector>

#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "concurrency/transaction_manager.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  // 标记已经更新过
  has_updated_ = true;
  // 初始化
  int count = 0;
  auto schema = table_info_->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple old_tuple;
  RID old_rid;
  // 遍历更新tuple
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    auto tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
    auto txn = exec_ctx_->GetTransaction();
    auto txn_manager = exec_ctx_->GetTransactionManager();
    // 判断是否是当前事务已在操作的tuple
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // 当前事务第一次执行write操作

      // 生成UndoLog中数据
      std::vector<Value> new_values;
      std::vector<bool> modified_fields;
      std::vector<Value> modified_values;
      std::vector<uint32_t> cols;
      modified_fields.reserve(plan_->target_expressions_.size());
      new_values.reserve(plan_->target_expressions_.size());
      // 生成更新后的数据
      for (auto &expr : plan_->target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, schema));
      }
      // 生成撤销日志中需要的数据
      for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
        auto old_value = old_tuple.GetValue(&schema, i);
        if (old_value.CompareEquals(new_values[i]) == CmpBool::CmpTrue) {
          modified_fields.emplace_back(false);
        } else {
          modified_fields.emplace_back(true);
          cols.push_back(i);
          modified_values.push_back(old_value);
        }
      }
      auto modified_tuple_schema = Schema::CopySchema(&schema, cols);
      auto modified_tuple = Tuple{modified_values, &modified_tuple_schema};
      // 更新TableHeap中数据
      auto new_tuple = Tuple{new_values, &schema};
      table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), tuple_meta.is_deleted_}, new_tuple, old_rid);
      
      auto pre_link = txn_manager->GetUndoLink(old_rid);
      auto undo_link = txn->AppendUndoLog(UndoLog{false, modified_fields, modified_tuple, tuple_meta.ts_, *pre_link});
      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);
      txn_manager->UpdateUndoLink(old_rid, undo_link, nullptr);
    } else if (tuple_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    } else {
      // 当前事务再次执行write操作
      // 获取修改前的数据
      auto old_link = txn_manager->GetUndoLink(old_rid);
      if (old_link.has_value()) {
        // 最开始执行的不是插入操作
        auto old_undo_log = txn->GetUndoLog(old_link->prev_log_idx_);
        auto base_tuple = ReconstructTuple(&schema, old_tuple, tuple_meta, {old_undo_log});

        // 进行当前修改
        std::vector<Value> new_values;
        std::vector<bool> modified_fields;
        std::vector<Value> modified_values;
        std::vector<uint32_t> cols;
        modified_fields.reserve(plan_->target_expressions_.size());
        new_values.reserve(plan_->target_expressions_.size());
        // 生成更新后的数据
        for (auto &expr : plan_->target_expressions_) {
          new_values.push_back(expr->Evaluate(&old_tuple, schema));
        }
        // 生成撤销日志中需要的数据
        for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
          auto old_value = base_tuple->GetValue(&schema, i);
          std::cout << "old: " << old_value.ToString() << ", new: " << new_values[i].ToString() << std::endl;
          if (old_value.CompareEquals(new_values[i]) == CmpBool::CmpTrue) {
            modified_fields.emplace_back(false);
          } else {
            modified_fields.emplace_back(true);
            cols.push_back(i);
            modified_values.push_back(old_value);
          }
        }

        auto modified_tuple_schema = Schema::CopySchema(&schema, cols);
        auto modified_tuple = Tuple{modified_values, &modified_tuple_schema};
        // 更新TableHeap中数据
        auto new_tuple = Tuple{new_values, &schema};
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), tuple_meta.is_deleted_}, new_tuple, old_rid);

        txn->ModifyUndoLog(old_link->prev_log_idx_, UndoLog{false, modified_fields, modified_tuple, old_undo_log.ts_, old_undo_log.prev_version_});
      } else {
        // 生成UndoLog中数据
        std::vector<Value> new_values;
        new_values.reserve(plan_->target_expressions_.size());
        // 生成更新后的数据
        for (auto &expr : plan_->target_expressions_) {
          new_values.push_back(expr->Evaluate(&old_tuple, schema));
        }
        // 更新TableHeap中数据
        auto new_tuple = Tuple{new_values, &schema};
        table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), tuple_meta.is_deleted_}, new_tuple, old_rid);
      }
    }

    // // 将原先的tuple设置为删除状态
    // table_info_->table_->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, old_rid);

    // // 从expression中获取update后的值
    // std::vector<Value> values;
    // values.reserve(plan_->target_expressions_.size());
    // for (auto &expr : plan_->target_expressions_) {
    //   values.push_back(expr->Evaluate(&old_tuple, schema));
    // }

    // // 生成新的tuple并插入到表中
    // auto new_tuple = Tuple{values, &schema};
    // auto new_rid_optional = table_info_->table_->InsertTuple({txn->GetTransactionTempTs(), false}, new_tuple);
    // auto new_rid = new_rid_optional.value();

    // // 更新索引
    // for (auto &index_info : indexes) {
    //   auto key_schema = index_info->key_schema_;
    //   auto attrs = index_info->index_->GetKeyAttrs();
    //   auto old_key = old_tuple.KeyFromTuple(schema, key_schema, attrs);
    //   auto new_key = new_tuple.KeyFromTuple(schema, key_schema, attrs);
    //   index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
    //   index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    // }
    ++count;
  }
  // 返回更新的tuple数目
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}
}  // namespace bustub
