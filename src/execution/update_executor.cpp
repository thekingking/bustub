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

#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->GetTableOid();
  std::vector<Tuple> new_tuples;
  std::vector<Tuple> old_tuples;
  // 遍历更新tuple
  while (child_executor_->Next(tuple, rid)) {
    // 生成更新后数据
    std::vector<Value> new_values;
    new_values.reserve(schema.GetColumnCount());
    // 生成更新后的数据
    for (auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(tuple, schema));
    }
    // 更新TableHeap中数据
    auto new_tuple = Tuple{new_values, &schema};
    old_tuples.push_back(*tuple);
    new_tuples.push_back(new_tuple);
    ++count;
  }
  bool has_index_update = false;
  for (auto &index_info : indexes) {
    auto key_schema = index_info->key_schema_;
    auto attrs = index_info->index_->GetKeyAttrs();
    // 判断有无主键更新
    for (int i = 0; i < count; ++i) {
      auto old_key = old_tuples[i].KeyFromTuple(schema, key_schema, attrs);
      auto new_key = new_tuples[i].KeyFromTuple(schema, key_schema, attrs);
      for (uint32_t k = 0; k < key_schema.GetColumnCount(); ++k) {
        if (old_key.GetValue(&key_schema, k).CompareNotEquals(new_key.GetValue(&key_schema, k)) == CmpBool::CmpTrue) {
          has_index_update = true;
          break;
        }
      }
    }
    // 检查是否存在索引冲突
    if (has_index_update) {
      for (int i = 0; i < count; ++i) {
        auto key1 = new_tuples[i].KeyFromTuple(schema, key_schema, attrs);
        for (int j = 0; j < count; ++j) {
          if (i != j) {
            auto key2 = new_tuples[j].KeyFromTuple(schema, key_schema, attrs);
            for (uint32_t k = 0; k < key_schema.GetColumnCount(); ++k) {
              if (key1.GetValue(&key_schema, k).CompareEquals(key2.GetValue(&key_schema, k)) == CmpBool::CmpTrue) {
                txn->SetTainted();
                throw ExecutionException("write-write conflict");
              }
            }
          }
        }
      }
    }
  }
  if (has_index_update) {
    // 有索引更新
    for (auto &old_tuple : old_tuples) {
      DeleteTuple(txn, txn_manager, catalog, table_oid, old_tuple);
    }
    for (auto &new_tuple : new_tuples) {
      InsertTuple(txn, txn_manager, catalog, table_oid, new_tuple);
    }
  } else {
    // 无索引更新
    for (int i = 0; i < count; ++i) {
      auto old_tuple = old_tuples[i];
      auto old_rid = old_tuple.GetRid();
      auto tuple_meta = table_info_->table_->GetTupleMeta(old_rid);
      auto txn = exec_ctx_->GetTransaction();
      auto txn_manager = exec_ctx_->GetTransactionManager();
      // 生成更新后数据
      std::vector<Value> new_values;
      new_values.reserve(schema.GetColumnCount());
      // 生成更新后的数据
      for (auto &expr : plan_->target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, schema));
      }
      // 更新TableHeap中数据
      auto new_tuple = Tuple{new_values, &schema};
      // 判断是否是当前事务已在操作的tuple
      if (tuple_meta.ts_ <= txn->GetReadTs()) {
        // 当前事务第一次执行write操作

        // 生成UndoLog中数据
        std::vector<bool> modified_fields;
        std::vector<Value> modified_values;
        std::vector<uint32_t> cols;
        modified_fields.reserve(schema.GetColumnCount());
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

        auto pre_link = txn_manager->GetUndoLink(old_rid);
        auto undo_link = txn->AppendUndoLog(
            UndoLog{false, modified_fields, modified_tuple, tuple_meta.ts_, pre_link.value_or(UndoLink{})});
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

          // 检查当前update操作产生的modified操作
          std::vector<bool> modified_fields;
          modified_fields.reserve(schema.GetColumnCount());
          // 生成当前修改记录
          for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
            auto old_value = old_tuple.GetValue(&schema, i);
            if (old_value.CompareEquals(new_values[i]) == CmpBool::CmpTrue) {
              modified_fields.emplace_back(false);
            } else {
              modified_fields.emplace_back(true);
            }
          }
          // 获取之前执行的modified记录
          std::vector<bool> old_modified_fields = old_undo_log.modified_fields_;
          auto old_modified_tuple = old_undo_log.tuple_;
          // 生成新的modified记录
          std::vector<bool> new_modified_fields;
          std::vector<Value> new_modified_values;
          std::vector<uint32_t> old_cols;
          std::vector<uint32_t> cols;
          // 生成old_modified_tuple的schema
          for (uint32_t i = 0; i < old_modified_fields.size(); ++i) {
            if (old_modified_fields[i]) {
              old_cols.push_back(i);
            }
          }
          auto old_schema = Schema::CopySchema(&schema, old_cols);
          // 生成new_modified_tuple的schema和modified记录
          for (uint32_t i = 0, j = 0; i < old_modified_fields.size(); ++i) {
            if (old_modified_fields[i]) {
              cols.push_back(i);
              new_modified_values.push_back(old_modified_tuple.GetValue(&old_schema, j));
              new_modified_fields.push_back(true);
              ++j;
            } else if (modified_fields[i]) {
              cols.push_back(i);
              new_modified_values.push_back(old_tuple.GetValue(&schema, i));
              new_modified_fields.push_back(true);
            } else {
              new_modified_fields.push_back(false);
            }
          }
          auto new_schema = Schema::CopySchema(&schema, cols);
          auto new_modified_tuple = Tuple{new_modified_values, &new_schema};

          txn->ModifyUndoLog(old_link->prev_log_idx_,
                             UndoLog{old_undo_log.is_deleted_, new_modified_fields, new_modified_tuple,
                                     old_undo_log.ts_, old_undo_log.prev_version_});
        }
      }
      table_info_->table_->UpdateTupleInPlace({txn->GetTransactionTempTs(), tuple_meta.is_deleted_}, new_tuple,
                                              old_rid);
    }
  }

  // 返回更新的tuple数目
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};
  return true;
}
}  // namespace bustub
