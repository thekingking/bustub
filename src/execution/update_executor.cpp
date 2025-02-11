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
#include <optional>
#include <vector>

#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "fmt/core.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
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
  auto txn = exec_ctx_->GetTransaction();
  auto txn_manager = exec_ctx_->GetTransactionManager();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto schema = table_info->schema_;

  std::vector<Tuple> new_tuples;
  std::vector<Tuple> old_tuples;
  std::vector<RID> rids;
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
    rids.push_back(*rid);
    ++count;
  }

  // 返回更新的tuple数目
  std::vector<Value> result{{TypeId::INTEGER, count}};
  *tuple = Tuple{result, &GetOutputSchema()};

  // 检查有无索引更新
  auto has_index_update = false;
  for (uint32_t col_idx = 0; col_idx < schema.GetColumnCount(); ++col_idx) {
    auto expr = plan_->target_expressions_[col_idx];
    auto *column_value_expr = dynamic_cast<const ColumnValueExpression *>(expr.get());
    if (column_value_expr == nullptr) {
      for (auto index : indexes) {
        auto key_attrs = index->index_->GetKeyAttrs();
        if (std::find(key_attrs.begin(), key_attrs.end(), col_idx) != key_attrs.end()) {
          has_index_update = true;
          break;
        }
      }
    }
  }
  // 有索引更新
  if (has_index_update) {
    for (int i = 0; i < count; ++i) {
      DeleteTuple(txn, txn_manager, table_info, rids[i]);
    }
    for (auto &new_tuple : new_tuples) {
      InsertTuple(txn, txn_manager, table_info, indexes, new_tuple);
    }
    return true;
  }

  // 无索引更新
  for (int i = 0; i < count; ++i) {
    RID old_rid = rids[i];

    auto page_guard = table_info->table_->AcquireTablePageWriteLock(old_rid);
    auto page = page_guard.AsMut<TablePage>();

    auto [tuple_meta, cur_tuple] = table_info->table_->GetTupleWithLockAcquired(old_rid, page);
    std::optional<VersionUndoLink> version_link = txn_manager->GetVersionLink(old_rid);
    UndoLink undo_link = version_link->prev_;

    // 判断是否有写写冲突
    if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }

    std::vector<Value> new_values;
    new_values.reserve(schema.GetColumnCount());
    // 生成更新后的数据
    for (auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&cur_tuple, schema));
    }
    // 更新TableHeap中数据
    auto new_tuple = Tuple{new_values, &schema};

    // 判断是否是当前事务已在操作的tuple
    if (tuple_meta.ts_ <= txn->GetReadTs()) {
      // 当前事务第一次执行write操作

      // 生成UndoLog中数据
      std::vector<bool> new_modified_fields;
      std::vector<Value> new_modified_values;
      std::vector<uint32_t> cols;
      new_modified_fields.reserve(schema.GetColumnCount());
      // 生成撤销日志中需要的数据
      for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
        auto old_value = cur_tuple.GetValue(&schema, i);
        if (old_value.CompareEquals(new_values[i]) == CmpBool::CmpTrue) {
          new_modified_fields.emplace_back(false);
        } else {
          new_modified_fields.emplace_back(true);
          cols.push_back(i);
          new_modified_values.push_back(old_value);
        }
      }
      auto new_schema = Schema::CopySchema(&schema, cols);
      auto new_modified_tuple = Tuple{new_modified_values, &new_schema};

      // 更新version_link状态
      undo_link = txn->AppendUndoLog(
          UndoLog{tuple_meta.is_deleted_, new_modified_fields, new_modified_tuple, tuple_meta.ts_, undo_link});
      version_link = VersionUndoLink{undo_link, true};
      txn_manager->UpdateVersionLink(old_rid, version_link);

      // 将修改的tuple信息写入write set中
      txn->AppendWriteSet(plan_->GetTableOid(), old_rid);

    } else {
      // 当前事务再次执行write操作
      // 获取修改前的数据
      // 最开始执行的不是插入操作
      auto undo_log = txn_manager->GetUndoLogOptional(undo_link);
      if (undo_log.has_value() && !undo_log->is_deleted_) {
        // 获取之前执行的modified记录
        std::vector<bool> old_modified_fields = undo_log->modified_fields_;
        std::optional<Tuple> old_tuple = ReconstructTuple(&schema, cur_tuple, tuple_meta, {*undo_log});

        // 生成新的modified记录
        std::vector<bool> new_modified_fields;
        std::vector<Value> new_modified_values;
        std::vector<uint32_t> cols;
        // 生成new_modified_tuple的schema和modified记录
        for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
          auto old_value = cur_tuple.GetValue(&schema, i);
          if (old_modified_fields[i] || old_value.CompareEquals(new_values[i]) != CmpBool::CmpTrue) {
            cols.push_back(i);
            new_modified_values.push_back(old_tuple->GetValue(&schema, i));
            new_modified_fields.push_back(true);
          } else {
            new_modified_fields.push_back(false);
          }
        }
        auto new_schema = Schema::CopySchema(&schema, cols);
        auto new_modified_tuple = Tuple{new_modified_values, &new_schema};

        // 更新undo_log
        txn->ModifyUndoLog(undo_link.prev_log_idx_,
                           UndoLog{undo_log->is_deleted_, new_modified_fields, new_modified_tuple, undo_log->ts_,
                                   undo_log->prev_version_});
      }
    }

    // 更新tuple
    table_info->table_->UpdateTupleInPlaceWithLockAcquired({txn->GetTransactionTempTs(), tuple_meta.is_deleted_},
                                                            new_tuple, old_rid, page);
  }

  return true;
}
}  // namespace bustub
