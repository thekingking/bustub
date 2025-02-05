#include "execution/execution_common.h"
#include <cstdint>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  // if base tuple is deleted, return nullopt
  bool is_deleted = base_meta.is_deleted_;
  // 生成base tuple的values
  std::vector<Value> values;
  values.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    values.push_back(base_tuple.GetValue(schema, i));
  }

  // Undo操作
  for (const auto &undo_log : undo_logs) {
    // 执行删除操作
    if (undo_log.is_deleted_) {
      is_deleted = true;
      for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
        values[i] = ValueFactory::GetNullValueByType(schema->GetColumn(i).GetType());
      }
    } else {
      // 执行修改操作
      is_deleted = false;
      // 生成undo schema(只包含修改的列，原本没有这个)
      std::vector<uint32_t> cols;
      for (uint32_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
        if (undo_log.modified_fields_[i]) {
          cols.push_back(i);
        }
      }
      Schema undo_schema = Schema::CopySchema(schema, cols);
      // 执行undo操作，修改对应列
      for (uint32_t i = 0, j = 0; i < undo_log.modified_fields_.size(); ++i) {
        if (undo_log.modified_fields_[i]) {
          values[i] = undo_log.tuple_.GetValue(&undo_schema, j);
          ++j;
        }
      }
    }
  }
  // 如果tuple被删除，返回nullopt
  if (is_deleted) {
    return std::nullopt;
  }
  // 生成新的tuple
  Tuple res = Tuple(values, schema);
  res.SetRid(base_tuple.GetRid());
  return res;
}

auto DeleteTuple(Transaction *txn, TransactionManager *txn_manager, Catalog *catalog, table_oid_t table_oid, RID &rid)
    -> void {
  auto table_info = catalog->GetTable(table_oid);
  auto schema = table_info->schema_;

  auto page_guard = table_info->table_->AcquireTablePageWriteLock(rid);
  auto page = page_guard.AsMut<TablePage>();
  // 循环并发冲突检查
  // con_check:
  auto [tuple_meta, tuple] = table_info->table_->GetTupleWithLockAcquired(rid, page);
  std::optional<VersionUndoLink> version_link = txn_manager->GetVersionLink(rid);
  UndoLink undo_link = version_link->prev_;
  // 判断是否有写写冲突
  if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId()) {
    txn->SetTainted();
    throw ExecutionException("write-write conflict");
  }
  // 判断是否是当前事务已在操作的tuple
  if (tuple_meta.ts_ <= txn->GetReadTs()) {
    // 更新version_link状态
    std::vector<bool> modified_fields = std::vector<bool>(schema.GetColumnCount(), true);
    undo_link = txn->AppendUndoLog(UndoLog{false, modified_fields, tuple, tuple_meta.ts_, undo_link});
    txn_manager->UpdateVersionLink(rid, VersionUndoLink{undo_link, true});

    txn->AppendWriteSet(table_info->oid_, rid);
  } else {
    // 最开始执行的不是插入操作
    auto undo_log = txn_manager->GetUndoLogOptional(undo_link);
    if (undo_log.has_value() && !undo_log->is_deleted_) {
      auto old_tuple = ReconstructTuple(&schema, tuple, tuple_meta, {*undo_log});
      txn->ModifyUndoLog(undo_link.prev_log_idx_,
                         UndoLog{undo_log->is_deleted_, std::vector<bool>(schema.GetColumnCount(), true),
                                 old_tuple.value_or(Tuple{}), undo_log->ts_, undo_log->prev_version_});
    }
  }
  page->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, rid);
}

auto InsertTuple(Transaction *txn, TransactionManager *txn_manager, Catalog *catalog, table_oid_t table_oid,
                 Tuple &tuple) -> void {
  std::vector<RID> rids;
  auto table_info = catalog->GetTable(table_oid);
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto schema = table_info->schema_;

  // 判断是否存在索引冲突
  for (auto &index_info : indexes) {
    auto key = tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
    index_info->index_->ScanKey(key, &rids, txn);
    // 索引已经存在
    if (!rids.empty()) {
      auto page_guard = table_info->table_->AcquireTablePageReadLock(rids[0]);
      auto page = page_guard.As<TablePage>();
      auto tuple_meta = table_info->table_->GetTupleMetaWithLockAcquired(rids[0], page);
      // 索引对应的tuple已经删除或者删除了但是是另一个事务在操作（写写冲突）
      if (!tuple_meta.is_deleted_ ||
          (tuple_meta.is_deleted_ && tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId())) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict");
      }
    }
  }
  if (rids.empty()) {
    // 索引不存在，插入新的tuple，并在关联的索引中插入新的索引项
    std::optional<RID> new_rid_optional =
        table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionTempTs(), false}, tuple);
    if (!new_rid_optional.has_value()) {
      return;
    }
    RID new_rid = new_rid_optional.value();

    txn_manager->UpdateVersionLink(new_rid, VersionUndoLink{}, nullptr);
    txn->AppendWriteSet(table_info->oid_, new_rid);

    // 在关联的索引中插入新的索引项
    for (auto &index_info : indexes) {
      // 将tuple转换为key
      auto key = tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto res = index_info->index_->InsertEntry(key, new_rid, txn);
      if (!res) {
        txn->SetTainted();
        throw ExecutionException("write-write conflict");
      }
    }
  } else {
    // 索引存在，但是被删除了，更新tuple的元数据
    // 获取tuple的元数据
    RID rid = rids[0];
    auto page_guard = table_info->table_->AcquireTablePageWriteLock(rid);
    auto page = page_guard.AsMut<TablePage>();
    // 循环并发冲突检查
    // con_check:
    auto tuple_meta = table_info->table_->GetTupleMetaWithLockAcquired(rid, page);
    std::optional<VersionUndoLink> version_link = txn_manager->GetVersionLink(rid);
    UndoLink undo_link = version_link->prev_;
    // 判断是否有写写冲突
    if (tuple_meta.ts_ > txn->GetReadTs() && tuple_meta.ts_ != txn->GetTransactionId()) {
      txn->SetTainted();
      throw ExecutionException("write-write conflict");
    }
    // 判断是否是当前事务已在操作的tuple
    if (tuple_meta.ts_ != txn->GetTransactionId()) {
      // 原先tuple被删除，且已经提交
      // 更新version_link状态
      undo_link = txn->AppendUndoLog(UndoLog{
          tuple_meta.is_deleted_, std::vector<bool>(schema.GetColumnCount(), false), {}, tuple_meta.ts_, undo_link});
      txn_manager->UpdateVersionLink(rid, VersionUndoLink{undo_link, true});
      txn->AppendWriteSet(table_info->oid_, rid);
    }
    // 如果tuple已经被删除，直接更新tuple的元数据，因为删除时已经更新过undo_log
    table_info->table_->UpdateTupleInPlaceWithLockAcquired({txn->GetTransactionTempTs(), false}, tuple, rid, page);
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_manager, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  for (auto &txn : txn_manager->txn_map_) {
    fmt::println(stderr, "txn_id: {}, state: {}, read_ts: {}, commit_ts: {}", txn.first,
                 txn.second->GetTransactionState(), txn.second->GetReadTs(), txn.second->GetCommitTs());
  }
  fmt::println(stderr, "table_name: {}, table_schema: {}", table_info->name_, table_info->schema_.ToString());
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto rid = iter.GetRID();
    auto tuple = iter.GetTuple().second;
    auto tuple_meta = iter.GetTuple().first;
    auto pre_link = txn_manager->GetUndoLink(rid);
    fmt::println(stderr, "tuple={}, tuple_meta={},{}", tuple.ToString(&table_info->schema_), tuple_meta.ts_,
                 tuple_meta.is_deleted_ ? "deleted" : "not deleted");
    if (!pre_link.has_value()) {
      continue;
    }
    UndoLink undo_link = pre_link.value();
    while (undo_link.IsValid()) {
      auto undo_log = txn_manager->GetUndoLogOptional(undo_link);
      if (!undo_log.has_value()) {
        break;
      }
      auto old_tuple = ReconstructTuple(&table_info->schema_, tuple, tuple_meta, {*undo_log});
      if (old_tuple.has_value()) {
        tuple = old_tuple.value();
        tuple_meta = TupleMeta{undo_log->ts_, undo_log->is_deleted_};
        fmt::println(stderr, " => tuple={}, tuple_meta={},{}", tuple.ToString(&table_info->schema_), tuple_meta.ts_,
                     tuple_meta.is_deleted_ ? "deleted" : "not deleted");
        undo_link = undo_log->prev_version_;
      } else {
        fmt::println(stderr, " => tuple=deleted, tuple_meta={},deleted", undo_log->ts_);
        tuple_meta = TupleMeta{undo_log->ts_, true};
        undo_link = undo_log->prev_version_;
      }
    }
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
