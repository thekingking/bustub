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

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");
  for (auto &txn : txn_mgr->txn_map_) {
    fmt::println(stderr, "txn_id: {}, state: {}, read_ts: {}, commit_ts: {}", txn.first,
                 txn.second->GetTransactionState(), txn.second->GetReadTs(), txn.second->GetCommitTs());
  }
  fmt::println(stderr, "table_name: {}, table_schema: {}", table_info->name_, table_info->schema_.ToString());
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    fmt::println(stderr, "tuple={}", iter.GetTuple().second.ToString(&table_info->schema_));
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
