//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  // 获取txn的扫描谓词
  auto predicate_map = txn->GetScanPredicates();
  std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
  for (auto &t : txn_map_) {
    auto other_txn = t.second;
    // 如果other_txn已经提交，且读时间戳大于等于txn的读时间戳，检查是否有冲突
    if (other_txn->GetTransactionState() == TransactionState::COMMITTED &&
        other_txn->GetCommitTs() > txn->GetReadTs()) {
      auto write_sets = other_txn->GetWriteSets();
      // 遍历other_txn的写集合，检查是否有冲突
      for (auto &[table_oid, rids] : write_sets) {
        // 如果table没有扫描谓词，跳过
        if (predicate_map.find(table_oid) == predicate_map.end()) {
          continue;
        }
        // 获取table的扫描谓词
        auto predicates = predicate_map[table_oid];
        auto table_info = catalog_->GetTable(table_oid);
        auto schema = table_info->schema_;
        // 遍历write_set中的rids，检查是否有冲突
        for (auto &rid : rids) {
          // 获取读锁
          auto page_guard = table_info->table_->AcquireTablePageReadLock(rid);
          auto page = page_guard.As<TablePage>();

          // 获取当前tuple的元数据
          auto [tuple_meta, tuple] = table_info->table_->GetTupleWithLockAcquired(rid, page);
          // 获取当前tuple的版本记录
          auto version_link = GetVersionLink(rid);
          auto undo_link = version_link->prev_;
          std::optional<UndoLog> undo_log = GetUndoLogOptional(undo_link);

          // 回退版本记录，直到当前事务不能够访问的版本
          while (undo_log.has_value() && tuple_meta.ts_ > txn->GetReadTs()) {
            // 判断是否有冲突
            if (!tuple_meta.is_deleted_ && tuple_meta.ts_ < TXN_START_ID) {
              for (auto &predicate : predicates) {
                if (predicate->Evaluate(&tuple, schema).GetAs<bool>()) {
                  return false;
                }
              }
            }
            auto new_tuple = ReconstructTuple(&schema, tuple, tuple_meta, {*undo_log});
            if (new_tuple.has_value()) {
              tuple = new_tuple.value();
            }
            tuple_meta = TupleMeta{undo_log->ts_, undo_log->is_deleted_};
            // 获取下一个版本记录
            undo_link = undo_log->prev_version_;
            undo_log = GetUndoLogOptional(undo_link);
          }
          // 判断是否有冲突
          if (!tuple_meta.is_deleted_ && tuple_meta.ts_ < TXN_START_ID) {
            for (auto &predicate : predicates) {
              if (predicate->Evaluate(&tuple, schema).GetAs<bool>()) {
                return false;
              }
            }
          }
        }
      }
    }
  }
  return true;
}

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  ++last_commit_ts_;
  timestamp_t commit_ts = last_commit_ts_;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // 更新写集合中的tuple的时间戳为提交时间
  auto write_set = txn->GetWriteSets();
  for (auto &table : write_set) {
    TableInfo *table_info = catalog_->GetTable(table.first);
    auto rids = table.second;
    auto table_heap = table_info->table_.get();
    for (auto &rid : rids) {
      // 更新tuple的时间戳
      auto page_guard = table_info->table_->AcquireTablePageWriteLock(rid);
      auto page = page_guard.AsMut<TablePage>();
      TupleMeta tuple_meta = table_heap->GetTupleMetaWithLockAcquired(rid, page);
      page->UpdateTupleMeta({commit_ts, tuple_meta.is_deleted_}, rid);
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = last_commit_ts_.load();

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!
  auto write_set = txn->GetWriteSets();
  for (auto &table : write_set) {
    TableInfo *table_info = catalog_->GetTable(table.first);
    auto rids = table.second;
    auto table_heap = table_info->table_.get();
    for (auto &rid : rids) {
      // 更新tuple的时间戳
      auto page_guard = table_info->table_->AcquireTablePageWriteLock(rid);
      auto page = page_guard.AsMut<TablePage>();
      auto [tuple_meta, tuple] = table_heap->GetTupleWithLockAcquired(rid, page);
      std::optional<VersionUndoLink> version_link = GetVersionLink(rid);
      std::optional<UndoLog> undo_log = GetUndoLogOptional(version_link->prev_);

      if (undo_log.has_value()) {
        // revert tuple
        std::optional<Tuple> old_tuple = ReconstructTuple(&table_info->schema_, tuple, tuple_meta, {*undo_log});
        if (old_tuple.has_value()) {
          table_heap->UpdateTupleInPlaceWithLockAcquired({undo_log->ts_, undo_log->is_deleted_}, old_tuple.value(), rid,
                                                         page);
        } else {
          page->UpdateTupleMeta({undo_log->ts_, true}, rid);
        }
      } else {
        page->UpdateTupleMeta({0, true}, rid);
      }
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // 获取水印时间戳
  timestamp_t watermark_ts = running_txns_.GetWatermark();
  // 删除所有小于水印时间戳的事务
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    if (it->second->commit_ts_ != INVALID_TXN_ID &&
        (it->second->undo_logs_.empty() || it->second->commit_ts_ < watermark_ts)) {
      it = txn_map_.erase(it);  // 删除元素并更新迭代器
    } else {
      ++it;  // 仅更新迭代器
    }
  }
}

}  // namespace bustub
