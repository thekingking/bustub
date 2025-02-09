//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  /**
   * Construct a new hash join key.
   * @param values the values of the hash join key
   */
  std::vector<Value> values_;

  /**
   * Compares two hash join keys for equality.
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join keys have equivalent values, `false` otherwise
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.values_.size(); ++i) {
      if (values_[i].CompareEquals(other.values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

struct HashJoinValue {
  /**
   * Construct a new hash join value.
   * @param values the values of the hash join value
   */
  std::vector<Value> values_;
};
}  // namespace bustub
namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : key.values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as a HashJoinKey */
  auto MakeHashJoinKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &expressions, const Schema &schema)
      -> HashJoinKey {
    std::vector<Value> keys;
    keys.reserve(expressions.size());
    for (const auto &expr : expressions) {
      keys.emplace_back(expr->Evaluate(tuple, schema));
    }
    return {keys};
  }

  /** @return The tuple as a HashJoinValue */
  auto MakeHashJoinValue(const Tuple *tuple, const Schema &schema) -> HashJoinValue {
    std::vector<Value> vals;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      vals.emplace_back(tuple->GetValue(&schema, i));
    }
    return {vals};
  }

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** The child executor that produces tuples for the left side of join. */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** The child executor that produces tuples for the right side of join. */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** The current tuple from the left child executor. */
  std::unordered_map<HashJoinKey, std::vector<HashJoinValue>> hash_table_;

  std::list<Tuple> results_;
};

}  // namespace bustub
