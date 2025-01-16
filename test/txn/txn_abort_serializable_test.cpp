#include <cstdio>
#include "common/bustub_instance.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "txn_common.h"  // NOLINT

namespace bustub {

// NOLINTBEGIN(bugprone-unchecked-optional-access)

TEST(TxnBonusTest, DISABLED_SerializableTest) {  // NOLINT
  fmt::println(stderr, "--- SerializableTest2: Serializable ---");
  {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int, b int primary key)");
    auto table_info = bustub->catalog_->GetTable("maintable");
    auto txn1 = BeginTxnSerializable(*bustub, "txn1");
    WithTxn(txn1,
            ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 100), (1, 101), (0, 102), (0, 103)"));
    WithTxn(txn1, CommitTxn(*bustub, _var, _txn));

    auto txn2 = BeginTxnSerializable(*bustub, "txn2");
    auto txn3 = BeginTxnSerializable(*bustub, "txn3");
    auto txn_read = BeginTxnSerializable(*bustub, "txn_read");
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 0 WHERE a = 1"));
    WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET a = 1 WHERE a = 0"));
    TxnMgrDbg("after two updates", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn_read, ExecuteTxn(*bustub, _var, _txn, "SELECT * FROM maintable WHERE a = 0"));
    WithTxn(txn2, CommitTxn(*bustub, _var, _txn));
    WithTxn(txn3, CommitTxn(*bustub, _var, _txn, EXPECT_FAIL));
    WithTxn(txn_read, CommitTxn(*bustub, _var, _txn));
    // test continues on Gradescope...
  }
}

TEST(TxnBonusTest, DISABLED_AbortTest) {  // NOLINT
  fmt::println(stderr, "--- AbortTest1: Simple Abort ---");
  {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    auto table_info = bustub->catalog_->GetTable("maintable");
    auto txn1 = BeginTxn(*bustub, "txn1");
    WithTxn(txn1, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 233), (2, 2333)"));
    WithTxn(txn1, AbortTxn(*bustub, _var, _txn));
    TxnMgrDbg("after abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn2 = BeginTxn(*bustub, "txn2");
    WithTxn(txn2, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (1, 2333), (2, 23333), (3, 233)"));
    WithTxn(txn2, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TxnMgrDbg("after insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    WithTxn(txn2, CommitTxn(*bustub, _var, _txn));
    TxnMgrDbg("after commit", bustub->txn_manager_.get(), table_info, table_info->table_.get());
    auto txn3 = BeginTxn(*bustub, "txn3");
    WithTxn(txn3, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {1, 2333},
                                      {2, 23333},
                                      {3, 233},
                                  }));
    TableHeapEntryNoMoreThan(*bustub, table_info, 3);
    // test continues on Gradescope...
  }
}

TEST(TxnBonusTest, DISABLED_AbortTest2) {  // NOLINT
  fmt::println(stderr, "--- AbortTest1: Simple Abort ---");
  {
    auto bustub = std::make_unique<BustubInstance>();
    EnsureIndexScan(*bustub);
    Execute(*bustub, "CREATE TABLE maintable(a int primary key, b int)");
    auto table_info = bustub->catalog_->GetTable("maintable");
    auto txn_prepare_1 = BeginTxn(*bustub, "txn_prepare_1");
    WithTxn(txn_prepare_1, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (2, 2), (3, 2333)"));
    WithTxn(txn_prepare_1, CommitTxn(*bustub, _var, _txn));
    auto txn_verify_1 = BeginTxn(*bustub, "txn_verify_1");

    auto txn_prepare_2 = BeginTxn(*bustub, "txn_prepare_2");
    WithTxn(txn_prepare_2, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (4, 23333), (5, 233333)"));
    WithTxn(txn_prepare_2, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET b = 233 WHERE a = 2"));
    WithTxn(txn_prepare_2, ExecuteTxn(*bustub, _var, _txn, "DELETE FROM maintable WHERE a = 3"));
    WithTxn(txn_prepare_2, ExecuteTxn(*bustub, _var, _txn, "DELETE FROM maintable WHERE a = 5"));
    WithTxn(txn_prepare_2, CommitTxn(*bustub, _var, _txn));
    // auto txn_verify_2 = BeginTxn(*bustub, "txn_verify_2");

    fmt::println(stderr, "A: update and abort");
    auto txn3 = BeginTxn(*bustub, "txn3");
    WithTxn(txn3, ExecuteTxn(*bustub, _var, _txn, "UPDATE maintable SET b = 0"));
    WithTxn(txn3, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {2, 0},
                                      {4, 0},
                                  }));
    WithTxn(txn3, AbortTxn(*bustub, _var, _txn));
    TxnMgrDbg("after abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    fmt::println(stderr, "B: delete and abort");
    auto txn4 = BeginTxn(*bustub, "txn4");
    WithTxn(txn4, ExecuteTxn(*bustub, _var, _txn, "DELETE FROM maintable"));
    WithTxn(txn4, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable", IntResult{}));
    WithTxn(txn4, AbortTxn(*bustub, _var, _txn));
    TxnMgrDbg("after abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    fmt::println(stderr, "C: update and commit");
    auto txn5 = BeginTxn(*bustub, "txn5");
    WithTxn(txn5, ExecuteTxn(*bustub, _var, _txn, "INSERT INTO maintable VALUES (3, 0), (5, 0)"));
    WithTxn(txn5, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                  IntResult{
                                      {2, 233},
                                      {3, 0},
                                      {4, 23333},
                                      {5, 0},
                                  }));
    WithTxn(txn5, AbortTxn(*bustub, _var, _txn));
    TxnMgrDbg("after abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());

    WithTxn(txn_verify_1, QueryShowResult(*bustub, _var, _txn, "SELECT * FROM maintable",
                                          IntResult{
                                              {2, 2},
                                              {3, 2333},
                                          }));

    TableHeapEntryNoMoreThan(*bustub, table_info, 4);
    // test continues on Gradescope...
  }
}
// NOLINTEND(bugprone-unchecked-optional-access))

}  // namespace bustub
