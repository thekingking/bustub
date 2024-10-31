//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // create the header page
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  // get the hash value
  auto hash = Hash(key);
  // get the header page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  // get the directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  //! int32_t变成了uint32_t，可能会有问题
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();

  // get the bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  directory_guard.Drop();
  directory_page = nullptr;
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // search the bucket
  V res;
  if (bucket_page->Lookup(key, res, cmp_)) {
    result->push_back(res);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // get the header page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // get the directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }

  // header_page不再使用，释放
  header_page = nullptr;
  header_guard.Drop();

  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // get the bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }
  {
    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // insert into the bucket
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }
    //! key已经存在，value是否插入，即更新对应value
    V res;
    if (bucket_page->Lookup(key, res, cmp_)) {
      return false;
    }
  }

  // 分裂bucket，如果分裂后bucket依然无法插入，则继续分裂，直到directory满了
  while (SplitBucket(directory_page, bucket_idx)) {
    bucket_idx = directory_page->HashToBucketIndex(hash);
    auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  // create a new directory page
  page_id_t new_directory_page_id;
  BasicPageGuard new_directory_guard = bpm_->NewPageGuarded(&new_directory_page_id);
  auto new_directory_page = new_directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  new_directory_page->Init(directory_max_depth_);

  //! 考虑可能会插入失败
  // insert the new directory page into the header
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);

  // get bucket index
  auto bucket_idx = new_directory_page->HashToBucketIndex(hash);
  return InsertToNewBucket(new_directory_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  // create a new bucket page
  page_id_t new_bucket_page_id;
  BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // initialize the new bucket page
  new_bucket_page->Init(bucket_max_size_);

  // insert the new bucket page into the directory
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);

  // insert into the bucket
  return new_bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx)
    -> bool {
  // judge whether the bucket can be split
  auto local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == directory_max_depth_) {
    return false;
  }
  // split a new bucket
  auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);

  // get the old bucket page
  auto old_bucket_page_id = directory->GetBucketPageId(bucket_idx);
  WritePageGuard old_bucket_guard = bpm_->FetchPageWrite(old_bucket_page_id);
  auto old_bucket_page = old_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // create a new bucket page
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);

  // grow the directory if local_depth == global_depth
  if (local_depth == directory->GetGlobalDepth()) {
    directory->IncrGlobalDepth();
  }

  auto new_local_depth = local_depth + 1;
  // update local depth and bucket page id
  for (uint32_t i = 0; i < (1 << (directory_max_depth_ - new_local_depth)); ++i) {
    directory->SetLocalDepth(bucket_idx + (i << new_local_depth), new_local_depth);
    directory->SetLocalDepth(new_bucket_idx + (i << new_local_depth), new_local_depth);
    directory->SetBucketPageId(bucket_idx + (i << new_local_depth), old_bucket_page_id);
    directory->SetBucketPageId(new_bucket_idx + (i << new_local_depth), new_bucket_page_id);
  }

  // migrate the entries
  MigrateEntries(old_bucket_page, new_bucket_page, new_bucket_idx, directory->GetLocalDepthMask(bucket_idx));
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  // migrate the entries
  uint32_t i = 0;
  while (i < old_bucket->Size()) {
    auto key = old_bucket->KeyAt(i);
    auto value = old_bucket->ValueAt(i);
    if ((Hash(key) & local_depth_mask) == new_bucket_idx) {
      // insert into the new bucket
      new_bucket->Insert(key, value, cmp_);
      // remove from the old bucket
      old_bucket->RemoveAt(i);
    } else {
      i++;
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = Hash(key);
  // get the header page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // get the directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == static_cast<uint32_t>(INVALID_PAGE_ID)) {
    return false;
  }
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // header_page不再使用，释放
  header_page = nullptr;
  header_guard.Drop();

  // get the bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  {
    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // remove the key from the bucket
    if (!bucket_page->Remove(key, cmp_)) {
      return false;
    }
    if (!bucket_page->IsEmpty()) {
      return true;
    }
  }
  while (MergeBucket(directory_page, bucket_idx)) {
    while (directory_page->CanShrink()) {
      // shrink the directory
      directory_page->DecrGlobalDepth();
    }
  }
  //! bucket merge
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::MergeBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx)
    -> bool {
  // judge whether the bucket can be merged
  auto local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == 0) {
    return false;
  }

  auto new_local_depth = local_depth - 1;
  auto idx = bucket_idx & ((1 << new_local_depth) - 1);
  auto split_bucket_idx = bucket_idx ^ (1 << new_local_depth);

  for (uint32_t i = 0; i < (1 << (directory_max_depth_ - new_local_depth)); ++i) {
    if (directory->GetLocalDepth(idx + (i << new_local_depth)) != local_depth) {
      return false;
    }
  }

  // get the bucket page
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // get the split bucket page
  auto split_bucket_page_id = directory->GetBucketPageId(split_bucket_idx);
  WritePageGuard split_bucket_guard = bpm_->FetchPageWrite(split_bucket_page_id);
  auto split_bucket_page = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (bucket_page->IsEmpty()) {
    // merge the bucket
    for (uint32_t i = 0; i < (1 << (directory_max_depth_ - new_local_depth)); ++i) {
      directory->SetLocalDepth(idx + (i << new_local_depth), new_local_depth);
      directory->SetBucketPageId(idx + (i << new_local_depth), split_bucket_page_id);
    }
    return true;
  }
  if (split_bucket_page->IsEmpty()) {
    // merge the split bucket
    for (uint32_t i = 0; i < (1 << (directory_max_depth_ - new_local_depth)); ++i) {
      directory->SetLocalDepth(idx + (i << new_local_depth), new_local_depth);
      directory->SetBucketPageId(idx + (i << new_local_depth), bucket_page_id);
    }
    return true;
  }
  return false;
}

/*****************************************************************************
 * Explicit template instantiation
 *****************************************************************************/

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
