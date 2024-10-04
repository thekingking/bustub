#include "primer/trie.h"
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto node = root_;

  for (char ch : key) {
    // 如果get访问的元素不存在，则返回null
    if ((node == nullptr) || (node->children_.find(ch) == node->children_.end())) {
      return nullptr;
    }
    // 向下递归搜索
    node = node->children_.at(ch);
  }

  // 如果key对应节点存在value，则将这个节点的指针转换为TrieNodeWithValue并获取其值
  // node为nullptr，node不为TrieNodeWithValue，value类型不匹配都会转换失败，返回nullptr
  const auto *node_with_val = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (node_with_val != nullptr) {
    return node_with_val->value_.get();
  }
  // key不存在对应value
  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
void PutRec(std::shared_ptr<TrieNode> &new_root,std::string_view key, T val) {
  auto ch = key[0];
  // 如果key.size() == 1,则当前节点下一个节点为插入节点，在此进行插入操作
  if (key.size() == 1) {
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(val));
    // 如果子节点存在，则覆盖，否则创建新的子节点
    if (new_root->children_.find(ch) != new_root->children_.end()) {
      auto node = new_root->children_[ch];
      new_root->children_[ch] = std::make_shared<const TrieNodeWithValue<T>>(node->children_ ,std::move(val_p));
    } else {
      new_root->children_[ch] = std::make_shared<const TrieNodeWithValue<T>>(std::move(val_p));
    }
    return ;
  }
  // 如果key.size() > 1,则当前节点下一个节点不为插入节点，在此进行递归操作
  std::shared_ptr<TrieNode> node = nullptr;
  if (new_root->children_.find(ch) != new_root->children_.end()) {
    node = new_root->children_.at(ch)->Clone();
  } else {
    node = std::make_shared<TrieNode>();
  }
  PutRec(node, key.substr(1), std::move(val));
  new_root->children_[ch] = std::shared_ptr<const TrieNode>(node);
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  // 新根节点
  std::shared_ptr<TrieNode> new_root = nullptr;
  // key为空
  if (key.empty()) {
    // 创建值的shared_ptr
    std::shared_ptr<T> val = std::make_shared<T>(std::move(value));
    // 根节点有无子节点
    if (root_->children_.empty()) {
      new_root = std::make_shared<TrieNodeWithValue<T>>(std::move(val));
    } else {
      new_root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::move(val));
    }
    return Trie(std::move(new_root));
  }
  // 原本无根节点
  if (root_ == nullptr) {
    // 没有就创建新的根节点
    new_root = std::make_shared<TrieNode>();
  } else {
    // 有则将原根节点复制到新节点中
    new_root = root_->Clone();
  }
  PutRec<T>(new_root, key, std::move(value));
  return Trie(std::move(new_root));

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

bool RemoveCycle(const std::shared_ptr<TrieNode> &new_root, std::string_view key) {
  for (auto &pair : new_root->children_) {
    if (key.at(0) != pair.first) {
      continue;
    }
    if (key.size() == 1) {
      if (!pair.second->is_value_node_) {
        return false;
      }
      if (pair.second->children_.empty()) {
        // 子节点为空，直接删除
        new_root->children_.erase(pair.first);
      } else {
        // 子节点不为空，则创建空节点，并将原本节点的子节点转移到新节点上
        pair.second = std::make_shared<const TrieNode>(pair.second->children_);
      }
      return true;
    }
    std::shared_ptr<TrieNode> ptr = pair.second->Clone();
    bool flag = RemoveCycle(ptr, key.substr(1));
    // 没有删除节点，则直接退出
    if (!flag) {
      return false;
    }
    // 如果有删除的节点，且删除后子节点无value且为空，则删除当前节点
    if (ptr->children_.empty() && !ptr->is_value_node_) {
      new_root->children_.erase(pair.first);
    } else {
      // 否则用删除的子树覆盖原来的子树
      pair.second = std::shared_ptr<const TrieNode>(ptr);
    }
    return true;
  }
  return false;
}

void RemoveRec(const std::shared_ptr<TrieNode> &new_root, std::string_view key) {
  auto ch = key.at(0);
  // key.size() == 1，删除节点在当前节点上
  if (key.size() == 1) {
    // 若查找到节点，将节点子节点复制到新节点上
    if (new_root->children_.find(ch) != new_root->children_.end()) {
      auto node = new_root->children_.at(ch);
      if (node->children_.empty()) {
        new_root->children_.erase(ch);
      } else {
        new_root->children_[ch] = std::make_shared<const TrieNode>(node->children_);
      }
    }
    return ;
  }
  // 没查找到节点，直接返回
  if (new_root->children_.find(ch) == new_root->children_.end()) {
    return ;
  }
  // 查找到节点，递归删除
  std::shared_ptr<TrieNode> node = new_root->children_.at(ch)->Clone();
  RemoveRec(node, key.substr(1));
  if (node->children_.empty() && !node->is_value_node_) {
    new_root->children_.erase(ch);
  } else {
    new_root->children_[ch] = std::shared_ptr<const TrieNode>(node);
  }
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // 树为空树
  if (this->root_ == nullptr) {
    return *this;
  }

  // key为空，删除节点在根节点上
  if (key.empty()) {
    // 根节点有value，删除根节点的值
    if (root_->is_value_node_) {
      // 根节点无子节点
      if (root_->children_.empty()) {
        // 返回空树
        return Trie(nullptr);
      }
      // 将根节点的子节点转移到新的根节点上
      std::shared_ptr<TrieNode> new_root = std::make_shared<TrieNode>(root_->children_);
      return Trie(new_root);
    }
    // 根节点无value，直接返回
    return *this;
  }

  std::shared_ptr<TrieNode> new_root = root_->Clone();
  RemoveRec(new_root, key);
  if (new_root->children_.empty() && !new_root->is_value_node_) {
    new_root = nullptr;
  }
  return Trie(std::move(new_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
