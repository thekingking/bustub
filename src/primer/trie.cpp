#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto node = root_;

  for (char ch : key) {
    if ((node == nullptr) || (node->children_.find(ch) == node->children_.end())) {
        return nullptr;
      }

    node = node->children_.at(ch);
  }

  const auto *node_with_val = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  if (node_with_val != nullptr) {
    return node_with_val->value_.get();
  }

  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
void PutCycle(const std::shared_ptr<bustub::TrieNode> &new_root, std::string_view key, T value) {
  bool flag = false;

  for (auto &pair : new_root->children_) {
    if (key.at(0) == pair.first) {
      flag = true;
      // 剩余键长大于1
      if (key.size() > 1) {
        // 复制当前子节点，对其递归写入
        std::shared_ptr<TrieNode> ptr = pair.second->Clone();
        PutCycle(ptr, key.substr(1), std::move(value));
        // 覆盖原本节点
        pair.second = std::shared_ptr<const TrieNode>(ptr);
      } else {
        // 当前节点为值插入位置，创建新的带value的子节点
        std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
        TrieNodeWithValue<T> node_with_val(pair.second->children_, val_p);
        // 覆盖原本的子节点
        pair.second = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
      }
      return;
    }
  }

  // 在原树中没找到
  if (!flag) {
    char c = key.at(0);
    if (key.size() == 1) {
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
      new_root->children_.insert({c, std::make_shared<const TrieNodeWithValue<T>>(val_p)});
    } else {
      // 创建一个空的children
      auto ptr = std::make_shared<TrieNode>();
      PutCycle(ptr, key.substr(1), std::move(value));
      new_root->children_.insert({c, std::move(ptr)});
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // 第一种情况：key为空，值放在根节点中
  if (key.empty()) {
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
    // 创建新根
    std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    // 原根节点有无子节点
    if (root_->children_.empty()) {
      // 无子节点，创建新的带值根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
    } else {
      // 有子节点，将原根关系转移给新根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }
    // 返回新的根节点
    return Trie(std::move(new_root));
  }

  // 第二种情况：key不为空，值不存放在根节点中
  std::shared_ptr<TrieNode> new_root = nullptr;
  // 原树中有无根节点
  if (root_ == nullptr) {
    // 没有就创建新的根节点
    new_root = std::make_unique<TrieNode>();
  } else {
    // 有则将原根节点复制到新节点中
    new_root = root_->Clone();
  }

  PutCycle<T>(new_root, key, std::move(value));
  return Trie(std::move(new_root));

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

bool RemoveCycle(const std::shared_ptr<TrieNode> &new_root, std::string_view key) {
  for (auto &pair : new_root->children_) {
    if (key.at(0) != pair.first) { continue; }
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

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // 树为空树
  if (this->root_ == nullptr) {
    return *this;
  }

  // key为空
  if (key.empty()) {
    // 根节点有value
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
    // 根节点无value
    return *this;
  }

  std::shared_ptr<TrieNode> new_root = root_->Clone();
  bool flag = RemoveCycle(new_root, key);
  // 没有删除节点
  if (!flag) {
    return *this;
  }
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
