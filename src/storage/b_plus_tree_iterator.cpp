#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/storage/page/b_plus_tree_leaf_page.h"

namespace onebase {

template class BPlusTreeIterator<int, RID, std::less<int>>;

// ---------------------------------------------------------------------------
// Constructor (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(page_id_t page_id, int index, BufferPoolManager *bpm)
    : page_id_(page_id), index_(index), bpm_(bpm) {}

// ---------------------------------------------------------------------------
// IsEnd (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::IsEnd() const -> bool {
  return page_id_ == INVALID_PAGE_ID;
}

// ---------------------------------------------------------------------------
// operator*
//
// The iterator header stores a BufferPoolManager* so we can fetch the current
// leaf page and read the key-value pair at index_.
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator*() -> const std::pair<KeyType, ValueType> & {
  auto *page = bpm_->FetchPage(page_id_);
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  current_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
  bpm_->UnpinPage(page_id_, false);
  return current_;
}

// ---------------------------------------------------------------------------
// operator++
//
// Advancing requires reading the current leaf page; when we pass the leaf's
// last slot we move to the next leaf through next_page_id_.
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator++() -> BPlusTreeIterator & {
  if (IsEnd()) {
    return *this;
  }

  auto *page = bpm_->FetchPage(page_id_);
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(page->GetData());
  index_++;
  if (index_ >= leaf->GetSize()) {
    page_id_ = leaf->GetNextPageId();
    index_ = 0;
  }
  bpm_->UnpinPage(page->GetPageId(), false);
  return *this;
}

// ---------------------------------------------------------------------------
// Comparison operators (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator==(const BPlusTreeIterator &other) const -> bool {
  return page_id_ == other.page_id_ && index_ == other.index_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator!=(const BPlusTreeIterator &other) const -> bool {
  return !(*this == other);
}

}  // namespace onebase
