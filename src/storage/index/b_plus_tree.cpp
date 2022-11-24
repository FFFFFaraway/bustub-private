#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->RLatch();
  auto res = root_page_id_ == INVALID_PAGE_ID;
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, false);
  return res;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  std::vector<Page *> _;
  auto leaf_page = FindLeafPage(key, _, false);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page);
  ValueType tmp;
  auto find = leaf->Lookup(key, &tmp, comparator_);
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  result->push_back(std::move(tmp));
  return find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // MakeNonEmpty
  auto header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->RLatch();
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value);
  }
  header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
  return InsertIntoLeaf(key, value, transaction);
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto root_page = buffer_pool_manager_->NewPage(&root_page_id_);
  BUSTUB_ASSERT(root_page, "out of memory");
  UpdateRootPageId();
  reinterpret_cast<LeafPage *>(root_page)->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::vector<Page *> locked;
  auto leaf_page = FindLeafPage(key, locked, false, true);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page);

  // leaf if safe then release all locks except leaf
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    for (size_t i = 0; i < locked.size() - 1; i++) {
      locked[i]->WUnlatch();
      buffer_pool_manager_->UnpinPage(locked[i]->GetPageId(), true);
    }
  }

  ValueType tmp;
  if (leaf->Lookup(key, &tmp, comparator_)) {
    // don't forget to unlatch all, when it is unsafe, but we don't have to do insertion
    if (leaf->GetSize() == leaf->GetMaxSize()) {
      for (size_t i = 0; i < locked.size() - 1; i++) {
        locked[i]->WUnlatch();
        buffer_pool_manager_->UnpinPage(locked[i]->GetPageId(), true);
      }
    }
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return false;
  }
  if (leaf->GetSize() < leaf->GetMaxSize()) {
    leaf->Insert(key, value, comparator_);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return true;
  }
  // now leaf is full
  auto new_page = Split(leaf);
  auto mid_key = new_page->KeyAt(0);
  if (comparator_(key, mid_key) > 0) {
    new_page->Insert(key, value, comparator_);
  } else {
    leaf->Insert(key, value, comparator_);
  }
  InsertIntoParent(leaf, mid_key, new_page, transaction);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);

  for (auto &page : locked) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(LeafPage *node) -> LeafPage * {
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_page_id));
  BUSTUB_ASSERT(new_page, "out of memory");
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  new_page->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  node->MoveHalfTo(new_page);
  return new_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(InternalPage *node, const page_id_t page_id) -> InternalPage * {
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id));
  BUSTUB_ASSERT(new_page, "out of memory");
  new_page->Init(new_page_id, node->GetParentPageId(), node->GetMaxSize());
  new_page->IncreaseSize(1);
  new_page->SetValueAt(0, page_id);
  node->MoveHalfTo(new_page, buffer_pool_manager_);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id));
    BUSTUB_ASSERT(new_page, "out of memory");
    new_page->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);
    root_page_id_ = new_page_id;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  auto parent_page_id = old_node->GetParentPageId();
  new_node->SetParentPageId(parent_page_id);
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));

  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  auto mid = parent_page->InsertAndGetMid(key, new_node->GetPageId(), comparator_);
  auto new_page = Split(parent_page, mid.second);
  InsertIntoParent(parent_page, mid.first, new_page, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  std::vector<Page *> locked;
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, locked, false, true));
  auto is_safe = leaf->IsRootPage() || leaf->GetSize() >= leaf->GetMinSize() + 1;
  if (is_safe) {
    for (size_t i = 0; i < locked.size() - 1; i++) {
      locked[i]->WUnlatch();
      buffer_pool_manager_->UnpinPage(locked[i]->GetPageId(), true);
    }
    leaf->RemoveAndDeleteRecord(key, comparator_);
    reinterpret_cast<Page *>(leaf)->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    return;
  }
  leaf->RemoveAndDeleteRecord(key, comparator_);
  std::vector<Page *> need_delete;
  CoalesceOrRedistribute(leaf, need_delete, transaction);
  for (auto &page : locked) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  for (auto &page : need_delete) {
    BUSTUB_ASSERT(buffer_pool_manager_->DeletePage(page->GetPageId()), "can't delete page");
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, std::vector<Page *> &need_delete, Transaction *transaction)
    -> bool {
  auto parent_page_id = node->GetParentPageId();
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id));
  auto idx = parent_page->ValueIndex(node->GetPageId());
  // use left as sibling
  auto sibling_idx = idx - 1;
  // no left, then use right
  if (idx == 0) {
    sibling_idx = 1;
  }
  auto sibling_is_left = idx != 0;
  auto sibling_page_id = parent_page->ValueAt(sibling_idx);
  auto sibling_page = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_page_id));
  // redistribute
  if (sibling_page->GetSize() + node->GetSize() > sibling_page->GetMaxSize()) {
    Redistribute(node, sibling_page, parent_page, idx, sibling_idx, sibling_is_left);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return false;
  }
  // merge
  Coalesce(node, sibling_page, parent_page, idx, sibling_idx, sibling_is_left);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page_id, true);

  if (sibling_is_left) {
    need_delete.push_back(reinterpret_cast<Page *>(node));
  } else {
    need_delete.push_back(reinterpret_cast<Page *>(sibling_page));
  }

  if (parent_page->IsRootPage()) {
    if (parent_page->GetSize() > 1) {
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      return false;
    }
    // root has only one child, set this child as new root
    auto child = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(0)));
    child->SetParentPageId(parent_page->GetParentPageId());
    root_page_id_ = child->GetPageId();
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return false;
  }
  if (parent_page->GetSize() <= parent_page->GetMinSize()) {
    CoalesceOrRedistribute(parent_page, need_delete, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(LeafPage *node, LeafPage *sibling_page, InternalPage *parent_page, int idx,
                              int sibling_idx, bool sibling_is_left) {
  if (sibling_is_left) {
    node->MoveAllToEndOf(sibling_page);
    parent_page->Remove(idx);
    sibling_page->SetNextPageId(node->GetNextPageId());
  } else {
    sibling_page->MoveAllToEndOf(node);
    parent_page->Remove(sibling_idx);
    node->SetNextPageId(sibling_page->GetNextPageId());
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Coalesce(InternalPage *node, InternalPage *sibling_page, InternalPage *parent_page, int idx,
                              int sibling_idx, bool sibling_is_left) {
  if (sibling_is_left) {
    auto split_entry = parent_page->ItemAt(idx);
    sibling_page->PushBack(std::make_pair(split_entry.first, node->GetX()));

    auto trans_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetX()));
    trans_page->SetParentPageId(sibling_page->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetX(), true);

    node->MoveAllTo(sibling_page, buffer_pool_manager_);
    parent_page->Remove(idx);
  } else {
    auto split_entry = parent_page->ItemAt(sibling_idx);
    node->PushBack(std::make_pair(split_entry.first, sibling_page->GetX()));

    auto trans_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(sibling_page->GetX()));
    trans_page->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_page->GetX(), true);

    sibling_page->MoveAllTo(node, buffer_pool_manager_);
    parent_page->Remove(sibling_idx);
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(LeafPage *node, LeafPage *sibling_page, InternalPage *parent_page, int idx,
                                  int sibling_idx, bool sibling_is_left) {
  if (sibling_is_left) {
    sibling_page->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(idx, node->KeyAt(0));
  } else {
    sibling_page->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(sibling_idx, sibling_page->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Redistribute(InternalPage *node, InternalPage *sibling_page, InternalPage *parent_page, int idx,
                                  int sibling_idx, bool sibling_is_left) {
  if (sibling_is_left) {
    auto split_entry = parent_page->ItemAt(idx);
    node->PushFront(std::make_pair(split_entry.first, node->GetX()));
    auto sibling_back = sibling_page->PopBack();
    node->SetX(sibling_back.second);

    auto trans_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(sibling_back.second));
    trans_page->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_back.second, true);

    parent_page->SetKeyAt(idx, sibling_back.first);
    parent_page->SetValueAt(idx, node->GetPageId());
  } else {
    auto split_entry = parent_page->ItemAt(sibling_idx);
    node->PushBack(std::make_pair(split_entry.first, sibling_page->GetX()));

    auto trans_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(sibling_page->GetX()));
    trans_page->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(sibling_page->GetX(), true);

    auto sibling_front = sibling_page->PopFront();
    sibling_page->SetX(sibling_front.second);

    parent_page->SetKeyAt(sibling_idx, sibling_front.first);
    parent_page->SetValueAt(sibling_idx, sibling_page->GetPageId());
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  KeyType k;
  std::vector<Page *> _;
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(k, _, true));
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf->GetPageId());
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::vector<Page *> _;
  auto leaf = reinterpret_cast<LeafPage *>(FindLeafPage(key, _, false));
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf->GetPageId(), leaf->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, std::vector<Page *> &locked, bool leftMost, bool isWrite)
    -> Page * {
  // start from HEADER_PAGE_ID to protect the root id
  auto header_page = buffer_pool_manager_->FetchPage(HEADER_PAGE_ID);
  if (isWrite) {
    header_page->WLatch();
    locked.push_back(header_page);
  } else {
    header_page->RLatch();
  }
  auto cur = buffer_pool_manager_->FetchPage(root_page_id_);
  if (isWrite) {
    cur->WLatch();
    locked.push_back(cur);
  } else {
    cur->RLatch();
    header_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(header_page->GetPageId(), false);
  }
  while (!reinterpret_cast<BPlusTreePage *>(cur)->IsLeafPage()) {
    page_id_t next_page_id;
    if (leftMost) {
      next_page_id = reinterpret_cast<InternalPage *>(cur)->ValueAt(0);
    } else {
      next_page_id = reinterpret_cast<InternalPage *>(cur)->Lookup(key, comparator_);
    }
    auto next = buffer_pool_manager_->FetchPage(next_page_id);
    if (isWrite) {
      next->WLatch();
      locked.push_back(next);
    } else {
      next->RLatch();
      cur->RUnlatch();
      buffer_pool_manager_->UnpinPage(cur->GetPageId(), false);
    }
    cur = next;
  }
  return cur;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
