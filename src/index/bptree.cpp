#include "index/bptree.h"
#include <algorithm>
#include <stdexcept>

namespace flexql {

BPTree::BPTree() {
    root_ = new Node();
    root_->is_leaf = true;
}

BPTree::~BPTree() { destroy(root_); }

void BPTree::destroy(Node *n) {
    if (!n) return;
    if (!n->is_leaf)
        for (auto *c : n->children) destroy(c);
    delete n;
}

BPTree::Node *BPTree::find_leaf(const std::string &key) const {
    Node *cur = root_;
    while (!cur->is_leaf) {
        int i = (int)cur->keys.size() - 1;
        while (i >= 0 && key < cur->keys[i]) --i;
        cur = cur->children[i+1];
    }
    return cur;
}

bool BPTree::find(const std::string &key, size_t &row_idx) const {
    Node *leaf = find_leaf(key);
    for (size_t i = 0; i < leaf->keys.size(); ++i) {
        if (leaf->keys[i] == key) { row_idx = leaf->vals[i]; return true; }
    }
    return false;
}

void BPTree::insert_non_full(Node *node, const std::string &key, size_t val) {
    if (node->is_leaf) {
        auto it = std::lower_bound(node->keys.begin(), node->keys.end(), key);
        int idx = (int)(it - node->keys.begin());
        node->keys.insert(it, key);
        node->vals.insert(node->vals.begin() + idx, val);
    } else {
        int i = (int)node->keys.size() - 1;
        while (i >= 0 && key < node->keys[i]) --i;
        i++;
        Node *child = node->children[i];
        if ((int)child->keys.size() == ORDER - 1) {
            split_child(node, i, child);
            if (key >= node->keys[i]) i++;
        }
        insert_non_full(node->children[i], key, val);
    }
}

void BPTree::split_child(Node *parent, int idx, Node *child) {
    int mid = (ORDER - 1) / 2;
    Node *sibling = new Node();
    sibling->is_leaf = child->is_leaf;

    if (child->is_leaf) {
        // Copy right half to sibling
        sibling->keys.assign(child->keys.begin() + mid, child->keys.end());
        sibling->vals.assign(child->vals.begin() + mid, child->vals.end());
        child->keys.resize(mid);
        child->vals.resize(mid);
        sibling->next = child->next;
        child->next   = sibling;
        // Push up middle key
        parent->keys.insert(parent->keys.begin() + idx, sibling->keys[0]);
    } else {
        sibling->keys.assign(child->keys.begin() + mid + 1, child->keys.end());
        sibling->children.assign(child->children.begin() + mid + 1, child->children.end());
        std::string push_up = child->keys[mid];
        child->keys.resize(mid);
        child->children.resize(mid + 1);
        parent->keys.insert(parent->keys.begin() + idx, push_up);
    }
    parent->children.insert(parent->children.begin() + idx + 1, sibling);
}

void BPTree::insert(const std::string &key, size_t row_idx) {
    if ((int)root_->keys.size() == ORDER - 1) {
        Node *new_root = new Node();
        new_root->is_leaf = false;
        new_root->children.push_back(root_);
        split_child(new_root, 0, root_);
        root_ = new_root;
    }
    insert_non_full(root_, key, row_idx);
}

void BPTree::remove(const std::string &key) {
    // Simple lazy deletion: mark by setting val to SIZE_MAX
    Node *leaf = find_leaf(key);
    for (size_t i = 0; i < leaf->keys.size(); ++i) {
        if (leaf->keys[i] == key) {
            leaf->keys.erase(leaf->keys.begin() + i);
            leaf->vals.erase(leaf->vals.begin() + i);
            return;
        }
    }
}

void BPTree::all(std::vector<size_t> &out) const {
    // Traverse leaf linked list
    Node *cur = root_;
    while (!cur->is_leaf) cur = cur->children[0];
    while (cur) {
        for (size_t v : cur->vals) out.push_back(v);
        cur = cur->next;
    }
}

void BPTree::range(const std::string &lo, const std::string &hi,
                   std::vector<size_t> &out) const {
    Node *cur = find_leaf(lo);
    while (cur) {
        for (size_t i = 0; i < cur->keys.size(); ++i) {
            if (cur->keys[i] > hi) return;
            if (cur->keys[i] >= lo) out.push_back(cur->vals[i]);
        }
        cur = cur->next;
    }
}

} // namespace flexql
