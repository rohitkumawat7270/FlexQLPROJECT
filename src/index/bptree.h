#pragma once
#include <vector>
#include <string>
#include <functional>
#include <memory>

namespace flexql {

/*
 * Simple B+-tree for primary key indexing.
 * Keys are strings (we convert INT/DECIMAL to zero-padded strings for comparison).
 * Values are row indices (size_t) into the table's row vector.
 */
class BPTree {
public:
    static const int ORDER = 64; // max keys per node

    BPTree();
    ~BPTree();

    void   insert(const std::string &key, size_t row_idx);
    bool   find(const std::string &key, size_t &row_idx) const;
    void   remove(const std::string &key);
    void   range(const std::string &lo, const std::string &hi,
                 std::vector<size_t> &out) const;
    void   all(std::vector<size_t> &out) const;

private:
    struct Node {
        bool                     is_leaf = true;
        std::vector<std::string> keys;
        std::vector<size_t>      vals;       // only in leaf
        std::vector<Node*>       children;   // only in internal
        Node*                    next = nullptr; // leaf linked list
    };

    Node *root_;

    Node *find_leaf(const std::string &key) const;
    void  split_child(Node *parent, int idx, Node *child);
    void  insert_non_full(Node *node, const std::string &key, size_t val);
    void  destroy(Node *n);
};

} // namespace flexql
