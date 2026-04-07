#pragma once
#include "common/types.h"
#include "index/bptree.h"
#include <functional>
#include <shared_mutex>
#include <vector>

namespace flexql {

class StorageEngine;

class Table {
public:
    explicit Table(const TableSchema &schema, StorageEngine *storage = nullptr);

    /* Insert a row. Returns "" on success, error message on failure. */
    std::string insert(const std::vector<std::string> &values,
                       time_t expiration = 0,
                       const std::function<bool(const Row&, uint64_t&)> &commit_hook = {});

    /* batch_insert: insert multiple rows under ONE mutex acquisition.
     * Returns "" on full success, or error string on first failure.
     * out_inserted is set to the number of rows successfully inserted. */
    std::string batch_insert(const std::vector<std::vector<std::string>> &rows,
                             time_t expiration,
                             int &out_inserted,
                             const std::function<bool(const std::vector<Row>&, std::vector<uint64_t>&)> &commit_hook = {});

    /* insert_raw: bypass duplicate-PK check — used only during WAL replay.
     * During replay we trust the WAL is already consistent. */
    void insert_raw(const std::vector<std::string> &values,
                    time_t expiration = 0,
                    uint64_t offset = 0);

    /* Load startup metadata without materializing the full row in memory. */
    void load_index_entry(const std::string &pk_value,
                          uint64_t offset,
                          time_t expiration);

    void attach_storage(StorageEngine *storage) { storage_ = storage; }

    /* snapshot: return all live (non-expired, non-deleted) rows */
    std::vector<Row> snapshot() const;

    /* Full scan */
    std::vector<Row> scan(const std::vector<std::string> &select_cols,
                          bool select_star) const;

    /* Scan with WHERE filter (uses B+-tree for PK equality) */
    std::vector<Row> scan_where(const std::vector<std::string> &select_cols,
                                bool select_star,
                                const std::string &col,
                                const std::string &op,
                                const std::string &val) const;

    /* Returns the resolved column-name list for a projection */
    std::vector<std::string> col_names(const std::vector<std::string> &select_cols,
                                       bool select_star) const;

    const TableSchema &schema() const { return schema_; }
    size_t row_count() const;
    void   delete_all();  /* DELETE FROM — mark all rows deleted */
    int    col_index(const std::string &name) const;  /* public for executor */

private:
    TableSchema          schema_;
    BPTree               index_;
    StorageEngine       *storage_ = nullptr;
    size_t               live_row_count_ = 0;
    mutable std::shared_mutex rwmtx_;

    bool              is_expired(time_t expiration) const;
    bool              matches_where(const Row &row, int col_idx,
                                    const std::string &op,
                                    const std::string &val) const;
    Row               project(const Row &row,
                               const std::vector<int> &col_indices) const;
    std::vector<int>  build_indices(const std::vector<std::string> &select_cols,
                                    bool select_star) const;
    std::string       normalize_and_validate(const std::vector<std::string> &values,
                                             time_t expiration,
                                             Row &out_row) const;

    static bool compare_values(const std::string &a, const std::string &op,
                                const std::string &b, DataType dtype);
};

} // namespace flexql
