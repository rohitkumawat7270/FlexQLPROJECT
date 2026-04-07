#pragma once
#include "storage/table.h"
#include "storage/wal.h"
#include "storage/storage_engine.h"
#include "cache/lru_cache.h"
#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace flexql {

class Database {
public:
    Database();

    // Create a new table; returns error string or "" on success
    std::string create_table(const TableSchema &schema, bool if_not_exists = false);

    // Drop an existing table; returns error string or "" on success
    std::string drop_table(const std::string &name);

    // Returns nullptr if not found
    Table *get_table(const std::string &name);

    LRUCache &cache() { return cache_; }

    /* Attach storage engine and WAL */
    void set_wal(WAL *wal)               { wal_     = wal;     }
    void set_storage(StorageEngine *se)  { storage_ = se;      }

    /* Load schemas and lightweight row metadata from disk on startup */
    void load_from_disk();

    /* Called by Executor after validation but before commit */
    bool wal_log_insert(const std::string &table,
                        const std::vector<std::string> &vals,
                        time_t exp,
                        uint64_t &lsn) {
        return wal_ ? wal_->log_insert(table, vals, exp, lsn) : (lsn = 0, true);
    }
    bool wal_log_insert_batch(const std::string &table,
                              const std::vector<Row> &rows,
                              uint64_t &last_lsn) {
        return wal_ ? wal_->log_insert_batch(table, rows, last_lsn) : (last_lsn = 0, true);
    }
    bool wal_log_delete_all(const std::string &table, uint64_t &lsn) {
        return wal_ ? wal_->log_delete_all(table, lsn) : (lsn = 0, true);
    }
    bool wal_mark_applied(uint64_t lsn) {
        return wal_ ? wal_->mark_applied(lsn) : true;
    }

    /* Called by Executor to persist rows to the .data file */
    bool storage_append_row(const std::string &table, const Row &row, uint64_t &offset) {
        return storage_ ? storage_->append_row(table, row, offset) : (offset = 0, true);
    }
    bool storage_mark_deleted(const std::string &table) {
        return storage_ ? storage_->mark_all_deleted(table) : true;
    }
    bool storage_append_batch(const std::string &table,
                              const std::vector<Row> &rows,
                              std::vector<uint64_t> &offsets) {
        return storage_ ? storage_->append_rows_batch(table, rows, offsets) : (offsets.clear(), true);
    }

    /* Return a full snapshot of all tables and their live rows */
    std::vector<std::pair<TableSchema, std::vector<Row>>> snapshot();

private:
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    mutable std::shared_mutex mtx_;
    LRUCache cache_{4096};
    WAL            *wal_     = nullptr;
    StorageEngine  *storage_ = nullptr;

    static std::string to_upper(const std::string &s);
};

} // namespace flexql
