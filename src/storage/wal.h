#pragma once
/*
 * Write-Ahead Log
 *
 * The WAL is the durable runtime journal. A mutation is acknowledged only
 * after its log record has been written and fsync'd. Table data files are
 * rewritten from an in-memory checkpoint during clean shutdown.
 */
#include "common/types.h"
#include <string>
#include <vector>
#include <cstdint>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>

namespace flexql {

class WAL {
public:
    explicit WAL(const std::string &path);
    ~WAL();

    bool log_create_table(const TableSchema &schema, uint64_t &out_lsn);
    bool log_drop_table(const std::string &name, uint64_t &out_lsn);
    bool log_insert(const std::string &table,
                    const std::vector<std::string> &values,
                    time_t expiration,
                    uint64_t &out_lsn);
    bool log_insert_batch(const std::string &table,
                          const std::vector<Row> &rows,
                          uint64_t &out_last_lsn);
    bool log_delete_all(const std::string &table, uint64_t &out_lsn);
    bool mark_applied(uint64_t lsn);

    /* Replay WAL into database on startup */
    static void replay(const std::string &path, class Database &db);

    /* Ensure everything currently in the file is on disk. */
    void flush_and_sync();
    void truncate();

private:
    std::string  path_;
    std::string  meta_path_;
    int          fd_   = -1;
    std::mutex   mtx_;
    uint64_t     next_lsn_    = 1;
    uint64_t     applied_lsn_ = 0;

    static std::string encode(const std::string &s);
    static std::string decode(const std::string &s);
    bool write_entry(const std::string &entry);
    bool persist_applied_lsn();
    static uint64_t load_applied_lsn(const std::string &meta_path);
    static uint64_t scan_max_lsn(const std::string &path);
};

} // namespace flexql
