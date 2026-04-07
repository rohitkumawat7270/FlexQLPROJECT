#include "storage/database.h"
#include "storage/wal.h"
#include "storage/storage_engine.h"
#include <iostream>
#include <algorithm>
#include <cctype>

namespace flexql {

std::string Database::to_upper(const std::string &s) {
    std::string r = s;
    for (auto &c : r) c = toupper((unsigned char)c);
    return r;
}

Database::Database() {}

std::string Database::create_table(const TableSchema &schema, bool if_not_exists) {
    std::unique_lock<std::shared_mutex> lk(mtx_);
    std::string key = to_upper(schema.name);
    if (tables_.count(key)) {
        if (if_not_exists) return ""; /* silently succeed */
        return "Table already exists: " + schema.name;
    }
    uint64_t lsn = 0;
    if (wal_ && !wal_->log_create_table(schema, lsn))
        return "WAL write failed while creating table: " + schema.name;
    if (storage_ && !storage_->write_schema(schema))
        return "Disk write failed while creating table: " + schema.name;
    tables_[key] = std::make_unique<Table>(schema, storage_);
    if (wal_ && !wal_->mark_applied(lsn))
        return "WAL metadata update failed while creating table: " + schema.name;
    return "";
}

std::string Database::drop_table(const std::string &name) {
    std::unique_lock<std::shared_mutex> lk(mtx_);
    std::string key = to_upper(name);
    if (!tables_.count(key)) return "Table does not exist: " + name;
    uint64_t lsn = 0;
    if (wal_ && !wal_->log_drop_table(name, lsn))
        return "WAL write failed while dropping table: " + name;
    if (storage_ && !storage_->delete_table_files(name))
        return "Disk delete failed while dropping table: " + name;
    tables_.erase(key);
    if (wal_ && !wal_->mark_applied(lsn))
        return "WAL metadata update failed while dropping table: " + name;
    return "";
}

Table *Database::get_table(const std::string &name) {
    std::shared_lock<std::shared_mutex> lk(mtx_);
    auto it = tables_.find(to_upper(name));
    if (it == tables_.end()) return nullptr;
    return it->second.get();
}

} // namespace flexql

namespace flexql {
std::vector<std::pair<TableSchema, std::vector<Row>>> Database::snapshot() {
    std::shared_lock<std::shared_mutex> lk(mtx_);
    std::vector<std::pair<TableSchema, std::vector<Row>>> result;
    for (auto &[key, tbl] : tables_)
        result.push_back({ tbl->schema(), tbl->snapshot() });
    return result;
}

void Database::load_from_disk() {
    if (!storage_) return;

    auto schemas = storage_->load_all_schemas();
    if (schemas.empty()) {
        std::cout << "[STORAGE] No existing tables found on disk.\n";
        return;
    }

    int table_count = 0, row_count = 0;
    for (const auto &schema : schemas) {
        std::string key = to_upper(schema.name);
        if (!tables_.count(key)) {
            tables_[key] = std::make_unique<Table>(schema, storage_);
        }
        Table *t = tables_[key].get();
        t->attach_storage(storage_);

        size_t live_rows = 0;
        storage_->load_table_metadata(
            schema,
            live_rows,
            [t](uint64_t offset, time_t exp, const std::string &pk_value) {
                t->load_index_entry(pk_value, offset, exp);
                return true;
            });
        row_count += (int)live_rows;
        table_count++;
        std::cout << "[STORAGE] Loaded table " << schema.name
                  << " (" << live_rows << " rows)\n";
    }
    std::cout << "[STORAGE] Startup complete: "
              << table_count << " tables, "
              << row_count   << " rows loaded from disk.\n";
}

} // namespace flexql
