#pragma once
/*
 * FlexQL Per-Table Storage Engine
 * ════════════════════════════════════════════════════════════════════
 *
 * Every table gets THREE files in data/tables/:
 *
 *   <TABLE_NAME>.schema  — schema definition (columns, types, constraints)
 *   <TABLE_NAME>.data    — row data, one row per fixed block
 *   <TABLE_NAME>.txt     — human-readable mirror of live rows
 *
 * ── Schema file format (.schema) ────────────────────────────────────
 * Plain text, human-readable:
 *
 *   FLEXQL_SCHEMA_V1
 *   TABLE_NAME <name>
 *   COLUMNS <n>
 *   <col_name> <type> <not_null:0|1> <primary_key:0|1>
 *   ... (n lines)
 *
 * ── Data file format (.data) ────────────────────────────────────────
 * Binary, row-major. Each row is a fixed-size header + variable data:
 *
 *   [4 bytes] magic  = 0xF1EC4044  ("FLEX" in hex)
 *   [1 byte]  flags  = bit0: deleted, bit1: has_expiry
 *   [8 bytes] expiry = unix timestamp (int64), 0 = no expiry
 *   [4 bytes] nvals  = number of column values
 *   for each val:
 *     [4 bytes] len   = byte length of value string
 *     [len bytes]     = raw UTF-8 string (NOT null-terminated)
 *
 * Deleted rows are marked with flags bit0=1 and skipped on load.
 * This means DELETE FROM is O(n) on disk but O(1) on the data file
 * (just rewrites the flag byte).
 *
 * ── Index file (.index) — future extension ──────────────────────────
 * Currently the B+-tree index is rebuilt in memory from the data file
 * on startup. A persisted index is a future optimisation.
 *
 * ── WAL (.wal) — crash safety ────────────────────────────────────────
 * The WAL is still used as a crash-recovery journal. On a clean
 * shutdown the WAL is truncated (data is already in .data files).
 * On an unclean restart the WAL is replayed to recover any rows that
 * were not flushed to the .data file.
 */
#include "common/types.h"
#include <functional>
#include <string>
#include <vector>
#include <cstdint>

namespace flexql {

/* Magic number written at the start of every row block */
static constexpr uint32_t ROW_MAGIC = 0xF1EC4044u;

/* Row flags */
static constexpr uint8_t FLAG_DELETED    = 0x01u;
static constexpr uint8_t FLAG_HAS_EXPIRY = 0x02u;

class StorageEngine {
public:
    using RowVisitor = std::function<bool(uint64_t, const Row &)>;
    using MetadataVisitor = std::function<bool(uint64_t, time_t, const std::string &)>;

    /* data_dir: path to directory where table files are stored */
    explicit StorageEngine(const std::string &data_dir);

    /* Write schema file for a new table */
    bool write_schema(const TableSchema &schema);

    /* Delete both files for a dropped table */
    bool delete_table_files(const std::string &table_name);

    /* Append one row to the data file */
    bool append_row(const std::string &table_name, const Row &row, uint64_t &offset);

    /* Append multiple rows in ONE file open/close — critical for batch INSERT.
     * Opens the .data file once, writes all rows sequentially, flushes once.
     * 5000-row batch = 1 open + 1 flush vs 5000 opens + 5000 flushes. */
    bool append_rows_batch(const std::string &table_name,
                           const std::vector<Row> &rows,
                           std::vector<uint64_t> &offsets);

    /* Mark ALL rows in a table's data file as deleted (DELETE FROM) */
    bool mark_all_deleted(const std::string &table_name);

    /* Load all table schemas from disk */
    std::vector<TableSchema> load_all_schemas();

    /* Load all live rows for a table from its .data file */
    std::vector<std::pair<std::vector<std::string>, time_t>>
        load_rows(const std::string &table_name);

    /* Read one row at a known file offset */
    bool read_row_at(const std::string &table_name,
                     uint64_t offset,
                     Row &row,
                     bool &is_deleted);

    /* Visit live rows sequentially without loading the full table */
    bool scan_live_rows(const std::string &table_name,
                        const RowVisitor &visitor);

    /* Build lightweight startup metadata without materializing row bodies */
    bool load_table_metadata(const TableSchema &schema,
                             size_t &live_rows,
                             const MetadataVisitor &visitor);

    /* Compact a table's data file: rewrite with only live rows */
    bool compact_table(const std::string &table_name);

    /* Rewrite the on-disk snapshot from the current in-memory database state. */
    bool checkpoint(const std::vector<std::pair<TableSchema, std::vector<Row>>> &snapshot);

private:
    std::string data_dir_;

    std::string schema_path(const std::string &name) const;
    std::string data_path(const std::string &name)   const;
    std::string txt_path(const std::string &name)    const;

    static std::string to_upper(const std::string &s);
};

} // namespace flexql
