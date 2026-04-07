#include "query/executor.h"
#include <algorithm>
#include <cctype>
#include <unordered_map>

namespace flexql {

static std::string normalize(const std::string &sql) {
    std::string r;
    bool last_space = false;
    for (char c : sql) {
        char u = toupper((unsigned char)c);
        if (isspace((unsigned char)c)) {
            if (!last_space) { r += ' '; last_space = true; }
        } else {
            r += u; last_space = false;
        }
    }
    return r;
}

Executor::Executor(Database &db) : db_(db) {}

QueryResult Executor::execute(const std::string &sql) {
    std::string norm = normalize(sql);

    /* Cache only SELECT queries */
    bool is_select = (norm.rfind("SELECT", 0) == 0);
    if (is_select) {
        QueryResult cached;
        if (db_.cache().get(norm, cached)) return cached;
    }

    ParsedQuery pq = Parser::parse(sql);
    if (!pq.error.empty()) {
        QueryResult r; r.ok = false; r.error = pq.error; return r;
    }

    QueryResult result;
    switch (pq.type) {
    case QueryType::CREATE_TABLE: result = exec_create(pq); break;
    case QueryType::DROP_TABLE:   result = exec_drop(pq);   break;
    case QueryType::DELETE_ROWS:  result = exec_delete(pq); break;
    case QueryType::INSERT:       result = exec_insert(pq); break;
    case QueryType::SELECT:       result = exec_select(pq); break;
    default:
        result.ok = false; result.error = "Unsupported query type"; break;
    }

    /* Only cache small result sets — large results cost more to
     * deserialize from cache than to re-scan from storage */
    if (is_select && result.ok && result.rows.size() <= 10000)
        db_.cache().put(norm, result);

    return result;
}

QueryResult Executor::exec_create(const ParsedQuery &q) {
    QueryResult r;
    TableSchema schema;
    schema.name    = q.table_name;
    schema.columns = q.col_defs;
    std::string err = db_.create_table(schema, q.if_not_exists);
    if (!err.empty()) { r.ok = false; r.error = err; return r; }
    db_.cache().invalidate(q.table_name);
    return r;
}

QueryResult Executor::exec_delete(const ParsedQuery &q) {
    QueryResult r;
    Table *t = db_.get_table(q.table_name);
    if (!t) { r.ok = false; r.error = "Table not found: " + q.table_name; return r; }
    uint64_t lsn = 0;
    if (!db_.wal_log_delete_all(q.table_name, lsn)) {
        r.ok = false;
        r.error = "WAL write failed for DELETE FROM " + q.table_name;
        return r;
    }
    t->delete_all();
    if (!db_.storage_mark_deleted(q.table_name)) {
        r.ok = false;
        r.error = "Disk write failed for DELETE FROM " + q.table_name;
        return r;
    }
    if (!db_.wal_mark_applied(lsn)) {
        r.ok = false;
        r.error = "WAL metadata update failed for DELETE FROM " + q.table_name;
        return r;
    }
    db_.cache().invalidate(q.table_name);
    return r;
}

QueryResult Executor::exec_drop(const ParsedQuery &q) {
    QueryResult r;
    std::string err = db_.drop_table(q.table_name);
    if (!err.empty()) { r.ok = false; r.error = err; return r; }
    // Invalidate all cached results that reference this table
    db_.cache().invalidate(q.table_name);
    return r;
}

QueryResult Executor::exec_insert(const ParsedQuery &q) {
    QueryResult r;
    Table *t = db_.get_table(q.table_name);
    if (!t) { r.ok = false; r.error = "Table not found: " + q.table_name; return r; }

    const auto &rows = q.multi_row_values;

    if (rows.size() > 1) {
        /* Validate, WAL-log, and append under one table write lock. */
        int inserted = 0;
        std::string err = t->batch_insert(
            rows,
            q.expiration,
            inserted,
            [this, &q](const std::vector<Row> &pending_rows, std::vector<uint64_t> &offsets) {
                uint64_t last_lsn = 0;
                if (!db_.wal_log_insert_batch(q.table_name, pending_rows, last_lsn))
                    return false;
                if (!db_.storage_append_batch(q.table_name, pending_rows, offsets))
                    return false;
                return db_.wal_mark_applied(last_lsn);
            });
        if (!err.empty()) { r.ok = false; r.error = err; return r; }

    } else {
        std::string err = t->insert(
            q.insert_values,
            q.expiration,
            [this, &q](const Row &row, uint64_t &offset) {
                uint64_t lsn = 0;
                if (!db_.wal_log_insert(q.table_name, row.values, row.expiration, lsn))
                    return false;
                if (!db_.storage_append_row(q.table_name, row, offset))
                    return false;
                return db_.wal_mark_applied(lsn);
            });
        if (!err.empty()) { r.ok = false; r.error = err; return r; }
    }

    db_.cache().invalidate(q.table_name);
    return r;
}

QueryResult Executor::exec_select(const ParsedQuery &q) {
    if (q.join.present) return exec_join(q);

    QueryResult r;
    Table *t = db_.get_table(q.from_table);
    if (!t) { r.ok = false; r.error = "Table not found: " + q.from_table; return r; }

    r.column_names = t->col_names(q.select_cols, q.select_star);

    /* Validate that every requested column actually exists */
    if (!q.select_star) {
        for (const auto &col : q.select_cols) {
            if (t->col_index(col) < 0) {
                r.ok = false;
                r.error = "Unknown column: " + col;
                return r;
            }
        }
    }

    if (q.where.present) {
        r.rows = t->scan_where(q.select_cols, q.select_star,
                               q.where.column, q.where.op, q.where.value);
    } else {
        r.rows = t->scan(q.select_cols, q.select_star);
    }
    return r;
}

QueryResult Executor::exec_join(const ParsedQuery &q) {
    QueryResult r;
    Table *ta = db_.get_table(q.from_table);
    Table *tb = db_.get_table(q.join.table_b);
    if (!ta) { r.ok = false; r.error = "Table not found: " + q.from_table;   return r; }
    if (!tb) { r.ok = false; r.error = "Table not found: " + q.join.table_b; return r; }

    /* Build combined column list: tableA.col, ..., tableB.col, ... */
    std::vector<std::string> all_col_names;
    for (auto &col : ta->schema().columns)
        all_col_names.push_back(q.from_table + "." + col.name);
    for (auto &col : tb->schema().columns)
        all_col_names.push_back(q.join.table_b + "." + col.name);

    /* Determine which columns to project */
    std::vector<int> proj_indices;
    if (q.select_star) {
        r.column_names = all_col_names;
        for (int i = 0; i < (int)all_col_names.size(); ++i)
            proj_indices.push_back(i);
    } else {
        /* Map each requested col to its index in the combined row */
        for (const auto &req : q.select_cols) {
            std::string req_u = req;
            for (auto &ch : req_u) ch = toupper((unsigned char)ch);
            int found = -1;
            for (int i = 0; i < (int)all_col_names.size(); ++i) {
                std::string cn = all_col_names[i];
                for (auto &ch : cn) ch = toupper((unsigned char)ch);
                /* Match either "TABLE.COL" or just "COL" */
                size_t dot = cn.find('.');
                std::string bare = (dot != std::string::npos) ? cn.substr(dot+1) : cn;
                if (cn == req_u || bare == req_u) { found = i; break; }
            }
            if (found < 0) {
                r.ok = false; r.error = "Unknown column in JOIN: " + req; return r;
            }
            proj_indices.push_back(found);
            r.column_names.push_back(req);
        }
    }

    /* Strip table prefix for column lookup */
    auto strip = [](const std::string &s) -> std::string {
        size_t dot = s.find('.');
        return dot == std::string::npos ? s : s.substr(dot + 1);
    };

    /* Find join column indices */
    auto find_col = [](const TableSchema &sc, const std::string &name) -> int {
        std::string nu = name;
        for (auto &c : nu) c = toupper((unsigned char)c);
        for (int i = 0; i < (int)sc.columns.size(); ++i) {
            std::string b = sc.columns[i].name;
            for (auto &c : b) c = toupper((unsigned char)c);
            if (nu == b) return i;
        }
        return -1;
    };

    int idx_a = find_col(ta->schema(), strip(q.join.col_a));
    int idx_b = find_col(tb->schema(), strip(q.join.col_b));
    if (idx_a < 0) { r.ok=false; r.error="Join column not found: "+q.join.col_a; return r; }
    if (idx_b < 0) { r.ok=false; r.error="Join column not found: "+q.join.col_b; return r; }

    /* Scan both tables */
    std::vector<Row> rows_a = ta->scan({}, true);
    std::vector<Row> rows_b = tb->scan({}, true);

    /* Hash join: build hash map on tableB join column */
    std::unordered_map<std::string, std::vector<size_t>> hash_b;
    hash_b.reserve(rows_b.size());
    for (size_t i = 0; i < rows_b.size(); ++i)
        if (idx_b < (int)rows_b[i].values.size())
            hash_b[rows_b[i].values[idx_b]].push_back(i);

    /* Probe */
    for (auto &ra : rows_a) {
        if (idx_a >= (int)ra.values.size()) continue;
        auto it = hash_b.find(ra.values[idx_a]);
        if (it == hash_b.end()) continue;

        for (size_t bi : it->second) {
            auto &rb = rows_b[bi];

            /* Apply WHERE if present */
            if (q.where.present) {
                std::string wcol = strip(q.where.column);
                /* Try tableA first, then tableB */
                int widx = find_col(ta->schema(), wcol);
                std::string wval;
                if (widx >= 0 && widx < (int)ra.values.size()) {
                    wval = ra.values[widx];
                } else {
                    widx = find_col(tb->schema(), wcol);
                    if (widx >= 0 && widx < (int)rb.values.size())
                        wval = rb.values[widx];
                    else widx = -1;
                }
                if (widx >= 0) {
                    auto &op  = q.where.op;
                    auto &exp = q.where.value;
                    bool match = false;
                    if (op=="=")  match = (wval==exp);
                    else if (op=="!=") match = (wval!=exp);
                    else if (op=="<")  match = (wval<exp);
                    else if (op==">")  match = (wval>exp);
                    else if (op=="<=") match = (wval<=exp);
                    else if (op==">=") match = (wval>=exp);
                    if (!match) continue;
                }
            }

            /* Build full combined row first, then project */
            std::vector<std::string> full;
            full.insert(full.end(), ra.values.begin(), ra.values.end());
            full.insert(full.end(), rb.values.begin(), rb.values.end());

            Row combined;
            for (int pi : proj_indices) {
                if (pi < (int)full.size())
                    combined.values.push_back(full[pi]);
                else
                    combined.values.push_back("NULL");
            }
            r.rows.push_back(std::move(combined));
        }
    }
    return r;
}

} // namespace flexql
