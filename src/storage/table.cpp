#include "storage/table.h"
#include "storage/storage_engine.h"
#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <sstream>
#include <unordered_set>

namespace flexql {

Table::Table(const TableSchema &schema, StorageEngine *storage)
    : schema_(schema), storage_(storage) {
    for (int i = 0; i < (int)schema_.columns.size(); ++i) {
        if (schema_.columns[i].primary_key) {
            schema_.pk_index = i;
            return;
        }
    }
}

bool Table::is_expired(time_t expiration) const {
    if (expiration == 0) return false;
    return time(nullptr) > expiration;
}

int Table::col_index(const std::string &name) const {
    std::string n = name;
    size_t dot = n.find('.');
    if (dot != std::string::npos) n = n.substr(dot + 1);

    for (int i = 0; i < (int)schema_.columns.size(); ++i) {
        std::string a = n, b = schema_.columns[i].name;
        for (auto &c : a) c = toupper((unsigned char)c);
        for (auto &c : b) c = toupper((unsigned char)c);
        if (a == b) return i;
    }
    return -1;
}

static bool is_valid_int(const std::string &s) {
    if (s.empty()) return false;
    size_t start = 0;
    if (s[0] == '-' || s[0] == '+') start = 1;
    if (start >= s.size()) return false;
    for (size_t i = start; i < s.size(); ++i)
        if (!isdigit((unsigned char)s[i])) return false;
    return true;
}

static bool is_valid_decimal(const std::string &s) {
    if (s.empty()) return false;
    bool dot_seen = false;
    size_t start = 0;
    if (s[0] == '-' || s[0] == '+') start = 1;
    for (size_t i = start; i < s.size(); ++i) {
        if (s[i] == '.') {
            if (dot_seen) return false;
            dot_seen = true;
        } else if (!isdigit((unsigned char)s[i])) return false;
    }
    return true;
}

std::string Table::normalize_and_validate(const std::vector<std::string> &values,
                                          time_t expiration,
                                          Row &out_row) const {
    size_t expected = schema_.columns.size();
    time_t exp = expiration;
    std::vector<std::string> vals = values;

    if (vals.size() == expected + 1) {
        const std::string &last = vals.back();
        bool all_digits = !last.empty();
        for (char c : last)
            if (!isdigit((unsigned char)c) && c != '-') { all_digits = false; break; }
        if (all_digits) {
            try { exp = (time_t)std::stoll(last); } catch (...) {}
            vals.pop_back();
        }
    }

    if (vals.size() != expected) {
        return "Column count mismatch: expected " + std::to_string(expected) +
               ", got " + std::to_string(vals.size());
    }

    for (int i = 0; i < (int)schema_.columns.size(); ++i) {
        const auto &cd = schema_.columns[i];
        const std::string &v = vals[i];

        if (cd.not_null && v.empty())
            return "NOT NULL constraint violated for column '" + cd.name + "'";

        if (!v.empty()) {
            if (cd.type == DataType::INT && !is_valid_int(v))
                return "Type error: '" + v + "' is not a valid INT for column '" + cd.name + "'";
            if (cd.type == DataType::DECIMAL && !is_valid_decimal(v))
                return "Type error: '" + v + "' is not a valid DECIMAL for column '" + cd.name + "'";
        }
    }

    out_row.values = std::move(vals);
    out_row.expiration = exp;
    return "";
}

std::string Table::insert(const std::vector<std::string> &values,
                          time_t expiration,
                          const std::function<bool(const Row&, uint64_t&)> &commit_hook) {
    std::unique_lock<std::shared_mutex> lk(rwmtx_);

    Row row;
    std::string err = normalize_and_validate(values, expiration, row);
    if (!err.empty()) return err;

    if (schema_.pk_index >= 0) {
        size_t dummy;
        const std::string &pk = row.values[schema_.pk_index];
        if (index_.find(pk, dummy))
            return "Duplicate primary key: " + pk;
    }

    uint64_t offset = 0;
    if (commit_hook && !commit_hook(row, offset))
        return "Commit persistence step failed";

    if (schema_.pk_index >= 0)
        index_.insert(row.values[schema_.pk_index], offset);
    live_row_count_++;
    return "";
}

bool Table::compare_values(const std::string &a, const std::string &op,
                           const std::string &b, DataType dtype) {
    if (dtype == DataType::INT || dtype == DataType::DECIMAL) {
        double da = a.empty() ? 0 : std::stod(a);
        double db = b.empty() ? 0 : std::stod(b);
        if (op == "=")  return da == db;
        if (op == "!=") return da != db;
        if (op == "<")  return da <  db;
        if (op == ">")  return da >  db;
        if (op == "<=") return da <= db;
        if (op == ">=") return da >= db;
    }
    if (op == "=")  return a == b;
    if (op == "!=") return a != b;
    if (op == "<")  return a <  b;
    if (op == ">")  return a >  b;
    if (op == "<=") return a <= b;
    if (op == ">=") return a >= b;
    return false;
}

bool Table::matches_where(const Row &row, int col_idx,
                          const std::string &op,
                          const std::string &val) const {
    if (col_idx < 0 || col_idx >= (int)row.values.size()) return false;
    return compare_values(row.values[col_idx], op, val,
                          schema_.columns[col_idx].type);
}

Row Table::project(const Row &row,
                   const std::vector<int> &col_indices) const {
    Row r;
    r.expiration = row.expiration;
    for (int ci : col_indices) {
        if (ci >= 0 && ci < (int)row.values.size())
            r.values.push_back(row.values[ci]);
        else
            r.values.push_back("NULL");
    }
    return r;
}

std::vector<std::string>
Table::col_names(const std::vector<std::string> &select_cols,
                 bool select_star) const {
    if (select_star) {
        std::vector<std::string> names;
        for (auto &c : schema_.columns) names.push_back(c.name);
        return names;
    }
    return select_cols;
}

std::vector<int> Table::build_indices(const std::vector<std::string> &select_cols,
                                      bool select_star) const {
    std::vector<int> indices;
    if (select_star) {
        for (int i = 0; i < (int)schema_.columns.size(); ++i) indices.push_back(i);
    } else {
        for (auto &c : select_cols) indices.push_back(col_index(c));
    }
    return indices;
}

std::vector<Row> Table::scan(const std::vector<std::string> &select_cols,
                             bool select_star) const {
    std::shared_lock<std::shared_mutex> lk(rwmtx_);
    auto indices = build_indices(select_cols, select_star);
    std::vector<Row> result;
    if (!storage_) return result;

    storage_->scan_live_rows(schema_.name, [&](uint64_t, const Row &row) {
        if (!is_expired(row.expiration))
            result.push_back(project(row, indices));
        return true;
    });
    return result;
}

std::vector<Row> Table::scan_where(const std::vector<std::string> &select_cols,
                                   bool select_star,
                                   const std::string &col,
                                   const std::string &op,
                                   const std::string &val) const {
    std::shared_lock<std::shared_mutex> lk(rwmtx_);
    int where_col = col_index(col);
    auto indices  = build_indices(select_cols, select_star);
    std::vector<Row> result;

    if (where_col == schema_.pk_index && schema_.pk_index >= 0 && op == "=" && storage_) {
        size_t row_offset = 0;
        if (index_.find(val, row_offset)) {
            Row row;
            bool deleted = false;
            if (storage_->read_row_at(schema_.name, row_offset, row, deleted) &&
                !deleted && !is_expired(row.expiration)) {
                result.push_back(project(row, indices));
            }
        }
        return result;
    }

    if (!storage_) return result;
    storage_->scan_live_rows(schema_.name, [&](uint64_t, const Row &row) {
        if (!is_expired(row.expiration) && matches_where(row, where_col, op, val))
            result.push_back(project(row, indices));
        return true;
    });
    return result;
}

std::string Table::batch_insert(const std::vector<std::vector<std::string>> &rows,
                                time_t expiration,
                                int &out_inserted,
                                const std::function<bool(const std::vector<Row>&, std::vector<uint64_t>&)> &commit_hook) {
    std::unique_lock<std::shared_mutex> lk(rwmtx_);
    out_inserted = 0;
    std::vector<Row> pending;
    pending.reserve(rows.size());
    std::unordered_set<std::string> batch_keys;

    for (const auto &values : rows) {
        Row row;
        std::string err = normalize_and_validate(values, expiration, row);
        if (!err.empty()) return err;

        if (schema_.pk_index >= 0) {
            size_t dummy;
            const std::string &pk = row.values[schema_.pk_index];
            if (index_.find(pk, dummy) || batch_keys.count(pk))
                return "Duplicate primary key: " + pk;
            batch_keys.insert(pk);
        }
        pending.push_back(std::move(row));
    }

    std::vector<uint64_t> offsets;
    if (commit_hook && !commit_hook(pending, offsets))
        return "Commit persistence step failed";
    if (!pending.empty() && offsets.size() != pending.size())
        return "Commit persistence step returned mismatched row offsets";

    for (size_t i = 0; i < pending.size(); ++i) {
        auto &row = pending[i];
        if (schema_.pk_index >= 0)
            index_.insert(row.values[schema_.pk_index], offsets[i]);
        out_inserted++;
        live_row_count_++;
    }
    return "";
}

void Table::insert_raw(const std::vector<std::string> &values,
                       time_t expiration,
                       uint64_t offset) {
    std::unique_lock<std::shared_mutex> lk(rwmtx_);
    if (schema_.pk_index >= 0 && schema_.pk_index < (int)values.size())
        index_.insert(values[schema_.pk_index], offset);
    if (!is_expired(expiration))
        live_row_count_++;
}

void Table::load_index_entry(const std::string &pk_value,
                             uint64_t offset,
                             time_t expiration) {
    std::unique_lock<std::shared_mutex> lk(rwmtx_);
    if (is_expired(expiration)) return;
    if (schema_.pk_index >= 0 && !pk_value.empty())
        index_.insert(pk_value, offset);
    live_row_count_++;
}

std::vector<Row> Table::snapshot() const {
    std::vector<Row> result;
    std::shared_lock<std::shared_mutex> lk(rwmtx_);
    if (!storage_) return result;

    storage_->scan_live_rows(schema_.name, [&](uint64_t, const Row &row) {
        if (!is_expired(row.expiration))
            result.push_back(row);
        return true;
    });
    return result;
}

void Table::delete_all() {
    std::unique_lock<std::shared_mutex> lk(rwmtx_);
    index_ = BPTree();
    live_row_count_ = 0;
}

size_t Table::row_count() const {
    std::shared_lock<std::shared_mutex> lk(rwmtx_);
    return live_row_count_;
}

} // namespace flexql
