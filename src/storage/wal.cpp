#include "storage/wal.h"
#include "storage/database.h"
#include <cerrno>
#include <cstdio>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>

namespace flexql {

std::string WAL::encode(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        if (c == '%' || c == '\n' || c == '\r' || c == '\0') {
            char buf[4];
            snprintf(buf, sizeof(buf), "%%%02X", c);
            out += buf;
        } else {
            out += (char)c;
        }
    }
    return out;
}

std::string WAL::decode(const std::string &s) {
    std::string out;
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '%' && i + 2 < s.size() &&
            isxdigit((unsigned char)s[i + 1]) &&
            isxdigit((unsigned char)s[i + 2])) {
            char hex[3] = { s[i + 1], s[i + 2], 0 };
            out += (char)strtol(hex, nullptr, 16);
            i += 2;
        } else {
            out += s[i];
        }
    }
    return out;
}

uint64_t WAL::load_applied_lsn(const std::string &meta_path) {
    std::ifstream in(meta_path);
    uint64_t lsn = 0;
    if (in) in >> lsn;
    return lsn;
}

uint64_t WAL::scan_max_lsn(const std::string &path) {
    std::ifstream f(path);
    if (!f.is_open()) return 0;

    uint64_t max_lsn = 0;
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        std::istringstream ss(line);
        uint64_t lsn = 0;
        std::string op;
        ss >> lsn >> op;
        if (lsn > max_lsn) max_lsn = lsn;

        if (op == "CREATE_TABLE") {
            std::string enc_name;
            size_t ncols = 0;
            ss >> enc_name >> ncols;
            for (size_t i = 0; i < ncols; ++i) {
                if (!std::getline(f, line)) break;
            }
        } else if (op == "INSERT") {
            std::string enc_table;
            long long expiry = 0;
            size_t nvals = 0;
            ss >> enc_table >> expiry >> nvals;
            for (size_t i = 0; i < nvals; ++i) {
                if (!std::getline(f, line)) break;
            }
        }
    }
    return max_lsn;
}

WAL::WAL(const std::string &path)
    : path_(path), meta_path_(path + ".meta")
{
    applied_lsn_ = load_applied_lsn(meta_path_);
    next_lsn_    = std::max(applied_lsn_, scan_max_lsn(path_)) + 1;
    fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0)
        std::cerr << "[WAL] WARNING: cannot open: " << path << "\n";
}

WAL::~WAL() {
    flush_and_sync();
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool WAL::write_entry(const std::string &entry) {
    if (fd_ < 0) return false;
    const char *ptr = entry.data();
    size_t rem = entry.size();
    while (rem > 0) {
        ssize_t n = ::write(fd_, ptr, rem);
        if (n <= 0) return false;
        ptr += n;
        rem -= (size_t)n;
    }
    return fsync(fd_) == 0;
}

bool WAL::persist_applied_lsn() {
    std::string tmp_path = meta_path_ + ".tmp";
    int fd = open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    std::string data = std::to_string(applied_lsn_) + "\n";
    const char *ptr = data.data();
    size_t rem = data.size();
    while (rem > 0) {
        ssize_t n = ::write(fd, ptr, rem);
        if (n <= 0) { ::close(fd); return false; }
        ptr += n;
        rem -= (size_t)n;
    }
    if (fsync(fd) != 0) { ::close(fd); return false; }
    ::close(fd);
    if (std::rename(tmp_path.c_str(), meta_path_.c_str()) != 0) return false;
    return true;
}

bool WAL::log_create_table(const TableSchema &schema, uint64_t &out_lsn) {
    std::lock_guard<std::mutex> lk(mtx_);
    out_lsn = next_lsn_++;

    std::string buf;
    buf.reserve(256);
    buf += std::to_string(out_lsn);
    buf += " CREATE_TABLE ";
    buf += encode(schema.name);
    buf += " ";
    buf += std::to_string(schema.columns.size());
    buf += "\n";
    for (const auto &col : schema.columns) {
        buf += encode(col.name);
        buf += " ";
        buf += type_to_str(col.type);
        buf += " ";
        buf += (col.not_null ? "1" : "0");
        buf += " ";
        buf += (col.primary_key ? "1" : "0");
        buf += "\n";
    }
    return write_entry(buf);
}

bool WAL::log_drop_table(const std::string &name, uint64_t &out_lsn) {
    std::lock_guard<std::mutex> lk(mtx_);
    out_lsn = next_lsn_++;
    return write_entry(std::to_string(out_lsn) + " DROP_TABLE " + encode(name) + "\n");
}

bool WAL::log_insert(const std::string &table,
                     const std::vector<std::string> &values,
                     time_t expiration,
                     uint64_t &out_lsn) {
    std::lock_guard<std::mutex> lk(mtx_);
    out_lsn = next_lsn_++;

    std::string buf;
    buf.reserve(256);
    buf += std::to_string(out_lsn);
    buf += " INSERT ";
    buf += encode(table);
    buf += " ";
    buf += std::to_string((long long)expiration);
    buf += " ";
    buf += std::to_string(values.size());
    buf += "\n";
    for (const auto &v : values) {
        buf += encode(v);
        buf += "\n";
    }
    return write_entry(buf);
}

bool WAL::log_insert_batch(const std::string &table,
                           const std::vector<Row> &rows,
                           uint64_t &out_last_lsn) {
    if (rows.empty()) {
        out_last_lsn = applied_lsn_;
        return true;
    }

    std::lock_guard<std::mutex> lk(mtx_);
    std::string enc_table = encode(table);
    std::string buf;
    buf.reserve(rows.size() * 96);

    for (const auto &row : rows) {
        uint64_t lsn = next_lsn_++;
        out_last_lsn = lsn;
        buf += std::to_string(lsn);
        buf += " INSERT ";
        buf += enc_table;
        buf += " ";
        buf += std::to_string((long long)row.expiration);
        buf += " ";
        buf += std::to_string(row.values.size());
        buf += "\n";
        for (const auto &v : row.values) {
            buf += encode(v);
            buf += "\n";
        }
    }
    return write_entry(buf);
}

bool WAL::log_delete_all(const std::string &table, uint64_t &out_lsn) {
    std::lock_guard<std::mutex> lk(mtx_);
    out_lsn = next_lsn_++;
    return write_entry(std::to_string(out_lsn) + " DELETE_ALL " + encode(table) + "\n");
}

bool WAL::mark_applied(uint64_t lsn) {
    std::lock_guard<std::mutex> lk(mtx_);
    if (lsn <= applied_lsn_) return true;
    applied_lsn_ = lsn;
    return persist_applied_lsn();
}

void WAL::flush_and_sync() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (fd_ >= 0) fsync(fd_);
}

void WAL::truncate() {
    std::lock_guard<std::mutex> lk(mtx_);
    if (fd_ < 0) return;
    if (ftruncate(fd_, 0) == 0) {
        lseek(fd_, 0, SEEK_END);
        fsync(fd_);
    }
}

void WAL::replay(const std::string &path, Database &db) {
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cout << "[WAL] No existing WAL — starting fresh.\n";
        return;
    }

    const uint64_t applied_lsn = load_applied_lsn(path + ".meta");
    std::string line;
    long long create_count = 0, insert_count = 0, drop_count = 0, delete_count = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;

        std::istringstream ss(line);
        uint64_t lsn = 0;
        std::string op;
        ss >> lsn >> op;

        if (op == "CREATE_TABLE") {
            std::string enc_name;
            size_t ncols = 0;
            ss >> enc_name >> ncols;

            TableSchema schema;
            schema.name = decode(enc_name);
            for (size_t i = 0; i < ncols; ++i) {
                std::string cline;
                if (!std::getline(f, cline)) break;
                std::istringstream cs(cline);
                std::string enc_cname, type_str, nn, pk;
                cs >> enc_cname >> type_str >> nn >> pk;
                ColumnDef cd;
                cd.name        = decode(enc_cname);
                cd.type        = parse_type(type_str);
                cd.not_null    = (nn == "1");
                cd.primary_key = (pk == "1");
                schema.columns.push_back(cd);
            }

            if (lsn > applied_lsn) {
                db.create_table(schema, true);
                create_count++;
            }
        } else if (op == "DROP_TABLE") {
            std::string enc_name;
            ss >> enc_name;
            if (lsn > applied_lsn) {
                db.drop_table(decode(enc_name));
                drop_count++;
            }
        } else if (op == "INSERT") {
            std::string enc_table;
            long long expiry = 0;
            size_t nvals = 0;
            ss >> enc_table >> expiry >> nvals;

            std::vector<std::string> vals;
            vals.reserve(nvals);
            for (size_t i = 0; i < nvals; ++i) {
                std::string vline;
                if (!std::getline(f, vline)) break;
                vals.push_back(decode(vline));
            }

            if (lsn > applied_lsn) {
                Table *t = db.get_table(decode(enc_table));
                if (t) {
                    std::string table_name = decode(enc_table);
                    std::string err = t->insert(
                        vals,
                        (time_t)expiry,
                        [&db, &table_name](const Row &row, uint64_t &offset) {
                            return db.storage_append_row(table_name, row, offset);
                        });
                    if (err.empty() || err.rfind("Duplicate primary key:", 0) == 0)
                        insert_count++;
                }
            }
        } else if (op == "DELETE_ALL") {
            std::string enc_name;
            ss >> enc_name;
            if (lsn > applied_lsn) {
                Table *t = db.get_table(decode(enc_name));
                if (t) {
                    db.storage_mark_deleted(decode(enc_name));
                    t->delete_all();
                    delete_count++;
                }
            }
        }
    }

    std::cout << "[WAL] Replay: " << create_count << " tables, "
              << insert_count << " rows, " << drop_count
              << " drops, " << delete_count << " deletes.\n";
}

} // namespace flexql
