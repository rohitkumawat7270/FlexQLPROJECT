#include "storage/storage_engine.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <sys/types.h>
#include <unordered_set>
#include <unistd.h>

namespace flexql {
namespace fs = std::filesystem;

static std::string escape_text_field(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '\\') out += "\\\\";
        else if (c == '\t') out += "\\t";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else out += c;
    }
    return out;
}

static bool write_text_table(const std::string &path,
                             const TableSchema &schema,
                             const std::vector<Row> &rows) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) return false;

    out << "FLEXQL_TEXT_V1\n";
    out << "TABLE_NAME\t" << schema.name << "\n";
    out << "COLUMNS\t";
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        if (i) out << '\t';
        out << schema.columns[i].name;
    }
    out << "\t__ROW_EXPIRATION\n";

    for (const auto &row : rows) {
        for (size_t i = 0; i < row.values.size(); ++i) {
            if (i) out << '\t';
            out << escape_text_field(row.values[i]);
        }
        out << '\t' << (long long)row.expiration << "\n";
    }
    return out.good();
}

static bool read_row_header(std::istream &in,
                            uint64_t &offset,
                            uint8_t &flags,
                            int64_t &exp,
                            uint32_t &nv) {
    offset = (uint64_t)in.tellg();

    uint32_t magic = 0;
    in.read(reinterpret_cast<char*>(&magic), 4);
    if (in.gcount() < 4) return false;
    if (magic != ROW_MAGIC) return false;

    in.read(reinterpret_cast<char*>(&flags), 1);
    in.read(reinterpret_cast<char*>(&exp), 8);
    in.read(reinterpret_cast<char*>(&nv), 4);
    return in.good();
}

static bool read_row_values(std::istream &in,
                            uint32_t nv,
                            std::vector<std::string> &vals,
                            int pk_index,
                            std::string *pk_value) {
    vals.clear();
    vals.reserve(nv);

    for (uint32_t i = 0; i < nv; ++i) {
        uint32_t len = 0;
        in.read(reinterpret_cast<char*>(&len), 4);
        if (!in.good()) return false;

        std::string v(len, '\0');
        if (len > 0) {
            in.read(&v[0], len);
            if (!in.good()) return false;
        }

        if ((int)i == pk_index && pk_value) *pk_value = v;
        vals.push_back(std::move(v));
    }
    return true;
}

std::string StorageEngine::to_upper(const std::string &s) {
    std::string r = s;
    for (auto &c : r) c = (char)toupper((unsigned char)c);
    return r;
}

StorageEngine::StorageEngine(const std::string &data_dir)
    : data_dir_(data_dir) {
    fs::create_directories(data_dir_);
}

std::string StorageEngine::schema_path(const std::string &name) const {
    return data_dir_ + "/" + to_upper(name) + ".schema";
}

std::string StorageEngine::data_path(const std::string &name) const {
    return data_dir_ + "/" + to_upper(name) + ".data";
}

std::string StorageEngine::txt_path(const std::string &name) const {
    return data_dir_ + "/" + to_upper(name) + ".txt";
}

bool StorageEngine::write_schema(const TableSchema &schema) {
    std::ofstream f(schema_path(schema.name), std::ios::trunc);
    if (!f) return false;

    f << "FLEXQL_SCHEMA_V1\n";
    f << "TABLE_NAME " << to_upper(schema.name) << "\n";
    f << "COLUMNS " << schema.columns.size() << "\n";
    for (const auto &col : schema.columns) {
        f << col.name
          << " " << type_to_str(col.type)
          << " " << (col.not_null ? "1" : "0")
          << " " << (col.primary_key ? "1" : "0")
          << "\n";
    }
    if (!f.good()) return false;
    return write_text_table(txt_path(schema.name), schema, {});
}

bool StorageEngine::delete_table_files(const std::string &name) {
    std::error_code ec;
    fs::remove(schema_path(name), ec);
    fs::remove(data_path(name), ec);
    fs::remove(txt_path(name), ec);
    return true;
}

bool StorageEngine::append_row(const std::string &table_name, const Row &row, uint64_t &offset) {
    int fd = open(data_path(table_name).c_str(),
                  O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return false;

    off_t pos = lseek(fd, 0, SEEK_END);
    if (pos < 0) {
        ::close(fd);
        return false;
    }
    offset = (uint64_t)pos;

    std::string buf;
    buf.reserve(17 + row.values.size() * 24);

    auto push_u8  = [&](uint8_t v)  { buf.append(reinterpret_cast<const char*>(&v), 1); };
    auto push_u32 = [&](uint32_t v) { buf.append(reinterpret_cast<const char*>(&v), 4); };
    auto push_i64 = [&](int64_t v)  { buf.append(reinterpret_cast<const char*>(&v), 8); };

    push_u32(ROW_MAGIC);
    push_u8((row.expiration != 0) ? FLAG_HAS_EXPIRY : 0);
    push_i64((int64_t)row.expiration);
    push_u32((uint32_t)row.values.size());
    for (const auto &v : row.values) {
        push_u32((uint32_t)v.size());
        if (!v.empty()) buf.append(v.data(), v.size());
    }

    const char *ptr = buf.data();
    size_t rem = buf.size();
    while (rem > 0) {
        ssize_t n = ::write(fd, ptr, rem);
        if (n <= 0) {
            ::close(fd);
            return false;
        }
        ptr += n;
        rem -= (size_t)n;
    }
    bool ok = (fsync(fd) == 0);
    ::close(fd);
    if (!ok) return false;

    std::ofstream txt(txt_path(table_name), std::ios::app);
    if (!txt) return false;
    for (size_t i = 0; i < row.values.size(); ++i) {
        if (i) txt << '\t';
        txt << escape_text_field(row.values[i]);
    }
    txt << '\t' << (long long)row.expiration << "\n";
    txt.flush();
    return txt.good();
}

bool StorageEngine::append_rows_batch(const std::string &table_name,
                                      const std::vector<Row> &rows,
                                      std::vector<uint64_t> &offsets) {
    offsets.clear();
    if (rows.empty()) return true;

    size_t est = rows.size() * 128;
    std::string buf;
    buf.reserve(est);
    offsets.reserve(rows.size());

    auto push_u8  = [&](uint8_t v)  { buf.append(reinterpret_cast<char*>(&v), 1); };
    auto push_u32 = [&](uint32_t v) { buf.append(reinterpret_cast<char*>(&v), 4); };
    auto push_u64 = [&](int64_t v)  { buf.append(reinterpret_cast<char*>(&v), 8); };

    uint64_t cursor = 0;
    for (const auto &row : rows) {
        offsets.push_back(cursor);
        push_u32(ROW_MAGIC);
        push_u8((row.expiration != 0) ? FLAG_HAS_EXPIRY : 0);
        push_u64((int64_t)row.expiration);
        push_u32((uint32_t)row.values.size());
        cursor += 17;
        for (const auto &v : row.values) {
            push_u32((uint32_t)v.size());
            cursor += 4 + v.size();
            if (!v.empty()) buf.append(v.data(), v.size());
        }
    }

    int fd = open(data_path(table_name).c_str(),
                  O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return false;

    off_t base = lseek(fd, 0, SEEK_END);
    if (base < 0) {
        ::close(fd);
        return false;
    }
    for (auto &off : offsets) off += (uint64_t)base;

    const char *ptr = buf.data();
    size_t remaining = buf.size();
    while (remaining > 0) {
        ssize_t n = ::write(fd, ptr, remaining);
        if (n <= 0) {
            ::close(fd);
            return false;
        }
        ptr += n;
        remaining -= (size_t)n;
    }
    bool ok = (fsync(fd) == 0);
    ::close(fd);
    if (!ok) return false;

    std::ofstream txt(txt_path(table_name), std::ios::app);
    if (!txt) return false;
    for (const auto &row : rows) {
        for (size_t i = 0; i < row.values.size(); ++i) {
            if (i) txt << '\t';
            txt << escape_text_field(row.values[i]);
        }
        txt << '\t' << (long long)row.expiration << "\n";
    }
    txt.flush();
    return txt.good();
}

bool StorageEngine::mark_all_deleted(const std::string &table_name) {
    std::fstream f(data_path(table_name), std::ios::binary | std::ios::in | std::ios::out);
    if (!f) return true;

    while (true) {
        std::streampos row_start = f.tellg();

        uint32_t magic = 0;
        f.read(reinterpret_cast<char*>(&magic), 4);
        if (f.gcount() < 4) break;
        if (magic != ROW_MAGIC) break;

        uint8_t flags = 0;
        f.read(reinterpret_cast<char*>(&flags), 1);
        if (!(flags & FLAG_DELETED)) {
            f.seekp((std::streamoff)row_start + 4);
            flags |= FLAG_DELETED;
            f.write(reinterpret_cast<const char*>(&flags), 1);
        }

        f.seekg((std::streamoff)row_start + 5);
        int64_t exp = 0;
        uint32_t nv = 0;
        f.read(reinterpret_cast<char*>(&exp), 8);
        f.read(reinterpret_cast<char*>(&nv), 4);
        for (uint32_t i = 0; i < nv; ++i) {
            uint32_t len = 0;
            f.read(reinterpret_cast<char*>(&len), 4);
            f.seekg(len, std::ios::cur);
        }
    }

    TableSchema schema;
    bool found_schema = false;
    for (auto &s : load_all_schemas()) {
        if (to_upper(s.name) == to_upper(table_name)) {
            schema = std::move(s);
            found_schema = true;
            break;
        }
    }
    if (found_schema) return write_text_table(txt_path(table_name), schema, {});

    std::ofstream txt(txt_path(table_name), std::ios::trunc);
    return txt.good();
}

std::vector<std::pair<std::vector<std::string>, time_t>>
StorageEngine::load_rows(const std::string &table_name) {
    std::vector<std::pair<std::vector<std::string>, time_t>> result;
    scan_live_rows(table_name, [&](uint64_t, const Row &row) {
        result.push_back({row.values, row.expiration});
        return true;
    });
    return result;
}

bool StorageEngine::read_row_at(const std::string &table_name,
                                uint64_t offset,
                                Row &row,
                                bool &is_deleted) {
    std::ifstream f(data_path(table_name), std::ios::binary);
    if (!f) return false;
    f.seekg((std::streamoff)offset);
    if (!f.good()) return false;

    uint64_t row_offset = 0;
    uint8_t flags = 0;
    int64_t exp = 0;
    uint32_t nv = 0;
    if (!read_row_header(f, row_offset, flags, exp, nv)) return false;

    std::vector<std::string> vals;
    if (!read_row_values(f, nv, vals, -1, nullptr)) return false;

    row.values = std::move(vals);
    row.expiration = (time_t)exp;
    is_deleted = (flags & FLAG_DELETED) != 0;
    return true;
}

bool StorageEngine::scan_live_rows(const std::string &table_name,
                                   const RowVisitor &visitor) {
    std::ifstream f(data_path(table_name), std::ios::binary);
    if (!f) return true;

    time_t now = time(nullptr);
    while (true) {
        uint64_t offset = 0;
        uint8_t flags = 0;
        int64_t exp = 0;
        uint32_t nv = 0;
        if (!read_row_header(f, offset, flags, exp, nv)) break;

        std::vector<std::string> vals;
        if (!read_row_values(f, nv, vals, -1, nullptr)) return false;

        if (flags & FLAG_DELETED) continue;
        if (exp != 0 && now > (time_t)exp) continue;

        Row row;
        row.values = std::move(vals);
        row.expiration = (time_t)exp;
        if (!visitor(offset, row)) break;
    }
    return true;
}

bool StorageEngine::load_table_metadata(const TableSchema &schema,
                                        size_t &live_rows,
                                        const MetadataVisitor &visitor) {
    live_rows = 0;
    std::ifstream f(data_path(schema.name), std::ios::binary);
    if (!f) return true;

    int pk_index = -1;
    for (int i = 0; i < (int)schema.columns.size(); ++i) {
        if (schema.columns[i].primary_key) {
            pk_index = i;
            break;
        }
    }

    time_t now = time(nullptr);
    while (true) {
        uint64_t offset = 0;
        uint8_t flags = 0;
        int64_t exp = 0;
        uint32_t nv = 0;
        if (!read_row_header(f, offset, flags, exp, nv)) break;

        std::vector<std::string> vals;
        std::string pk_value;
        if (!read_row_values(f, nv, vals, pk_index, pk_index >= 0 ? &pk_value : nullptr))
            return false;

        if (flags & FLAG_DELETED) continue;
        if (exp != 0 && now > (time_t)exp) continue;

        live_rows++;
        if (!visitor(offset, (time_t)exp, pk_value)) break;
    }
    return true;
}

std::vector<TableSchema> StorageEngine::load_all_schemas() {
    std::vector<TableSchema> schemas;

    if (!fs::exists(data_dir_)) return schemas;

    for (const auto &entry : fs::directory_iterator(data_dir_)) {
        if (entry.path().extension() != ".schema") continue;

        std::ifstream f(entry.path());
        if (!f) continue;

        std::string hdr;
        std::getline(f, hdr);
        if (hdr != "FLEXQL_SCHEMA_V1") continue;

        TableSchema schema;

        std::string line;
        std::getline(f, line);
        {
            std::istringstream ss(line);
            std::string tag;
            ss >> tag >> schema.name;
        }

        std::getline(f, line);
        size_t ncols = 0;
        {
            std::istringstream ss(line);
            std::string tag;
            ss >> tag >> ncols;
        }

        for (size_t i = 0; i < ncols; ++i) {
            std::getline(f, line);
            std::istringstream ss(line);
            ColumnDef cd;
            std::string type_str, nn, pk;
            ss >> cd.name >> type_str >> nn >> pk;
            cd.type = parse_type(type_str);
            cd.not_null = (nn == "1");
            cd.primary_key = (pk == "1");
            schema.columns.push_back(cd);
        }

        if (!schema.name.empty() && !schema.columns.empty())
            schemas.push_back(std::move(schema));
    }
    return schemas;
}

bool StorageEngine::compact_table(const std::string &table_name) {
    auto live_rows = load_rows(table_name);

    std::string tmp = data_path(table_name) + ".tmp";
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) return false;

    for (auto &[vals, exp] : live_rows) {
        uint32_t magic = ROW_MAGIC;
        out.write(reinterpret_cast<const char*>(&magic), 4);

        uint8_t flags = (exp != 0) ? FLAG_HAS_EXPIRY : 0;
        out.write(reinterpret_cast<const char*>(&flags), 1);

        int64_t e = (int64_t)exp;
        out.write(reinterpret_cast<const char*>(&e), 8);

        uint32_t nv = (uint32_t)vals.size();
        out.write(reinterpret_cast<const char*>(&nv), 4);

        for (const auto &v : vals) {
            uint32_t len = (uint32_t)v.size();
            out.write(reinterpret_cast<const char*>(&len), 4);
            if (len > 0) out.write(v.data(), len);
        }
    }
    out.close();

    std::string dest = data_path(table_name);
    if (std::rename(tmp.c_str(), dest.c_str()) != 0) return false;

    TableSchema schema;
    bool found_schema = false;
    for (auto &s : load_all_schemas()) {
        if (to_upper(s.name) == to_upper(table_name)) {
            schema = std::move(s);
            found_schema = true;
            break;
        }
    }
    if (found_schema) {
        std::vector<Row> txt_rows;
        txt_rows.reserve(live_rows.size());
        for (const auto &[vals, exp] : live_rows)
            txt_rows.push_back({vals, exp});
        if (!write_text_table(txt_path(table_name), schema, txt_rows))
            return false;
    }

    std::cout << "[STORAGE] Compacted " << table_name
              << " -> " << live_rows.size() << " rows\n";
    return true;
}

bool StorageEngine::checkpoint(
        const std::vector<std::pair<TableSchema, std::vector<Row>>> &snapshot) {
    fs::create_directories(data_dir_);

    std::unordered_set<std::string> live_tables;
    for (const auto &[schema, rows] : snapshot) {
        (void)rows;
        live_tables.insert(to_upper(schema.name));
    }

    for (const auto &entry : fs::directory_iterator(data_dir_)) {
        auto ext = entry.path().extension().string();
        if (ext != ".schema" && ext != ".data" && ext != ".txt") continue;
        std::string stem = entry.path().stem().string();
        if (!live_tables.count(to_upper(stem))) {
            std::error_code ec;
            fs::remove(entry.path(), ec);
        }
    }

    for (const auto &[schema, rows] : snapshot) {
        if (!write_schema(schema)) return false;

        std::string tmp = data_path(schema.name) + ".tmp";
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) return false;

        for (const auto &row : rows) {
            uint32_t magic = ROW_MAGIC;
            out.write(reinterpret_cast<const char*>(&magic), 4);

            uint8_t flags = (row.expiration != 0) ? FLAG_HAS_EXPIRY : 0;
            out.write(reinterpret_cast<const char*>(&flags), 1);

            int64_t exp = (int64_t)row.expiration;
            out.write(reinterpret_cast<const char*>(&exp), 8);

            uint32_t nv = (uint32_t)row.values.size();
            out.write(reinterpret_cast<const char*>(&nv), 4);

            for (const auto &v : row.values) {
                uint32_t len = (uint32_t)v.size();
                out.write(reinterpret_cast<const char*>(&len), 4);
                if (len > 0) out.write(v.data(), len);
            }
        }
        out.close();
        if (!out.good()) return false;

        std::string dest = data_path(schema.name);
        if (std::rename(tmp.c_str(), dest.c_str()) != 0) return false;
        if (!write_text_table(txt_path(schema.name), schema, rows)) return false;
    }

    std::cout << "[STORAGE] Checkpointed " << snapshot.size()
              << " tables to disk.\n";
    return true;
}

} // namespace flexql
