#pragma once
#include <string>
#include <vector>
#include <ctime>

namespace flexql {

enum class DataType { INT, DECIMAL, VARCHAR, DATETIME, UNKNOWN };

struct ColumnDef {
    std::string name;
    DataType    type       = DataType::UNKNOWN;
    bool        not_null   = false;
    bool        primary_key= false;
};

struct Row {
    std::vector<std::string> values;
    time_t expiration = 0; // 0 = no expiry
};

struct TableSchema {
    std::string            name;
    std::vector<ColumnDef> columns;
    int                    pk_index = -1;
};

struct QueryResult {
    bool                     ok = true;
    std::string              error;
    std::vector<std::string> column_names;
    std::vector<Row>         rows;
};

DataType    parse_type(const std::string &s);
std::string type_to_str(DataType t);

} // namespace flexql
