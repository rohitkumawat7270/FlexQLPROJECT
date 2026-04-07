#include "common/types.h"
#include <algorithm>
#include <cctype>

namespace flexql {

static std::string to_upper(std::string s) {
    for (auto &c : s) c = toupper(c);
    return s;
}

DataType parse_type(const std::string &s) {
    std::string u = to_upper(s);
    if (u == "INT")      return DataType::INT;
    if (u == "DECIMAL")  return DataType::DECIMAL;
    if (u == "VARCHAR" || u == "TEXT") return DataType::VARCHAR;
    if (u == "DATETIME") return DataType::DATETIME;
    return DataType::UNKNOWN;
}

std::string type_to_str(DataType t) {
    switch (t) {
    case DataType::INT:      return "INT";
    case DataType::DECIMAL:  return "DECIMAL";
    case DataType::VARCHAR:  return "VARCHAR";
    case DataType::DATETIME: return "DATETIME";
    default:                 return "UNKNOWN";
    }
}

} // namespace flexql
