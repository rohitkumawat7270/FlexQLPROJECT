#pragma once
#include "common/types.h"
#include <string>
#include <vector>

namespace flexql {

enum class QueryType {
    CREATE_TABLE,
    DROP_TABLE,
    DELETE_ROWS,
    INSERT,
    SELECT,
    UNKNOWN
};

struct WhereClause {
    std::string column;
    std::string op;     // =, <, >, <=, >=, !=
    std::string value;
    bool        present = false;
};

struct JoinClause {
    std::string table_b;
    std::string col_a;  // tableA.col
    std::string col_b;  // tableB.col
    bool        present = false;
};

struct ParsedQuery {
    QueryType              type = QueryType::UNKNOWN;
    std::string            error;

    // CREATE TABLE
    std::string            table_name;
    std::vector<ColumnDef> col_defs;
    bool                   if_not_exists = false;

    // INSERT — single row (backward compat) and multi-row
    std::vector<std::string>              insert_values;
    std::vector<std::vector<std::string>> multi_row_values;
    time_t                                expiration = 0;

    // SELECT
    bool                     select_star = true;
    std::vector<std::string> select_cols;
    std::string              from_table;
    WhereClause              where;
    JoinClause               join;
};

class Parser {
public:
    static ParsedQuery parse(const std::string &sql);
private:
    static ParsedQuery parse_create(const std::vector<std::string> &tokens, const std::string &raw);
    static ParsedQuery parse_drop(const std::vector<std::string> &tokens);
    static ParsedQuery parse_delete(const std::vector<std::string> &tokens);
    static ParsedQuery parse_insert(const std::vector<std::string> &tokens, const std::string &raw);
    static ParsedQuery parse_select(const std::vector<std::string> &tokens, const std::string &raw);
    static std::vector<std::string> tokenize(const std::string &s);
    static std::string trim(const std::string &s);
    static std::string to_upper(const std::string &s);
};

} // namespace flexql
