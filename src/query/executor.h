#pragma once
#include "common/types.h"
#include "parser/parser.h"
#include "storage/database.h"
#include <string>

namespace flexql {

class Executor {
public:
    explicit Executor(Database &db);
    QueryResult execute(const std::string &sql);

private:
    Database &db_;

    QueryResult exec_create(const ParsedQuery &q);
    QueryResult exec_drop(const ParsedQuery &q);
    QueryResult exec_delete(const ParsedQuery &q);
    QueryResult exec_insert(const ParsedQuery &q);
    QueryResult exec_select(const ParsedQuery &q);
    QueryResult exec_join(const ParsedQuery &q);
};

} // namespace flexql
