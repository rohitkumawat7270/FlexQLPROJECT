#pragma once
#include "common/types.h"
#include <list>
#include <unordered_map>
#include <string>
#include <mutex>

namespace flexql {

/*
 * LRU Query Cache
 * Key: normalized SQL string
 * Value: QueryResult
 */
class LRUCache {
public:
    explicit LRUCache(size_t capacity = 1024);

    bool get(const std::string &key, QueryResult &out);
    void put(const std::string &key, const QueryResult &val);
    void invalidate(const std::string &table); // called on INSERT/CREATE
    void clear();

private:
    struct Entry {
        std::string  key;
        QueryResult  val;
        std::string  table; // which table this query touches
    };

    size_t capacity_;
    std::list<Entry>                                   list_; // front = MRU
    std::unordered_map<std::string, std::list<Entry>::iterator> map_;
    std::mutex mtx_;
};

} // namespace flexql
