#include "cache/lru_cache.h"
#include <algorithm>
#include <cctype>

namespace flexql {

LRUCache::LRUCache(size_t capacity) : capacity_(capacity) {}

bool LRUCache::get(const std::string &key, QueryResult &out) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    // Move to front (MRU)
    list_.splice(list_.begin(), list_, it->second);
    out = it->second->val;
    return true;
}

void LRUCache::put(const std::string &key, const QueryResult &val) {
    std::lock_guard<std::mutex> lk(mtx_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        it->second->val = val;
        list_.splice(list_.begin(), list_, it->second);
        return;
    }
    if (list_.size() >= capacity_) {
        // Evict LRU
        auto last = std::prev(list_.end());
        map_.erase(last->key);
        list_.erase(last);
    }
    list_.push_front({key, val, ""});
    map_[key] = list_.begin();
}

void LRUCache::invalidate(const std::string &table) {
    std::lock_guard<std::mutex> lk(mtx_);
    // Remove all entries touching this table
    // Simple approach: remove all entries whose key contains the table name
    std::string upper_table = table;
    for (auto &c : upper_table) c = toupper((unsigned char)c);

    for (auto it = list_.begin(); it != list_.end(); ) {
        std::string k = it->key;
        for (auto &c : k) c = toupper((unsigned char)c);
        if (k.find(upper_table) != std::string::npos) {
            map_.erase(it->key);
            it = list_.erase(it);
        } else {
            ++it;
        }
    }
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lk(mtx_);
    list_.clear();
    map_.clear();
}

}
