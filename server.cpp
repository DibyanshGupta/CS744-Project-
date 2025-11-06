// ===========================
// server.cpp — HTTP-based Key-Value Store (String-only version)
// ===========================

// Build command:
// g++ server.cpp -o server -std=c++17 -O2 -pthread -lpqxx -lpq
// Requires: httplib.h and libpqxx (PostgreSQL C++ library)

#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>
#include <chrono>
#include <atomic>
#include <sstream>

#include <pqxx/pqxx>
#include "httplib.h"

using namespace httplib;  // So we can use Server, Request, Response directly

// ==========================================================
// ---------- Thread-safe LRU (Least Recently Used) Cache ----------
// ==========================================================
// Fixed version: key = std::string, value = std::string (no templates)

class LRUCache {
public:
    // 'explicit' prevents accidental implicit conversions like LRUCache c = 10;
    explicit LRUCache(size_t capacity) : cap(capacity) {}

    // Get value by key if present in cache
    std::optional<std::string> get(const std::string &key) {
        std::lock_guard<std::mutex> lg(mut);
        auto it = mpp.find(key);
        if (it == mpp.end()) return std::nullopt;

        // Move this key to the front (most recently used)
        lst.splice(lst.begin(), lst, it->second.second);
        return it->second.first;
    }

    // Insert or update a key-value pair
    void put(const std::string &key, const std::string &value) {
        std::lock_guard<std::mutex> lg(mut);
        auto it = mpp.find(key);

        if (it != mpp.end()) {
            // Key already exists → update and move to front
            it->second.first = value;
            lst.splice(lst.begin(), lst, it->second.second);
            return;
        }

        // Evict least recently used key if cache full
        if (mpp.size() >= cap) {
            auto lru = lst.back();
            mpp.erase(lru);
            lst.pop_back();
        }

        // Insert new key at front
        lst.push_front(key);
        mpp[key] = {value, lst.begin()};
    }

    // Remove a key from cache
    void erase(const std::string &key) {
        std::lock_guard<std::mutex> lg(mut);
        auto it = mpp.find(key);
        if (it == mpp.end()) return;
        lst.erase(it->second.second);
        mpp.erase(it);
    }

    // Return current cache size
    size_t size() const {
        std::lock_guard<std::mutex> lg(mut);
        return mpp.size();
    }

private:
    size_t cap;  // Maximum cache size
    std::list<std::string> lst;  // Stores keys (front = most recent)
    std::unordered_map<std::string, std::pair<std::string, std::list<std::string>::iterator>> mpp;
    mutable std::mutex mut;  // Thread safety
};

// ==========================================================
// ---------- Database Layer (using libpqxx) ----------
// ==========================================================

class KVDB {
public:
    explicit KVDB(const std::string &connstr): connstr_(connstr) {}

    // Insert or update
    bool put(const std::string &key, const std::string &value) {
        try {
            pqxx::connection C(connstr_);
            pqxx::work txn(C);
            txn.exec_params(
                "INSERT INTO kv_store (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value
            );
            txn.commit();
            return true;
        } catch (const std::exception &e) {
            std::cerr << "DB put error: " << e.what() << "\n";
            return false;
        }
    }

    // Retrieve
    std::optional<std::string> get(const std::string &key) {
        try {
            pqxx::connection C(connstr_);
            pqxx::work txn(C);
            pqxx::result r = txn.exec_params("SELECT value FROM kv_store WHERE key=$1", key);
            if (r.empty()) return std::nullopt;
            return r[0][0].as<std::string>();
        } catch (const std::exception &e) {
            std::cerr << "DB get error: " << e.what() << "\n";
            return std::nullopt;
        }
    }

    // Delete
    bool del(const std::string &key) {
        try {
            pqxx::connection C(connstr_);
            pqxx::work txn(C);
            txn.exec_params("DELETE FROM kv_store WHERE key=$1", key);
            txn.commit();
            return true;
        } catch (const std::exception &e) {
            std::cerr << "DB del error: " << e.what() << "\n";
            return false;
        }
    }

private:
    std::string connstr_;
};

// ==========================================================
// ---------- Global Performance Metrics ----------
// ==========================================================

std::atomic<uint64_t> total_requests{0}, total_success{0}, cache_hits{0}, cache_misses{0};

// ==========================================================
// ---------- Main: Start HTTP Server ----------
// ==========================================================

int main(int argc, char** argv) {
    // Read environment variables or use defaults
    const char* db_env = getenv("DB_CONN");
    std::string db_conn = db_env ? db_env : "host=postgres user=postgres password=postgres dbname=kvdb";
    const char* p_env = getenv("PORT");
    int port = p_env ? atoi(p_env) : 8080;
    const char* c_env = getenv("CACHE_CAPACITY");
    size_t cache_capacity = c_env ? std::stoul(c_env) : 500;

    std::cout << "Starting KV server on port " << port << "\n";
    std::cout << "DB_CONN=" << db_conn << " CACHE_CAPACITY=" << cache_capacity << "\n";

    KVDB db(db_conn);
    LRUCache cache(cache_capacity);

    Server svr;
    svr.set_read_timeout(5, 0);
    svr.set_write_timeout(5, 0);

    // ==========================================================
    // PUT /kv?key=KEY
    // ==========================================================
    svr.Put("/kv", [&](const Request& req, Response& res) {
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }

        std::string value = req.body;
        bool ok = db.put(key, value);
        if (ok) {
            cache.put(key, value);
            total_success++;
            res.status = 200; res.set_content("ok", "text/plain");
        } else {
            res.status = 500; res.set_content("db error", "text/plain");
        }
    });

    // ==========================================================
    // GET /kv?key=KEY
    // ==========================================================
    svr.Get("/kv", [&](const Request& req, Response& res) {
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }

        auto v = cache.get(key);
        if (v.has_value()) {
            cache_hits++;
            total_success++;
            res.status = 200; res.set_content(v.value(), "text/plain");
            return;
        }

        cache_misses++;
        auto val = db.get(key);
        if (!val.has_value()) {
            res.status = 404; res.set_content("not found", "text/plain");
            return;
        }
        cache.put(key, val.value());
        total_success++;
        res.status = 200; res.set_content(val.value(), "text/plain");
    });

    // ==========================================================
    // DELETE /kv?key=KEY
    // ==========================================================
    svr.Delete("/kv", [&](const Request& req, Response& res) {
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }

        bool ok = db.del(key);
        cache.erase(key);
        if (ok) {
            total_success++;
            res.status = 200; res.set_content("deleted", "text/plain");
        } else {
            res.status = 500; res.set_content("error", "text/plain");
        }
    });

    // ==========================================================
    // GET /metrics
    // ==========================================================
    svr.Get("/metrics", [&](const Request&, Response& res) {
        std::ostringstream ss;
        ss << "{\n";
        ss << "\"total_requests\": " << total_requests.load() << ",\n";
        ss << "\"total_success\": " << total_success.load() << ",\n";
        ss << "\"cache_hits\": " << cache_hits.load() << ",\n";
        ss << "\"cache_misses\": " << cache_misses.load() << ",\n";
        ss << "\"cache_size\": " << cache.size() << "\n";
        ss << "}\n";
        res.set_content(ss.str(), "application/json");
    });

    // Start the server
    svr.listen("0.0.0.0", port);
    return 0;
}
