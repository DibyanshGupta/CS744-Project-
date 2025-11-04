// server.cpp
// Build: g++ server.cpp -o server -std=c++17 -O2 -pthread -lpqxx -lpq
// Requires: httplib.h (cpp-httplib) in same folder and libpqxx installed.

#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <optional>
#include <chrono>
#include <atomic>

#include <pqxx/pqxx>
#include <httplib.h>

using namespace httplib;

// ---------- Thread-safe LRU cache ----------
template<typename K, typename V>
class LRUCache {
public:
    explicit LRUCache(size_t capacity): capacity_(capacity) {}

    std::optional<V> get(const K& key) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return std::nullopt;
        list_.splice(list_.begin(), list_, it->second.second);
        return it->second.first;
    }

    void put(const K& key, const V& value) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.first = value;
            list_.splice(list_.begin(), list_, it->second.second);
            return;
        }
        if (map_.size() >= capacity_) {
            auto lru = list_.back();
            map_.erase(lru);
            list_.pop_back();
        }
        list_.push_front(key);
        map_[key] = {value, list_.begin()};
    }

    void erase(const K& key) {
        std::lock_guard<std::mutex> lg(mu_);
        auto it = map_.find(key);
        if (it == map_.end()) return;
        list_.erase(it->second.second);
        map_.erase(it);
    }

    size_t size() const {
        std::lock_guard<std::mutex> lg(mu_);
        return map_.size();
    }

private:
    size_t capacity_;
    std::list<K> list_;
    std::unordered_map<K, std::pair<V, typename std::list<K>::iterator>> map_;
    mutable std::mutex mu_;
};

// ---------- DB wrapper (libpqxx) ----------
class KVDB {
public:
    explicit KVDB(const std::string &connstr): connstr_(connstr) {}

    bool put(const std::string &key, const std::string &value) {
        try {
            pqxx::connection C(connstr_);
            pqxx::work txn(C);
            txn.exec_params(
                "INSERT INTO kv_store (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value);
            txn.commit();
            return true;
        } catch (const std::exception &e) {
            std::cerr << "DB put error: " << e.what() << "\n";
            return false;
        }
    }

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

// ---------- Simple server with metrics ----------
std::atomic<uint64_t> total_requests{0}, total_success{0}, cache_hits{0}, cache_misses{0};

int main(int argc, char** argv) {
    // config via env or defaults
    const char* db_url_env = getenv("DB_CONN");
    std::string db_conn = db_url_env ? db_url_env : "host=localhost user=postgres password=postgres dbname=kvdb";
    const char* port_env = getenv("PORT");
    int port = port_env ? atoi(port_env) : 8080;
    const char* cache_env = getenv("CACHE_CAPACITY");
    size_t cache_capacity = cache_env ? (size_t)std::stoul(cache_env) : 10000;

    std::cout << "DB_CONN=\"" << db_conn << "\" port=" << port << " cache_capacity=" << cache_capacity << "\n";

    KVDB db(db_conn);
    LRUCache<std::string, std::string> cache(cache_capacity);

    Server svr;
    svr.set_read_timeout(5,0);
    svr.set_write_timeout(5,0);

    // PUT /kv?key=KEY  (body = value)
    svr.Put("/kv", [&](const Request& req, Response& res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }
        std::string value = req.body;
        bool ok = db.put(key, value);    // synchronous write for simplicity
        if (ok) {
            cache.put(key, value);
            total_success++;
            res.status = 200;
            res.set_content("ok", "text/plain");
        } else {
            res.status = 500;
            res.set_content("db error", "text/plain");
        }
    });

    // GET /kv?key=KEY
    svr.Get("/kv", [&](const Request& req, Response& res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }

        // check cache
        auto v = cache.get(key);
        if (v.has_value()) {
            cache_hits++;
            total_success++;
            res.status = 200;
            res.set_content(v.value(), "text/plain");
            return;
        }

        cache_misses++;
        auto val = db.get(key);
        if (!val.has_value()) {
            res.status = 404;
            res.set_content("not found", "text/plain");
            return;
        }
        cache.put(key, val.value());
        total_success++;
        res.status = 200;
        res.set_content(val.value(), "text/plain");
    });

    // DELETE /kv?key=KEY
    svr.Delete("/kv", [&](const Request& req, Response& res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { res.status = 400; res.set_content("missing key", "text/plain"); return; }
        bool ok = db.del(key);
        cache.erase(key);
        if (ok) { total_success++; res.status = 200; res.set_content("deleted", "text/plain"); }
        else { res.status = 500; res.set_content("error", "text/plain"); }
    });

    // metrics: simple JSON
    svr.Get("/metrics", [&](const Request&, Response& res){
        std::ostringstream o;
        o << "{\n";
        o << "\"total_requests\": " << total_requests.load() << ",\n";
        o << "\"total_success\": " << total_success.load() << ",\n";
        o << "\"cache_hits\": " << cache_hits.load() << ",\n";
        o << "\"cache_misses\": " << cache_misses.load() << ",\n";
        o << "\"cache_size\": " << cache.size() << "\n";
        o << "}\n";
        res.set_content(o.str(), "application/json");
    });

    std::cout << "Starting server on 0.0.0.0:" << port << "\n";
    svr.listen("0.0.0.0", port);
    return 0;
}
