#include "crow_all.h"
#include "json.hpp"
#include <iostream>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <list>
#include <string>
#include <chrono>
#include <libpq-fe.h>

using namespace std;
using json = nlohmann::json;

// =============== LRU CACHE (string->string) ================
class LRUCache {
    size_t capacity;
    list<pair<string, string>> kvcache;
    unordered_map<string, list<pair<string, string>>::iterator> kvmap;
    mutable mutex mtx;

public:
    LRUCache(size_t cap) { capacity = cap; }

    void put(const string& key, const string& value) {
        lock_guard<mutex> lock(mtx);
        auto it = kvmap.find(key);
        if (it != kvmap.end()) kvcache.erase(it->second);

        kvcache.push_front({key, value});
        kvmap[key] = kvcache.begin();

        if (kvcache.size() > capacity) {
            auto last = kvcache.back();
            kvmap.erase(last.first);
            kvcache.pop_back();
        }
    }

    bool get(const string& key, string& value) {
        lock_guard<mutex> lock(mtx);
        auto it = kvmap.find(key);
        if (it == kvmap.end()) return false;

        value = it->second->second;
        kvcache.erase(it->second);
        kvcache.push_front({key, value});
        kvmap[key] = kvcache.begin();
        return true;
    }

    void remove(const string& key) {
        lock_guard<mutex> lock(mtx);
        auto it = kvmap.find(key);
        if (it == kvmap.end()) return;
        kvcache.erase(it->second);
        kvmap.erase(it);
    }
};

// =================== POSTGRES CONFIG ===================
static const char* DB_CONNINFO =
    "host=localhost port=5432 user=postgres password=postgres dbname=kvdb";

thread_local PGconn* thread_conn = nullptr;
thread_local std::chrono::steady_clock::time_point last_ping;

PGconn* get_connection() {
    using namespace std::chrono;

    if (!thread_conn || PQstatus(thread_conn) != CONNECTION_OK) {
        if (thread_conn) {
            PQfinish(thread_conn);
            thread_conn = nullptr;
        }
        thread_conn = PQconnectdb(DB_CONNINFO);

        if (!thread_conn || PQstatus(thread_conn) != CONNECTION_OK) {
            cerr << "PostgreSQL connection failed: "
                 << (thread_conn ? PQerrorMessage(thread_conn) : "(null)") << endl;
            if (thread_conn) { PQfinish(thread_conn); thread_conn = nullptr; }
            return nullptr;
        }
        last_ping = steady_clock::now();
        return thread_conn;
    }

    auto now = steady_clock::now();
    if (duration_cast<seconds>(now - last_ping).count() >= 5) {
        PGresult* res = PQexec(thread_conn, "SELECT 1");
        if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
            cerr << "[PG] Ping failed, reconnecting: "
                 << PQerrorMessage(thread_conn) << endl;
            if (res) PQclear(res);
            PQfinish(thread_conn);
            thread_conn = PQconnectdb(DB_CONNINFO);

            if (!thread_conn || PQstatus(thread_conn) != CONNECTION_OK) {
                cerr << "[PG] Reconnect failed: "
                     << (thread_conn ? PQerrorMessage(thread_conn) : "(null)") << endl;
                if (thread_conn) { PQfinish(thread_conn); thread_conn = nullptr; }
                return nullptr;
            }
        }
        if (res) PQclear(res);
        last_ping = now;
    }

    return thread_conn;
}

// ==================== JSON helpers ======================
static std::string to_string_json_value(const nlohmann::json &v) {
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_integer()) return std::to_string(v.get<long long>());
    if (v.is_number_unsigned()) return std::to_string(v.get<unsigned long long>());
    if (v.is_number_float()) {
        std::ostringstream ss;
        ss << v.get<double>();
        return ss.str();
    }
    return v.dump();
}

// convert JSON value (number or string) to int64 safely; returns false on error
bool json_to_int64(const nlohmann::json &v, int64_t &out) {
    try {
        if (v.is_number_integer()) {
            out = v.get<int64_t>();
            return true;
        }
        if (v.is_string()) {
            std::string s = v.get<std::string>();
            if (s.empty()) return false;
            size_t pos = 0;
            long long val = std::stoll(s, &pos);
            if (pos != s.size()) return false;
            out = (int64_t)val;
            return true;
        }
        if (v.is_number_float()) {
            out = static_cast<int64_t>(v.get<double>());
            return true;
        }
    } catch (...) { }
    return false;
}

// convert route path (string) to int64
bool str_to_int64(const std::string &s, int64_t &out) {
    try {
        if (s.empty()) return false;
        size_t pos = 0;
        long long val = std::stoll(s, &pos);
        if (pos != s.size()) return false;
        out = (int64_t)val;
        return true;
    } catch (...) { return false; }
}

// ==================== DB OPERATIONS (integer key) ======================
bool db_create_or_update(int64_t key, const std::string& value) {
    PGconn* conn = get_connection();
    if (!conn) return false;

    std::string keystr = std::to_string(key);
    const char *paramValues[2] = { keystr.c_str(), value.c_str() };

    PGresult* res = PQexecParams(conn,
        "INSERT INTO kv_store(\"key\", value) VALUES ($1::bigint, $2) "
        "ON CONFLICT (\"key\") DO UPDATE SET value = EXCLUDED.value",
        2,            // number of params
        NULL,         // param types
        paramValues,
        NULL,         // param lengths
        NULL,         // param formats
        0);           // result format (text)

    if (!res) {
        cerr << "[DB] null result: " << PQerrorMessage(conn) << "\n";
        return false;
    }
    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok) cerr << "[DB] Insert/Update failed: " << PQerrorMessage(conn) << "\n";
    PQclear(res);
    return ok;
}

bool db_read(int64_t key, std::string& value) {
    PGconn* conn = get_connection();
    if (!conn) return false;

    std::string keystr = std::to_string(key);
    const char *paramValues[1] = { keystr.c_str() };

    PGresult* res = PQexecParams(conn,
        "SELECT value FROM kv_store WHERE \"key\" = $1::bigint",
        1, NULL, paramValues, NULL, NULL, 0);

    if (!res) {
        cerr << "[DB] null result from read: " << PQerrorMessage(conn) << "\n";
        return false;
    }

    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) == 0) {
        PQclear(res);
        return false;
    }

    char *val = PQgetvalue(res, 0, 0);
    if (val) value = val;
    PQclear(res);
    return true;
}

bool db_delete(int64_t key) {
    PGconn* conn = get_connection();
    if (!conn) return false;

    std::string keystr = std::to_string(key);
    const char *paramValues[1] = { keystr.c_str() };

    PGresult* res = PQexecParams(conn,
        "DELETE FROM kv_store WHERE \"key\" = $1::bigint",
        1, NULL, paramValues, NULL, NULL, 0);

    if (!res) {
        cerr << "[DB] null result from delete: " << PQerrorMessage(conn) << "\n";
        return false;
    }

    bool ok = (PQresultStatus(res) == PGRES_COMMAND_OK && atoi(PQcmdTuples(res)) > 0);
    PQclear(res);
    return ok;
}

// ================= SERVER ======================
LRUCache cache(100);

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << " <thread_pool_size>\n";
        return 1;
    }

    int threads = 1;
    try { threads = stoi(argv[1]); } catch (...) { threads = 1; }

    crow::SimpleApp app;

    CROW_ROUTE(app, "/create").methods("POST"_method)
    ([](const crow::request& req){
        if (req.body.empty()) return crow::response(400, "Empty body");

        nlohmann::json j;
        try {
            j = nlohmann::json::parse(req.body);
        } catch (const nlohmann::json::parse_error &e) {
            cerr << "[JSON] parse error: " << e.what() << " payload: " << req.body << "\n";
            return crow::response(400, "Invalid JSON");
        }

        if (!j.contains("key") || !j.contains("value")) {
            return crow::response(400, "Missing key or value");
        }

        int64_t key_num;
        if (!json_to_int64(j["key"], key_num)) {
            return crow::response(400, "Invalid key (expected integer)");
        }

        std::string value = to_string_json_value(j["value"]);
        bool done = db_create_or_update(key_num, value);
        if (done) cache.put(std::to_string(key_num), value);
        return crow::response(done ? 200 : 500, done ? "Created" : "DB Error");
    });

    CROW_ROUTE(app, "/read/<string>")
    ([](const std::string &key_path){
        std::string value;
        bool hit = cache.get(key_path, value);
        if (hit) return crow::response(200, value);

        int64_t key_num;
        if (!str_to_int64(key_path, key_num)) return crow::response(400, "Invalid key");

        if (db_read(key_num, value)) {
            cache.put(key_path, value);
            return crow::response(200, value);
        }
        return crow::response(404, "Not found");
    });

    CROW_ROUTE(app, "/delete/<string>").methods("DELETE"_method)
    ([](const std::string &key_path){
        int64_t key_num;
        if (!str_to_int64(key_path, key_num)) return crow::response(400, "Invalid key");

        bool done = db_delete(key_num);
        if (done) cache.remove(key_path);
        return crow::response(done ? 200 : 500, done ? "Deleted" : "Not found");
    });

    cout << "Server running on port 8080 using " << threads << " threads\n";
    app.loglevel(crow::LogLevel::Error);
    app.port(8080).concurrency(threads).run();
    return 0;
}
