#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <chrono>
#include <atomic>
#include <sstream>
#include <pqxx/pqxx>
#include "httplib.h"

using namespace std; 

// Variables for Performance Metrics
atomic<uint64_t> total_requests{0}, total_success{0}, cache_hits{0}, cache_misses{0};

// LRUCache Implementation
class Cache
{
public:
    // Constructor initialization
    Cache(int capacity) : cap(capacity) {}

    // Get value by key if present in cache
    bool get(string &key, string &out){
        lock_guard<mutex> lg(mut);
        auto it = mpp.find(key);
        if (it == mpp.end())
            return false;
        lst.splice(lst.begin(), lst, it->second.second);
        out = it->second.first;
        return true;
    }

    // Insert of update key in cache
    void put(string &key, string &value){
        lock_guard<mutex> lg(mut);
        auto it = mpp.find(key);

        if (it != mpp.end()){
            // Key already exists → update and move to front
            it->second.first = value;
            lst.splice(lst.begin(), lst, it->second.second);
            return;
        }

        // Evict least recently used key if cache full
        if (mpp.size() >= cap){
            auto lru = lst.back();
            mpp.erase(lru);
            lst.pop_back();
        }

        // Insert new key at front
        lst.push_front(key);
        mpp[key] = {value, lst.begin()};
    }

    // Remove a key from cache
    void erase(string &key){
        lock_guard<mutex> lg(mut);
        auto it = mpp.find(key);
        if (it == mpp.end())
            return;
        lst.erase(it->second.second);
        mpp.erase(it);
    }

    // Return current cache size
    int size(){
        lock_guard<mutex> lg(mut);
        return mpp.size();
    }

private:
    int cap;          // Maximum cache size
    list<string> lst; // Stores keys (front = most recent)
    unordered_map<string, pair<string, list<string>::iterator>> mpp;
    mutex mut;
};

// Database code
class KVDB{
public:
    // Constructor intialization
    KVDB(string &connstr) : connstr(connstr) {}

    // Insert or update
    bool put(string &key, string &value){
        try
        {
            pqxx::connection C(connstr);
            pqxx::work txn(C);
            txn.exec_params(
                "INSERT INTO kv_store (key, value) VALUES ($1, $2) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                key, value);
            txn.commit();
            return true;
        }
        catch (exception &e)
        {
            cerr << "DB put error: " << e.what() << "\n";
            return false;
        }
    }

    // Retrieve
    bool get(string &key, string &out){
        try
        {
            pqxx::connection C(connstr);
            pqxx::work txn(C);
            pqxx::result r = txn.exec_params("SELECT value FROM kv_store WHERE key=$1", key);
            if (r.empty())
                return false;
            out = r[0][0].as<string>();
            return true;
        }
        catch (exception &e)
        {
            cerr << "DB get error: " << e.what() << "\n";
            return false;
        }
    }

    // Delete
    bool del(string &key){
        try
        {
            pqxx::connection C(connstr);
            pqxx::work txn(C);
            txn.exec_params("DELETE FROM kv_store WHERE key=$1", key);
            txn.commit();
            return true;
        }
        catch (exception &e)
        {
            cerr << "DB del error: " << e.what() << "\n";
            return false;
        }
    }

private:
    string connstr;
};


// Main function
int main(int argc, char **argv){
    // Configuration setup
    string dbConnection = "host=localhost user=postgres password=postgres dbname=kvdb";
    int port = 8080;
    int cacheCapacity = 100;

    cout << "Starting KV server on port " << port << "\n";
    cout << "DB_CONN=" << dbConnection << " CACHE_CAPACITY=" << cacheCapacity << "\n";

    // Connection establishment and initialization
    KVDB db(dbConnection);
    Cache cache(cacheCapacity);

    httplib::Server svr;
    svr.set_read_timeout(5, 0);
    svr.set_write_timeout(5, 0);

    // Insert or Update a key-value pair
    svr.Put("/kv", [&](const httplib::Request &req, httplib::Response &res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()){ 
            res.status = 400;
            res.set_content("missing key", "text/plain"); 
            return; 
        }

        string value = req.body;
        bool ok = db.put(key, value);
        if (ok) {
            cache.put(key, value);
            total_success++;
            res.status = 200; res.set_content("Created/Updated\n", "text/plain");
        } 
        else {
            res.status = 500; res.set_content("Database error", "text/plain");
        } 
    });

    // Retrieve the value for a key
    svr.Get("/kv", [&](const httplib::Request &req, httplib::Response &res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()){
            res.status = 400;
            res.set_content("missing key", "text/plain");
            return;
        }

        string val;
        if (cache.get(key, val)){
            cache_hits++;
            total_success++;
            res.status = 200;
            res.set_content(val, "text/plain");
            return;
        }

        cache_misses++;
        if (db.get(key, val)){
            cache.put(key, val);
            total_success++;
            res.status = 200;
            res.set_content(val, "text/plain");
            return;
        }

        res.status = 404;
        res.set_content("Key not present\n", "text/plain"); 
    });

    // Delete
    svr.Delete("/kv", [&](const httplib::Request &req, httplib::Response &res){
        total_requests++;
        auto key = req.get_param_value("key");
        if (key.empty()) { 
            res.status = 400; 
            res.set_content("Missing Key\n", "text/plain"); 
            return; 
        }

        bool ok = db.del(key);
        cache.erase(key);
        if(ok){
            total_success++;
            res.status = 200; res.set_content("Deleted\n", "text/plain");
        } 
        else{
            res.status = 500; res.set_content("Database error\n", "text/plain");
        } 
    });

    // Get Metrics value
    svr.Get("/metrics", [&](const httplib::Request &, httplib::Response &res){
        ostringstream ss;
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