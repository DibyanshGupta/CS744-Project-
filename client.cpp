// loadgen.cpp
// Build: g++ loadgen.cpp -o loadgen -std=c++17 -O2 -pthread
// Requires: httplib.h (cpp-httplib) in same folder.

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <fstream>
#include <sstream>
#include <httplib.h>

using namespace httplib;

std::atomic<uint64_t> total_requests{0}, total_success{0}, total_fail{0};

struct Config {
    std::string host = "localhost";
    int port = 8080;
    int clients = 10;
    int duration = 60; // seconds
    std::string workload = "get-popular"; // put-all, get-all, get-popular, mixed
    int hot_keys = 100;
};

static std::string make_key_unique(int client_id, uint64_t seq) {
    return "k_" + std::to_string(client_id) + "_" + std::to_string(seq);
}

void client_fn(int id, const Config &cfg, const std::vector<std::string> &key_pool, std::vector<int> &latencies_ms) {
    Client cli(cfg.host.c_str(), cfg.port);
    std::mt19937_64 rng(id + 123456);
    std::uniform_int_distribution<int> hot_dist(0, (int)key_pool.size()-1);
    std::uniform_int_distribution<int> opdist(1, 100);

    auto end_time = std::chrono::steady_clock::now() + std::chrono::seconds(cfg.duration);
    uint64_t seq = 0;
    while (std::chrono::steady_clock::now() < end_time) {
        std::string method, key, body;
        if (cfg.workload == "put-all") {
            method = "PUT";
            key = make_key_unique(id, seq++);
            body = "value_" + std::to_string(seq);
        } else if (cfg.workload == "get-all") {
            method = "GET";
            key = make_key_unique(id, seq++);
        } else if (cfg.workload == "get-popular") {
            method = "GET";
            key = key_pool[hot_dist(rng)];
        } else { // mixed
            int r = opdist(rng);
            if (r <= 70) { method = "GET"; key = key_pool[hot_dist(rng)]; }
            else if (r <= 90) { method = "PUT"; key = make_key_unique(id, seq++); body = "v_" + std::to_string(seq); }
            else { method = "DELETE"; key = key_pool[hot_dist(rng)]; }
        }

        auto t1 = std::chrono::steady_clock::now();
        bool ok = false;
        if (method == "PUT") {
            std::string path = "/kv?key=" + key;
            auto res = cli.Put(path.c_str(), body, "text/plain");
            if (res && res->status == 200) ok = true;
        } else if (method == "GET") {
            std::string path = "/kv?key=" + key;
            auto res = cli.Get(path.c_str());
            if (res && res->status == 200) ok = true;
        } else if (method == "DELETE") {
            std::string path = "/kv?key=" + key;
            auto res = cli.Delete(path.c_str());
            if (res && res->status == 200) ok = true;
        }
        auto t2 = std::chrono::steady_clock::now();
        int ms = (int)std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
        latencies_ms.push_back(ms);
        total_requests++;
        if (ok) total_success++; else total_fail++;
    }
}

int main(int argc, char** argv) {
    Config cfg;
    for (int i=1;i<argc;i++){
        std::string a = argv[i];
        if (a=="--host") cfg.host = argv[++i];
        else if (a=="--port") cfg.port = std::stoi(argv[++i]);
        else if (a=="--clients") cfg.clients = std::stoi(argv[++i]);
        else if (a=="--duration") cfg.duration = std::stoi(argv[++i]);
        else if (a=="--workload") cfg.workload = argv[++i];
        else if (a=="--hot") cfg.hot_keys = std::stoi(argv[++i]);
    }

    std::vector<std::string> key_pool;
    for (int i=0;i<cfg.hot_keys;i++) key_pool.push_back("hot_" + std::to_string(i));

    // Pre-create hot keys if needed
    if (cfg.workload == "get-popular" || cfg.workload == "mixed") {
        Client cli(cfg.host.c_str(), cfg.port);
        for (auto &k: key_pool) {
            std::string path = "/kv?key=" + k;
            cli.Put(path.c_str(), "initial_value", "text/plain");
        }
    }

    std::vector<std::thread> threads;
    std::vector<std::vector<int>> lat(cfg.clients);

    for (int i=0;i<cfg.clients;i++) {
        threads.emplace_back(client_fn, i, std::ref(cfg), std::ref(key_pool), std::ref(lat[i]));
    }
    for (auto &t : threads) t.join();

    // write latencies to CSV
    std::ofstream fout("latencies.csv");
    fout << "client,lat_ms\n";
    for (int i=0;i<cfg.clients;i++){
        for (auto v : lat[i]) fout << i << "," << v << "\n";
    }
    fout.close();

    std::cout << "total_requests=" << total_requests.load()
              << " success=" << total_success.load()
              << " fail=" << total_fail.load() << "\n";
    return 0;
}
