// loadgen.cpp
// Usage: ./loadgen <num_clients> <duration_sec> <workload>
// Workload types: put_all | get_all | get_popular | mixed | delete_all
// Example: ./loadgen 10 30 mixed

#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <atomic>

using namespace std;
using namespace std::chrono;

atomic<uint64_t> total_requests(0);
atomic<uint64_t> total_success(0);
atomic<uint64_t> total_fail(0);
atomic<uint64_t> total_latency_us(0);

// build HTTP request strings
string make_http_request(const string &method, const string &path, const string &host, const string &body = "") {
    string req = method + " " + path + " HTTP/1.1\r\n";
    req += "Host: " + host + "\r\n";
    req += "Connection: keep-alive\r\n";
    if (!body.empty()) {
        req += "Content-Type: text/plain\r\n";
        req += "Content-Length: " + to_string(body.size()) + "\r\n";
    }
    req += "\r\n";
    if (!body.empty()) req += body;
    return req;
}

// read full response (headers + body if content-length present). returns true if any bytes read.
bool read_http_response(int sock, string &out) {
    out.clear();
    char buf[8192];
    ssize_t n;
    size_t total = 0;
    // read loop: try to read until headers parsed and Content-Length satisfied, or until socket has no more data for a short while
    // We'll do a few recv calls until we detect end-of-headers and content-length satisfied
    int rounds = 0;
    while (rounds++ < 20) {
        n = recv(sock, buf, sizeof(buf), 0);
        if (n < 0) {
            return false;
        } else if (n == 0) {
            // connection closed by peer
            break;
        } else {
            out.append(buf, (size_t)n);
            total += (size_t)n;
            // check if we have headers
            size_t pos = out.find("\r\n\r\n");
            if (pos != string::npos) {
                // try to find content-length
                string headers = out.substr(0, pos + 4);
                string lower = headers;
                for (auto &c : lower) c = tolower((unsigned char)c);
                size_t clpos = lower.find("content-length:");
                if (clpos != string::npos) {
                    size_t line_end = lower.find("\r\n", clpos);
                    string clline = lower.substr(clpos, line_end - clpos);
                    size_t colon = clline.find(':');
                    if (colon != string::npos) {
                        string num = clline.substr(colon + 1);
                        // trim
                        size_t s = num.find_first_not_of(" \t");
                        size_t e = num.find_last_not_of(" \t");
                        if (s != string::npos && e != string::npos && e >= s) num = num.substr(s, e - s + 1);
                        else num = "";
                        size_t content_len = 0;
                        if (!num.empty()) {
                            try { content_len = stoul(num); } catch (...) { content_len = 0; }
                        }
                        size_t body_len = out.size() - (pos + 4);
                        if (body_len >= content_len) return true;
                        // else continue reading
                    } else {
                        // malformed header: return what we have
                        return true;
                    }
                } else {
                    // no content-length -> return headers+any body so far
                    return true;
                }
            }
        }
        // small short sleep to allow more bytes to arrive in case of TCP segmentation
        this_thread::sleep_for(milliseconds(1));
    }
    // if we exited loop, return whether we got any bytes
    return !out.empty();
}

void client_thread(int id, int test_duration, const string &workload_type, int total_keys, const string &server_host, int server_port) {
    // random generator per thread
    mt19937_64 gen((uint64_t)steady_clock::now().time_since_epoch().count() ^ (uint64_t)id * 0x9e3779b97f4a7c15ULL);
    uniform_int_distribution<int> key_dist(1, total_keys);

    sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(server_port);
    inet_pton(AF_INET, server_host.c_str(), &serv.sin_addr);

    // create persistent socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return;
    }
    if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) {
        perror("connect");
        close(sock);
        return;
    }

    auto end_time = steady_clock::now() + seconds(test_duration);

    while (steady_clock::now() < end_time) {
        int key = key_dist(gen);
        string method, path, body;

        if (workload_type == "put_all") {
            method = "PUT";
            path = "/kv?key=" + to_string(key);
            body = "val" + to_string(key);
        } else if (workload_type == "get_all") {
            method = "GET";
            path = "/kv?key=" + to_string(key);
        } else if (workload_type == "get_popular") {
            // popular keys in small set 1..100
            int popular_key = (key % 100) + 1;
            method = "GET";
            path = "/kv?key=" + to_string(popular_key);
        } else if (workload_type == "mixed") {
            // mixed distribution: 70% GET, 20% PUT, 10% DELETE (aligning with server tests)
            int r = (int)(gen() % 100);
            if (r < 70) {
                method = "GET";
                path = "/kv?key=" + to_string(key);
            } else if (r < 90) {
                method = "PUT";
                path = "/kv?key=" + to_string(key);
                body = "val" + to_string(key);
            } else {
                method = "DELETE";
                path = "/kv?key=" + to_string(key);
            }
        } else if (workload_type == "delete_all") {
            method = "DELETE";
            path = "/kv?key=" + to_string(key);
        } else {
            // default to GET_ALL if unknown
            method = "GET";
            path = "/kv?key=" + to_string(key);
        }

        string request = make_http_request(method, path, server_host, body);

        auto start = steady_clock::now();
        ssize_t sent = send(sock, request.c_str(), request.size(), 0);
        if (sent <= 0) {
            // connection likely dropped; attempt reconnect once
            total_fail++;
            close(sock);
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { perror("socket"); break; }
            if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) { perror("reconnect failed"); close(sock); break; }
            continue;
        }

        string response;
        bool ok = read_http_response(sock, response);
        auto end = steady_clock::now();
        long long latency = duration_cast<microseconds>(end - start).count();
        total_latency_us += (uint64_t)latency;
        total_requests++;

        if (!ok) {
            total_fail++;
            // try to reconnect and continue
            close(sock);
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { perror("socket"); break; }
            if (connect(sock, (sockaddr*)&serv, sizeof(serv)) < 0) { perror("reconnect failed"); close(sock); break; }
            continue;
        }

        // parse status code from response status line
        bool success = false;
        size_t pos = response.find("HTTP/1.1 ");
        if (pos != string::npos && response.size() > pos + 12) {
            // expecting "HTTP/1.1 XXX"
            int code = 0;
            try {
                code = stoi(response.substr(pos + 9, 3));
            } catch (...) { code = 0; }
            if (code >= 200 && code < 300) success = true;
        } else {
            // fallback: if we see "200" anywhere, treat as success
            if (response.find("200") != string::npos) success = true;
        }

        if (success) total_success++;
        else total_fail++;
    }

    close(sock);
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        cout << "Usage: ./loadgen <num_clients> <duration_sec> <workload>\n";
        cout << "Workloads: put_all | get_all | get_popular | mixed | delete_all\n";
        return 1;
    }

    int num_clients = stoi(argv[1]);
    int duration = stoi(argv[2]);
    string workload = argv[3];
    int total_keys = 100000; // keyspace
    string server_host = "127.0.0.1";
    int server_port = 8080;

    cout << "Starting load test with " << num_clients << " persistent clients for "
         << duration << " seconds (" << workload << " workload)\n";

    auto start_time = steady_clock::now();

    vector<thread> clients;
    clients.reserve(num_clients);
    for (int i = 0; i < num_clients; ++i) {
        clients.emplace_back(client_thread, i, duration, workload, total_keys, server_host, server_port);
    }

    for (auto &t : clients) t.join();

    auto end_time = steady_clock::now();
    double total_time = std::chrono::duration<double>(end_time - start_time).count();


    uint64_t total = total_requests.load();
    double avg_latency_ms = (total == 0) ? 0.0 : (total_latency_us.load() / 1000.0) / (double)total;
    double throughput = total_success.load() / max(1.0, total_time);

    cout << "\n--- Load Test Results ---\n";
    cout << "Total Requests (attempted): " << total << "\n";
    cout << "Success (HTTP 2xx): " << total_success.load() << "\n";
    cout << "Failed (non-2xx or network): " << total_fail.load() << "\n";
    cout << "Average Latency (ms): " << avg_latency_ms << " ms\n";
    cout << "Throughput (successful req/s): " << throughput << " req/sec\n";

    return 0;
}
