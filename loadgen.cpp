#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <random>

using namespace std;

atomic<int> total_requests(0);
atomic<long long> total_latency_us(0);

string make_request(const string &method, const string &path,
                    const string &body = "")
{
    string req = method + " " + path + " HTTP/1.1\r\n";
    req += "Host: 127.0.0.1\r\n";
    req += "Connection: keep-alive\r\n";
    req += "Content-Type: application/json\r\n";

    if (!body.empty())
        req += "Content-Length: " + to_string(body.size()) + "\r\n";

    req += "\r\n";
    if (!body.empty())
        req += body;

    return req;
}


bool read_http_response(int sock)
{
    char buf[8192];
    string header;

    //  Read headers
    while (true)
    {
        ssize_t n = recv(sock, buf, sizeof(buf), 0);
        if (n <= 0)
            return false;

        header.append(buf, buf + n);

        size_t pos = header.find("\r\n\r\n");
        if (pos != string::npos){
            size_t content_len = 0;
            size_t cl = header.find("Content-Length:");
            if (cl != string::npos)
            {
                size_t end = header.find("\r\n", cl);
                string num = header.substr(cl + 15, end - (cl + 15));
                content_len = stoi(num);
            }

            size_t consumed = header.size() - (pos + 4);

            size_t need = (consumed >= content_len) ? 0 : (content_len - consumed);


            while (need > 0)
            {
                ssize_t x = recv(sock, buf, min(sizeof(buf), need), 0);
                if (x <= 0)
                    return false;
                need -= x;
            }

            break;
        }
    }

    return true;
}


void client_thread(int id, int duration, const string &workload, int total_keys)
{
    default_random_engine gen(id + time(nullptr));
    uniform_int_distribution<int> key_dist(1, total_keys);

    sockaddr_in serv{};
    serv.sin_family = AF_INET;
    serv.sin_port = htons(8000);
    inet_pton(AF_INET, "127.0.0.1", &serv.sin_addr);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return;

    if (connect(sock, (sockaddr *)&serv, sizeof(serv)) < 0)
    {
        perror("connect");
        close(sock);
        return;
    }

    auto end_time = chrono::steady_clock::now() + chrono::seconds(duration);

    while (chrono::steady_clock::now() < end_time)
    {
        int key = key_dist(gen);
        string method, path, body;

        // Workload s
        if (workload == "put_all"){
            method = "POST";
            path = "/create";
            body = "{\"key\":" + to_string(key) + ",\"value\":\"val" + to_string(key) + "\"}";
        }
        else if (workload == "get_all"){
            method = "GET";
            path = "/read/" + to_string(key);
        }
        else if (workload == "get_popular"){
            method = "GET";
            key = (key % 100) + 1;
            path = "/read/" + to_string(key);
        }
        else if (workload == "mixed"){
            int r = key % 3;
            if (r == 0){
                method = "POST";
                path = "/create";
                body = "{\"key\":" + to_string(key) + ",\"value\":\"val" + to_string(key) + "\"}";
            }
            else if (r == 1){
                method = "GET";
                path = "/read/" + to_string(key);
            }
            else{
                method = "DELETE";
                path = "/delete/" + to_string(key);
            }
        }
        else if (workload == "delete_all"){
            method = "DELETE";
            path = "/delete/" + to_string(key);
        }
        else
        {
            cerr << "Invalid workload\n";
            return;
        }

        
        string req = make_request(method, path, body);

        auto t0 = chrono::steady_clock::now();

        if (send(sock, req.c_str(), req.size(), 0) <= 0)
        {
            close(sock);
            sock = socket(AF_INET, SOCK_STREAM, 0);
            connect(sock, (sockaddr *)&serv, sizeof(serv));
            continue;
        }

        if (!read_http_response(sock))
        {
            cerr << "[Thread " << id << "] Broken connection, reconnecting...\n";
            close(sock);
            sock = socket(AF_INET, SOCK_STREAM, 0);
            connect(sock, (sockaddr *)&serv, sizeof(serv));
            continue;
        }

        auto t1 = chrono::steady_clock::now();
        long long latency = chrono::duration_cast<chrono::microseconds>(t1 - t0).count();

        total_latency_us += latency;
        total_requests++;
    }

    close(sock);
}

// Main code
int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        cout << "Usage: ./loadgen <num_clients> <duration_sec> <workload>\n";
        cout << "Workload types: put_all | get_all | get_popular | mixed | delete_all\n";
        return 1;
    }

    int num_clients = stoi(argv[1]);
    int duration = stoi(argv[2]);
    string workload = argv[3];

    cout << "Number of clients = " << num_clients << ", duration = "
         << duration << " seconds for workload: " << workload << endl;

    vector<thread> threads;

    for (int i = 0; i < num_clients; i++)
        threads.emplace_back(client_thread, i, duration, workload, 100000);

    for (auto &t : threads)
        t.join();

    double avg_latency_ms = (total_latency_us.load() / 1000.0) /
                            max(total_requests.load(), 1);
    double throughput = total_requests.load() / (double)duration;

    cout << "\n------------ Metrics------------\n";
    cout << "Number of Requests: " << total_requests << endl;
    cout << "Average Latency: " << avg_latency_ms << " ms\n";
    cout << "Throughput: " << throughput << " req/s\n";

    return 0;
}