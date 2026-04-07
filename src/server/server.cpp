#include <iostream>
#include <string>
#include <thread>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <signal.h>
#include <fcntl.h>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <filesystem>

#include "common/types.h"
#include "common/protocol.h"
#include "storage/database.h"
#include "storage/wal.h"
#include "storage/storage_engine.h"
#include "query/executor.h"

using namespace flexql;
using namespace flexql::proto;

static Database        g_db;
static WAL              *g_wal     = nullptr;
static StorageEngine    *g_storage = nullptr;
static int       g_server_fd = -1;
static int       g_epoll_fd = -1;

// --- Thread Pool ---
class ThreadPool {
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    bool stop;
public:
    ThreadPool(size_t threads) : stop(false) {
        for(size_t i = 0; i < threads; ++i)
            workers.emplace_back([this] {
                while(true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this]{ return this->stop || !this->tasks.empty(); });
                        if(this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
    }
    template<class F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one();
    }
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for(std::thread &worker: workers) worker.join();
    }
};

static ThreadPool* g_pool = nullptr;

static void shutdown_handler(int) {
    std::cout << "\n[INFO] Shutting down — saving data to disk...\n";

    if (g_storage) {
        auto schemas = g_storage->load_all_schemas();
        for (const auto &s : schemas)
            g_storage->compact_table(s.name);
    }

    if (g_wal) {
        g_wal->flush_and_sync();
        g_wal->truncate();
        delete g_wal;
        g_wal = nullptr;
    }

    if (g_server_fd >= 0) close(g_server_fd);
    if (g_epoll_fd >= 0) close(g_epoll_fd);
    std::cout << "[INFO] All data saved to data/tables/\n";
    exit(0);
}

// Blocking send with EAGAIN handling
static bool send_full(int fd, const std::string &data) {
    size_t total = 0;
    while (total < data.size()) {
        ssize_t n = write(fd, data.c_str() + total, data.size() - total);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(50);
                continue;
            }
            return false;
        }
        total += n;
    }
    return true;
}

static void send_result(int fd, const QueryResult &res) {
    std::string out;
    out.reserve(128);

    if (!res.ok) {
        out  = STATUS_ERR + "\n";
        out += TAG_ERRMSG + "\n";
        out += res.error  + "\n";
        out += TAG_END    + "\n";
        send_full(fd, out);
        return;
    }

    out = STATUS_OK + "\n";
    out += TAG_COLS + "\n";
    out += std::to_string(res.column_names.size()) + "\n";
    for (const auto &c : res.column_names) { out += c; out += "\n"; }

    out += TAG_ROWS + "\n";
    out += std::to_string(res.rows.size()) + "\n";
    for (const auto &row : res.rows) {
        out += TAG_NVALS + "\n";
        out += std::to_string(row.values.size()) + "\n";
        for (const auto &v : row.values) { out += v; out += "\n"; }
        out += std::to_string((long long)row.expiration) + "\n";
    }
    out += TAG_END + "\n";
    send_full(fd, out);
}

// Client Context
struct ClientContext {
    int fd;
    std::string ip;
    std::string buffer;
};

static std::unordered_map<int, ClientContext*> clients;

static void close_client(int fd) {
    auto it = clients.find(fd);
    if (it != clients.end()) {
        std::cout << "[INFO] Client disconnected: " << it->second->ip << "\n";
        delete it->second;
        clients.erase(it);
    }
    close(fd);
}

static void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void process_buffer(ClientContext* client) {
    while (true) {
        size_t nl_pos = client->buffer.find('\n');
        if (nl_pos == std::string::npos) break;
        
        std::string len_str = client->buffer.substr(0, nl_pos);
        if (len_str == "QUIT") {
            close_client(client->fd);
            return;
        }

        long long len = 0;
        try { len = std::stoll(len_str); }
        catch (...) { close_client(client->fd); return; }

        if (len <= 0 || len > 64 * 1024 * 1024) {
            close_client(client->fd);
            return;
        }

        if (client->buffer.size() < nl_pos + 1 + len) {
            break; // not enough bytes yet
        }

        std::string sql = client->buffer.substr(nl_pos + 1, len);
        client->buffer.erase(0, nl_pos + 1 + len);

        int cfd = client->fd;
        g_pool->enqueue([cfd, sql]() {
            Executor exec(g_db);
            QueryResult result = exec.execute(sql);
            send_result(cfd, result);
        });
    }
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT,  shutdown_handler);
    signal(SIGTERM, shutdown_handler);

    int port = 9000;
    if (argc >= 2) port = atoi(argv[1]);

    std::filesystem::create_directories("data/tables");
    std::filesystem::create_directories("data/wal");

    g_storage = new StorageEngine("data/tables");
    g_db.set_storage(g_storage);
    g_db.load_from_disk();               

    const std::string wal_path = "data/wal/flexql.wal";
    WAL::replay(wal_path, g_db);
    g_wal = new WAL(wal_path);
    g_db.set_wal(g_wal);
    std::cout << "[WAL] Crash recovery enabled: " << wal_path << "\n";

    g_pool = new ThreadPool(std::thread::hardware_concurrency());

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    set_nonblocking(server_fd);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#ifdef SO_REUSEPORT
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(server_fd, 128) < 0) {
        perror("listen"); return 1;
    }

    g_server_fd = server_fd;
    std::cout << "FlexQL server listening on port " << port << " (epoll mode)\n";

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) { perror("epoll_create1"); return 1; }
    g_epoll_fd = epoll_fd;

    epoll_event event{};
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event);

    constexpr int MAX_EVENTS = 1000;
    epoll_event events[MAX_EVENTS];

    while (true) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == server_fd) {
                while (true) {
                    sockaddr_in client_addr{};
                    socklen_t clen = sizeof(client_addr);
                    int client_fd = accept(server_fd, (sockaddr *)&client_addr, &clen);
                    
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    
                    set_nonblocking(client_fd);
                    int nodelay = 1;
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

                    int bufsize = 512 * 1024;
                    setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
                    setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

                    char ip_buf[INET_ADDRSTRLEN] = {};
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));

                    ClientContext* ctx = new ClientContext();
                    ctx->fd = client_fd;
                    ctx->ip = ip_buf;
                    clients[client_fd] = ctx;

                    epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLET;
                    client_ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev);

                    std::cout << "[INFO] Client connected: " << ctx->ip << "\n";
                }
            } else {
                int client_fd = events[i].data.fd;
                ClientContext* ctx = clients[client_fd];
                if (!ctx) continue;

                if (events[i].events & EPOLLIN) {
                    char buf[65536];
                    while (true) {
                        ssize_t count = read(client_fd, buf, sizeof(buf));
                        if (count < 0) {
                            if (errno != EAGAIN) {
                                close_client(client_fd);
                            }
                            break;
                        } else if (count == 0) {
                            close_client(client_fd);
                            break;
                        } else {
                            ctx->buffer.append(buf, count);
                        }
                    }
                    if (clients.count(client_fd)) {
                        process_buffer(ctx);
                    }
                }
                
                if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                    close_client(client_fd);
                }
            }
        }
    }
    
    return 0;
}
