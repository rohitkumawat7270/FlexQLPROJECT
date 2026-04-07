#include "flexql.h"
#include "common/protocol.h"

#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <string>
#include <vector>

using namespace flexql::proto;

struct FlexQL { int fd = -1; };

struct NetResult {
    bool ok = true;
    std::string error;
    std::vector<std::string> col_names;
    struct Row { std::vector<std::string> values; };
    std::vector<Row> rows;
};

static bool recv_result(int fd, NetResult &res) {
    std::string line;

    if (!recv_line(fd, line)) return false;
    if (line == "ERR") {
        res.ok = false;
        std::string tag;
        if (!recv_line(fd, tag))       return false;
        if (!recv_line(fd, res.error)) return false;
        recv_line(fd, tag);
        return true;
    }
    if (line != "OK") return false;

    if (!recv_line(fd, line) || line != "COLS") return false;
    if (!recv_line(fd, line)) return false;
    int ncols = std::stoi(line);
    res.col_names.reserve(ncols);
    for (int i = 0; i < ncols; ++i) {
        std::string col;
        if (!recv_line(fd, col)) return false;
        res.col_names.push_back(std::move(col));
    }

    if (!recv_line(fd, line) || line != "ROWS") return false;
    if (!recv_line(fd, line)) return false;
    int nrows = std::stoi(line);
    res.rows.reserve(nrows);

    for (int i = 0; i < nrows; ++i) {
        if (!recv_line(fd, line) || line != "NVALS") return false;
        if (!recv_line(fd, line)) return false;
        int nvals = std::stoi(line);

        NetResult::Row row;
        row.values.reserve(nvals);
        for (int j = 0; j < nvals; ++j) {
            std::string val;
            if (!recv_line(fd, val)) return false;
            row.values.push_back(std::move(val));
        }
        if (!recv_line(fd, line)) return false; /* expiration */
        res.rows.push_back(std::move(row));
    }
    recv_line(fd, line); /* END */
    return true;
}

extern "C" {

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;

    struct addrinfo hints{}, *res0 = nullptr;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", port);
    if (getaddrinfo(host, port_str, &hints, &res0) != 0 || !res0)
        return FLEXQL_ERROR;

    int fd = -1;
    for (struct addrinfo *r = res0; r; r = r->ai_next) {
        fd = socket(r->ai_family, r->ai_socktype, r->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, r->ai_addr, r->ai_addrlen) == 0) break;
        close(fd); fd = -1;
    }
    freeaddrinfo(res0);
    if (fd < 0) return FLEXQL_ERROR;

    int nodelay = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    int bufsize = 512 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));

    *db = new FlexQL();
    (*db)->fd = fd;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    if (db->fd >= 0) {
        send_all(db->fd, "QUIT\n");
        close(db->fd);
        db->fd = -1;
    }
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db,
                const char *sql,
                int (*callback)(void*, int, char**, char**),
                void *arg,
                char **errmsg)
{
    if (!db || db->fd < 0) {
        if (errmsg) *errmsg = strdup("Invalid database handle");
        return FLEXQL_ERROR;
    }
    if (!sql) {
        if (errmsg) *errmsg = strdup("NULL SQL string");
        return FLEXQL_ERROR;
    }

    size_t sql_len = strlen(sql);
    std::string msg;
    msg.reserve(sql_len + 12);
    msg  = std::to_string(sql_len);
    msg += '\n';
    msg.append(sql, sql_len);

    if (!send_all(db->fd, msg)) {
        if (errmsg) *errmsg = strdup("Network send failed");
        return FLEXQL_ERROR;
    }

    NetResult res;
    if (!recv_result(db->fd, res)) {
        if (errmsg) *errmsg = strdup("Failed to receive server response");
        return FLEXQL_ERROR;
    }
    if (!res.ok) {
        if (errmsg) *errmsg = strdup(res.error.c_str());
        return FLEXQL_ERROR;
    }

    if (callback && !res.rows.empty()) {
        int ncols = (int)res.col_names.size();
        std::vector<const char*> col_ptrs, val_ptrs;
        col_ptrs.reserve(ncols);
        val_ptrs.reserve(ncols);
        for (auto &c : res.col_names) col_ptrs.push_back(c.c_str());

        for (auto &row : res.rows) {
            val_ptrs.clear();
            for (auto &v : row.values) val_ptrs.push_back(v.c_str());
            while ((int)val_ptrs.size() < ncols) val_ptrs.push_back(nullptr);
            int ret = callback(arg, ncols,
                               const_cast<char**>(val_ptrs.data()),
                               const_cast<char**>(col_ptrs.data()));
            if (ret != 0) break;
        }
    }
    return FLEXQL_OK;
}

void flexql_free(void *ptr) { free(ptr); }

} 
