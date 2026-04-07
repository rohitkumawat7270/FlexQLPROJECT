#include "common/protocol.h"
#include <unistd.h>
#include <sys/socket.h>
#include <cerrno>
#include <cstring>

namespace flexql {
namespace proto {

bool send_all(int fd, const std::string &data) {
    size_t total = 0;
    const char *ptr = data.c_str();
    size_t len = data.size();
    while (total < len) {
        ssize_t n = write(fd, ptr + total, len - total);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

/*
 * Per-thread, per-fd read buffer.
 *
 * BOTH recv_line and recv_n draw from the same buffer so that greedy
 * reads by recv_line never cause recv_n to block re-reading bytes that
 * are already buffered.
 *
 * The server protocol is:
 *   recv_line  → reads the LENGTH token  (e.g. "47\n")
 *   recv_n     → reads exactly LENGTH bytes of SQL body
 *
 * With a 65536-byte buffer, recv_line's first read() pulls in the
 * length line AND the SQL body in one syscall.  recv_n must then
 * consume those buffered bytes instead of calling read() again.
 */
struct RecvBuf {
    char   data[65536];
    size_t pos     = 0;
    size_t len     = 0;
    int    last_fd = -1;
};

static thread_local RecvBuf g_buf;

/* Return the thread-local buffer, reset if fd changed */
static RecvBuf &get_buf(int fd) {
    if (g_buf.last_fd != fd) {
        g_buf.pos = g_buf.len = 0;
        g_buf.last_fd = fd;
    }
    return g_buf;
}

bool recv_line(int fd, std::string &line) {
    RecvBuf &buf = get_buf(fd);
    line.clear();

    while (true) {
        /* Refill if empty */
        if (buf.pos >= buf.len) {
            ssize_t n = read(fd, buf.data, sizeof(buf.data));
            if (n <= 0) return false;
            buf.pos = 0;
            buf.len = (size_t)n;
        }

        const char *start = buf.data + buf.pos;
        const char *end   = buf.data + buf.len;
        const char *nl    = (const char *)memchr(start, '\n', end - start);

        if (nl) {
            line.append(start, nl - start);
            buf.pos = (size_t)(nl - buf.data) + 1;
            return true;
        }
        line.append(start, end - start);
        buf.pos = buf.len;
    }
}

/*
 * recv_n: read exactly n bytes — MUST drain from the shared buffer
 * first before calling read() so it stays consistent with recv_line.
 */
bool recv_n(int fd, char *out, size_t n) {
    RecvBuf &buf = get_buf(fd);
    size_t total = 0;

    /* 1. Drain whatever is already buffered */
    if (buf.pos < buf.len) {
        size_t avail = buf.len - buf.pos;
        size_t take  = avail < n ? avail : n;
        memcpy(out, buf.data + buf.pos, take);
        buf.pos += take;
        total   += take;
    }

    /* 2. Read remaining bytes directly from socket */
    while (total < n) {
        ssize_t r = read(fd, out + total, n - total);
        if (r <= 0) return false;
        total += r;
    }
    return true;
}

} // proto
} // flexql
