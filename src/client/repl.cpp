#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <string>
#include <unistd.h>
#include <termios.h>
#include <sys/select.h>
#include "flexql.h"


static struct termios g_saved;
static bool g_raw = false;

static void term_restore() {
    if (g_raw) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &g_saved);
        g_raw = false;
    }
}

static bool term_raw() {
    if (!isatty(STDIN_FILENO)) return false;
    if (tcgetattr(STDIN_FILENO, &g_saved) < 0) return false;
    atexit(term_restore);

    struct termios t = g_saved;
    t.c_lflag &= ~(ECHO | ICANON | ISIG | IEXTEN);
    t.c_iflag &= ~(IXON | ICRNL | BRKINT | INPCK | ISTRIP);
    t.c_cflag |= CS8;
    t.c_oflag &= ~OPOST;
    t.c_cc[VMIN]  = 1;
    t.c_cc[VTIME] = 0;

    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &t) < 0) return false;
    g_raw = true;
    return true;
}

/* Write bytes to stdout (raw mode safe) */
static void out(const char *s, size_t n) {
    while (n > 0) {
        ssize_t w = write(STDOUT_FILENO, s, n);
        if (w <= 0) break;
        s += w; n -= w;
    }
}
static void outs(const char *s) { out(s, strlen(s)); }
static void outc(char c)        { out(&c, 1); }
static void outnl() { outs("\r\n"); }


static bool readline_raw(std::string &out_line, bool &cancelled) {
    out_line.clear();
    cancelled = false;

    while (true) {
        char c;
        ssize_t n = read(STDIN_FILENO, &c, 1);
        if (n <= 0) return false;

        if (c == '\r' || c == '\n') { outnl(); return true; }
        if (c == 3) { cancelled = true; outnl(); return true; }
        if (c == 4) { if (out_line.empty()) return false; continue; }
        if (c == 127 || c == 8) {
            if (!out_line.empty()) { out_line.pop_back(); outs("\b \b"); }
            continue;
        }
        if (c < 32) continue;

        out_line += c;
        outc(c);
    }
}


static int print_row(void*, int ncols, char **values, char **cols) {
    for (int i = 0; i < ncols; i++) {
        char buf[1024];
        int len = snprintf(buf, sizeof(buf), "%s = %s\r\n",
                           cols[i]   ? cols[i]   : "?",
                           values[i] ? values[i] : "NULL");
        out(buf, len);
    }
    outnl();
    return 0;
}

static void execute(FlexQL *db, const std::string &sql) {
    char *errmsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), print_row, nullptr, &errmsg);
    if (rc != FLEXQL_OK) {
        char buf[2048];
        snprintf(buf, sizeof(buf), "SQL error: %s\r\n",
                 errmsg ? errmsg : "unknown error");
        outs(buf);
        flexql_free(errmsg);
    }
}


static std::string to_upper(const std::string &s) {
    std::string r = s;
    for (char &c : r) c = (char)toupper((unsigned char)c);
    return r;
}

static std::string trim(const std::string &s) {
    const char *ws = " \t\r\n\v\f";
    size_t a = s.find_first_not_of(ws);
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(ws);
    return s.substr(a, b - a + 1);
}


static bool is_sql_start(const std::string &line) {
    std::string u = to_upper(trim(line));
    if (u.empty()) return false;
    /* split on the first whitespace or '(' so "SELECT(" also works */
    size_t sp = u.find_first_of(" \t(");
    std::string kw = (sp == std::string::npos) ? u : u.substr(0, sp);
    return (kw == "CREATE" || kw == "INSERT" || kw == "SELECT" || kw == "DROP" || kw == "DELETE");
}


static void run_interactive(FlexQL *db) {
    std::string pending;

    while (true) {
        if (pending.empty()) outs("flexql> ");
        else                 outs("     -> ");

        std::string line;
        bool cancelled = false;
        if (!readline_raw(line, cancelled)) { outnl(); break; }

        if (cancelled) {
            if (!pending.empty()) { outs("[cancelled]\r\n"); pending.clear(); }
            continue;
        }

        std::string t = trim(line);
        if (t.empty()) continue;

        std::string u = to_upper(t);

        if (pending.empty()) {
            /* Meta-commands only recognised at statement start */
            if (u == ".EXIT" || u == "EXIT" || u == ".QUIT" || u == "QUIT") break;
            if (u == ".CLEAR") { pending.clear(); continue; }
            if (u == ".HELP") {
                outs("Commands end with ';'\r\n"
                     "  CREATE TABLE t (col TYPE, ...);\r\n"
                     "  INSERT INTO t VALUES (v1, v2, ...);\r\n"
                     "  SELECT * FROM t;\r\n"
                     "  SELECT * FROM t WHERE col = val;\r\n"
                     "  SELECT * FROM a INNER JOIN b ON a.c = b.c;\r\n"
                     "  .exit   exit\r\n"
                     "  .clear  cancel current input\r\n"
                     "  Ctrl+C  cancel current input\r\n"
                     "Note: use single quotes 'Alice' not \"Alice\"\r\n\r\n");
                continue;
            }

            if (!is_sql_start(t)) {
                char buf[512];
                snprintf(buf, sizeof(buf),
                         "Not a SQL statement (ignored): %.60s\r\n", t.c_str());
                outs(buf);
                continue;
            }
        } else {
            if (u == ".CLEAR") {
                outs("[cancelled]\r\n");
                pending.clear();
                continue;
            }
        }


        if (!pending.empty()) pending += " ";
        pending += t;


        while (true) {
            size_t semi = pending.find(';');
            if (semi == std::string::npos) break;

            std::string stmt = trim(pending.substr(0, semi));
            pending = trim(pending.substr(semi + 1));
            if (stmt.empty()) continue;

            std::string su = to_upper(stmt);
            if (su == "EXIT" || su == "QUIT") {
                term_restore();
                goto done;
            }

            execute(db, stmt);

            if (!pending.empty()) outs("flexql> ");
        }
    }
done:;
}

static void run_pipe(FlexQL *db) {
    char buf[65536];
    std::string pending;
    long long ok = 0, err = 0;

    while (fgets(buf, sizeof(buf), stdin)) {
        std::string t = trim(std::string(buf));
        if (t.empty()) continue;

        if (pending.empty() && !is_sql_start(t)) continue;

        if (!pending.empty()) pending += " ";
        pending += t;

        while (pending.find(';') != std::string::npos) {
            size_t s = pending.find(';');
            std::string stmt = trim(pending.substr(0, s));
            pending = trim(pending.substr(s + 1));
            if (stmt.empty()) continue;
            if (to_upper(stmt) == "EXIT" || to_upper(stmt) == "QUIT") return;

            if (!is_sql_start(stmt)) {
                fprintf(stderr, "[WARN] Skipping non-SQL statement: %.80s\n",
                        stmt.c_str());
                err++;
                continue;
            }

            char *em = nullptr;
            if (flexql_exec(db, stmt.c_str(), nullptr, nullptr, &em) != FLEXQL_OK) {
                fprintf(stderr, "SQL error: %s\n", em ? em : "?");
                flexql_free(em); err++;
            } else {
                ok++;
                if (ok % 1000 == 0)
                    fprintf(stderr, "[progress] %lld OK\n", ok);
            }
        }
    }
    fprintf(stderr, "Done: %lld OK, %lld errors\n", ok, err);
}


int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 9000;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = atoi(argv[2]);

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to %s:%d\n"
                        "Start server: ./bin/flexql-server %d\n",
                host, port, port);
        return 1;
    }

    bool tty = isatty(STDIN_FILENO);

    printf("Connected to FlexQL server\n\n");
    fflush(stdout);

    if (tty) {
        term_raw();
        run_interactive(db);
        term_restore();
    } else {
        run_pipe(db);
    }

    flexql_close(db);
    printf("Connection closed\n");
    return 0;
}
