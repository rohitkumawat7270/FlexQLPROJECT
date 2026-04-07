/*
 * FlexQL Benchmark — 10M rows, single connection, frequent progress output
 */
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <chrono>
#include "flexql.h"

using Clock = std::chrono::steady_clock;
static long long ms_since(Clock::time_point t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               Clock::now() - t0).count();
}
static int null_cb(void*, int, char**, char**) { return 0; }
static int count_cb(void *d, int, char**, char**) {
    (*(long long *)d)++; return 0;
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1]        : "127.0.0.1";
    int         port = argc > 2 ? atoi(argv[2])  : 9000;
    long long   rows = argc > 3 ? atoll(argv[3]) : 10000000LL;

    printf("=======================================================\n");
    printf(" FlexQL 10M Benchmark\n");
    printf("=======================================================\n");
    printf("  Rows   : %lld\n", rows);
    printf("  Server : %s:%d\n\n", host, port);

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to %s:%d\n", host, port);
        return 1;
    }
    printf("Connected.\n\n");

    /* ── Setup ── */
    char *em = nullptr;
    flexql_exec(db, "DROP TABLE BENCH;", null_cb, nullptr, &em);
    flexql_free(em); em = nullptr;

    if (flexql_exec(db,
            "CREATE TABLE BENCH"
            " (ID INT PRIMARY KEY NOT NULL,"
            "  NAME VARCHAR NOT NULL,"
            "  SCORE DECIMAL NOT NULL);",
            null_cb, nullptr, &em) != FLEXQL_OK) {
        printf("CREATE error: %s\n", em);
        flexql_free(em); flexql_close(db); return 1;
    }
    printf("Table created. Starting inserts...\n\n");
    fflush(stdout);

    /* ── INSERT loop ── */
    long long errors = 0;
    char sql[256];
    auto t0  = Clock::now();
    auto lap = t0;

    for (long long i = 1; i <= rows; ++i) {
        snprintf(sql, sizeof(sql),
            "INSERT INTO BENCH VALUES (%lld,'User%lld',%lld.%lld);",
            i, i, i % 100LL, i % 10LL);

        if (flexql_exec(db, sql, null_cb, nullptr, &em) != FLEXQL_OK) {
            errors++;
            if (errors <= 3)
                printf("  [!] row %lld error: %s\n", i, em);
            flexql_free(em); em = nullptr;
        }

        /* Print progress every 100k rows */
        if (i % 100000 == 0) {
            long long lap_ms = ms_since(lap);
            double rate = lap_ms > 0
                ? 100000.0 * 1000.0 / (double)lap_ms : 0;
            printf("  %8lld / %lld  |  last 100k: %4lld ms  |  %.0f rows/s\n",
                   i, rows, lap_ms, rate);
            fflush(stdout);
            lap = Clock::now();
        }
    }
    long long insert_ms = ms_since(t0);

    /* ── B+-tree PK lookups ── */
    printf("\nRunning 1000 B+-tree PK lookups...\n");
    long long mid = rows / 2;
    snprintf(sql, sizeof(sql), "SELECT * FROM BENCH WHERE ID = %lld;", mid);
    long long pk_count = 0;
    auto t3 = Clock::now();
    for (int rep = 0; rep < 1000; ++rep)
        flexql_exec(db, sql, count_cb, &pk_count, nullptr);
    long long pk_ms = ms_since(t3);

    /* ── Full scan with name filter ── */
    printf("Running SELECT WHERE NAME='User42' (full scan)...\n");
    long long scan_count = 0;
    auto t4 = Clock::now();
    flexql_exec(db, "SELECT * FROM BENCH WHERE NAME = 'User42';",
                count_cb, &scan_count, nullptr);
    long long scan_ms = ms_since(t4);

    flexql_close(db);

    /* ── Results ── */
    long long inserted = rows - errors;
    double    rate     = insert_ms > 0
                         ? (double)inserted * 1000.0 / insert_ms : 0;

    printf("\n=======================================================\n");
    printf(" BENCHMARK RESULTS  (%lld rows)\n", rows);
    printf("=======================================================\n");
    printf("  INSERT total time   : %8lld ms  (%.0f rows/sec)\n",
           insert_ms, rate);
    printf("  INSERT errors       : %8lld\n",   errors);
    printf("  Rows inserted       : %8lld\n",   inserted);
    printf("  SELECT WHERE PK=x   : %8lld ms  (1000 B+-tree lookups)\n", pk_ms);
    printf("  SELECT WHERE name=x : %8lld ms  (%lld rows)\n",
           scan_ms, scan_count);
    printf("=======================================================\n\n");

    return errors == 0 ? 0 : 1;
}
