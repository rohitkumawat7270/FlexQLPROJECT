#!/usr/bin/env bash
# FlexQL Benchmark — macOS + Linux
# Usage: ./scripts/benchmark.sh [host] [port] [rows] [batch_size]
#   batch_size: statements pipelined before reading responses (default 512)

HOST="${1:-127.0.0.1}"
PORT="${2:-9000}"
ROWS="${3:-10000000}"
BATCH="${4:-512}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if command -v g++ &>/dev/null;       then CXX=g++
elif command -v clang++ &>/dev/null; then CXX=clang++
else echo "ERROR: no C++ compiler"; exit 1; fi

SRCS=(
    "$ROOT/include/common/types.cpp"
    "$ROOT/src/network/protocol.cpp"
    "$ROOT/src/parser/parser.cpp"
    "$ROOT/src/index/bptree.cpp"
    "$ROOT/src/cache/lru_cache.cpp"
    "$ROOT/src/storage/table.cpp"
    "$ROOT/src/storage/database.cpp"
    "$ROOT/src/query/executor.cpp"
    "$ROOT/src/client/flexql_client.cpp"
    "$ROOT/scripts/bench_main.cpp"
)

INCS=("-I$ROOT/include" "-I$ROOT/src")

mkdir -p "$ROOT/bin"
printf "Compiling benchmark... "
if "$CXX" -std=c++17 -O2 -pthread "${INCS[@]}" "${SRCS[@]}" \
       -o "$ROOT/bin/flexql_bench" 2>/tmp/bench_err; then
    echo "OK"
else
    echo "FAILED"; cat /tmp/bench_err; exit 1
fi

echo "Running benchmark ($ROWS rows, batch=$BATCH)..."
echo ""
"$ROOT/bin/flexql_bench" "$HOST" "$PORT" "$ROWS" "$BATCH"
