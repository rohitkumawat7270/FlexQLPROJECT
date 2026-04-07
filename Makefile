# Detect OS — use clang++ on macOS, g++ on Linux
UNAME := $(shell uname)
ifeq ($(UNAME), Darwin)
    CXX := clang++
else
    CXX := g++
endif

CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
LDFLAGS  := $(if $(filter Linux,$(UNAME)),-lstdc++fs,)
INCLUDES := -I./include -I./src

SRCS_COMMON := \
    src/network/protocol.cpp \
    include/common/types.cpp \
    src/parser/parser.cpp \
    src/index/bptree.cpp \
    src/cache/lru_cache.cpp \
    src/storage/table.cpp \
    src/storage/database.cpp \
    src/storage/wal.cpp \
    src/storage/storage_engine.cpp \
    src/query/executor.cpp

SRCS_SERVER     := $(SRCS_COMMON) src/server/server.cpp
SRCS_CLIENT_LIB := $(SRCS_COMMON) src/client/flexql_client.cpp
SRCS_REPL       := src/client/repl.cpp

all: bin/flexql-server bin/flexql-client bin/benchmark bin/benchmark_nilu
	@echo ""
	@echo "Build successful!"
	@echo "  Start server : ./bin/flexql-server 9000"
	@echo "  Start client : ./bin/flexql-client 127.0.0.1 9000"
	@echo "  Run benchmark : ./bin/benchmark 1000000"
	@echo "  Run nilu bench: ./bin/benchmark_nilu 100000 1"
	@echo ""

bin/flexql-server: $(SRCS_SERVER) | bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@
	@echo "[OK] Built bin/flexql-server"

bin/flexql-client: $(SRCS_CLIENT_LIB) $(SRCS_REPL) | bin
	$(CXX) $(CXXFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@
	@echo "[OK] Built bin/flexql-client"

bin/benchmark: $(SRCS_CLIENT_LIB) scripts/benchmark_flexql.cpp | bin
	$(CXX) $(CXXFLAGS) -Wno-unused-function $(INCLUDES) $^ -o $@
	@echo "[OK] Built bin/benchmark" 

bin/benchmark_nilu: $(SRCS_CLIENT_LIB) scripts/benchmark_flexql_nilu.cpp | bin
	$(CXX) $(CXXFLAGS) -Wno-unused-function $(INCLUDES) $^ -o $@
	@echo "[OK] Built bin/benchmark_nilu"

bin:
	mkdir -p bin

clean:
	rm -rf bin
	@echo "Cleaned."

benchmark: bin/benchmark
	@echo "Run: ./bin/benchmark --unit-test"
	@echo "Run: ./bin/benchmark 1000000"

benchmark_nilu: bin/benchmark_nilu
	@echo "Run: ./bin/benchmark_nilu --unit-test"
	@echo "Run: ./bin/benchmark_nilu 100000 1"

.PHONY: all clean benchmark benchmark_nilu
