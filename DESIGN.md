# FlexQL Design Document
GIT : https://github.com/rohitkumawat7270/FlexQLPROJECT


## Overview

I built FlexQL as a C++17 client-server database system that executes SQL-like queries over TCP. I implemented a handwritten SQL parser, a WAL-based persistence mechanism, and a disk-backed storage engine.
In my design, I do not load entire tables into memory at startup. Instead, I load only schemas and lightweight metadata, and fetch row data from disk when required. This makes the system memory-efficient and scalable.

---

## Architecture

My system follows a client-server architecture:

Client  
→ flexql_exec()  
→ TCP protocol  
→ Server  
  → Parser  
  → Executor  
  → Database  
    → Table registry  
    → LRU cache  
    → Storage engine  
    → WAL  

### Core Components

- `server.cpp` → I handle client connections and request dispatching here  
- `executor.cpp` → I parse SQL queries and execute them  
- `database.cpp` → I manage tables and integrate WAL  
- `table.cpp` → I provide abstraction over table operations  
- `storage_engine.cpp` → I handle all disk IO operations  
- `bptree.cpp` → I implement primary key indexing  

---

## Storage Model

For each table, I store:

- `<TABLE>.schema` → schema definition  
- `<TABLE>.data` → binary row storage  
- `<TABLE>.txt` → human-readable mirror  

### Row Format

[4 bytes] magic  
[1 byte ] flags  
[8 bytes] expiry  
[4 bytes] value count  
repeat:  
 [4 bytes] value length  
 [N bytes] value bytes  

### Design Decisions

- I store row data on disk instead of memory  
- I use append-only writes for inserts  
- I implement soft deletes instead of immediate removal  
- I read rows from disk on demand  

---

## Startup Behavior

At startup, I do not load full table data into memory.

Instead, I:

1. Load all schemas  
2. Create table objects  
3. Scan `.data` files once  
4. Build metadata (row count and offsets)  
5. Build B+ tree indexes for primary keys  

### Loaded into Memory

- Schemas  
- Table metadata  
- Primary key index (offset-based)  

### Not Loaded

- Full row data  

This reduces memory usage significantly while still enabling efficient queries.

---

## Query Execution

### Full Table Scan

For queries like:

SELECT * FROM table;


1. Resolve the table  
2. Stream rows from disk  
3. Apply projection  
4. Return results  

This is a disk-based sequential scan.

---

### WHERE Queries

I handle two cases:

#### Primary Key Equality

For queries like:

WHERE ID = x


- Use the B+ tree index  
- Find the file offset  
- Read only the required row  

#### Non-PK Conditions

For all other conditions:

- I perform a full scan  
- I apply the filter row by row  

---

## Insert Path

For insert operations, I follow an append-based approach:

1. I parse input values  
2. I validate using the table schema  
3. I write a WAL entry  
4. I append the row to the `.data` file  
5. I get the file offset  
6. I update row count  
7. I update B+ tree (if primary key exists)  

This ensures efficient and scalable inserts.

---

## Delete Path

For delete operations:

1. I write a WAL delete record  
2. I mark rows as deleted in the `.data` file  
3. I clear the B+ tree  
4. I reset the row count  

I do not immediately reclaim space. Cleanup happens later via compaction.

---

## WAL and Recovery

I use WAL to ensure crash safety.

### Recovery Process

1. I load schema and metadata  
2. I replay WAL records  
3. I reapply inserts (append to file)  
4. I reapply deletes (mark rows deleted)  

This ensures data consistency after crashes.

---

## Caching

I implemented an LRU query cache.

### Behavior

- I cache only SELECT queries  
- I invalidate cache on table updates  
- I cache results with ≤ 10000 rows  

This improves performance for repeated queries.

---

## Concurrency

I designed concurrency control as follows:

- Database table map → std::shared_mutex  
- Per-table locking → std::shared_mutex  
- Query cache → std::mutex  

### Behavior

- Multiple reads can run in parallel  
- Writes require exclusive locks  

This ensures correctness and avoids race conditions.

---

## Performance Tradeoffs

Compared to an in-memory design:

### Advantages

- Lower memory usage  
- Scales to large datasets  
- Efficient append-based inserts  

### Tradeoffs

- Full table scans are slower (disk-based)  
- Index still consumes memory  
- Large SELECT results require full materialization  

---

## Benchmark Note

For indexed performance:

- I must define a primary key  
- Otherwise, WHERE queries fall back to full scan  