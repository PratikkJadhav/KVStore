# Bitcask Key-Value Store

A persistent, log-structured key-value store implemented in Go, inspired by the [Riak Bitcask paper](https://riak.com/assets/bitcask-intro.pdf). Supports both a raw key-value interface and a SQL-like query layer with JSON value encoding.

## Architecture

All writes are sequential appends to an active data file. An in-memory hash table (`keyDir`) maps every key to its exact file ID, byte offset, and value size — giving O(1) reads with a single disk seek. When the active file exceeds a size threshold, it is rotated to a read-only archive and a new active file is created.

The query layer sits on top of the storage engine as a separate package. It implements a lexer, parser, and executor that translate SQL-like strings into raw `Get`, `Set`, and `Delete` calls on the underlying store.

```
user input → Lexer → []Token → Parser → AST → Executor → BitCask
```

## Features

- Append-only storage with sequential write performance
- In-memory KeyDir for O(1) key lookup
- File rotation when active file exceeds 10MB threshold
- Merge/compaction to reclaim disk space and remove stale entries
- Crash recovery by replaying all `.db` files chronologically on startup
- SQL-like query layer with INSERT, SELECT, DELETE, and UPSERT
- JSON value encoding with auto-generated primary keys
- Field-preserving UPSERT — only specified fields are overwritten

## Project Structure

```
KVStore/
  main.go           ← CLI entry point
  bitcask/
    bitcask.go      ← storage engine
  query/
    ast.go          ← token types and AST node definitions
    lexer.go        ← tokenizer
    parser.go       ← builds AST from token stream
    executor.go     ← translates AST to BitCask operations
```

## Getting Started

```bash
git clone https://github.com/PratikkJadhav/KVStore
cd KVStore
go run main.go
```

Requires Go 1.22 or higher.

## Query Layer

The query layer treats keys as `table:id` pairs and values as JSON blobs. This enables SQL-like operations over the raw key-value store.

### INSERT

Inserts a new record. Fails if the ID already exists — use UPSERT to modify.

Auto-generate ID:
```sql
INSERT INTO users (name, age) VALUES ('Pratik', 22)
-- INSERT OK. ID: 1
```

Provide ID explicitly:
```sql
INSERT INTO users (id, name, age) VALUES ('99', 'PJ', 21)
-- INSERT OK. ID: 99
```

### SELECT

Fetch all records in a table:
```sql
SELECT * FROM users
```

Fetch a single record by ID:
```sql
SELECT * FROM users WHERE id = '1'
```

### UPSERT

Updates a record if it exists, inserts if it doesn't. Only the specified fields are overwritten — other fields are preserved.

```sql
UPSERT INTO users (id, name) VALUES ('1', 'PratikJ')
-- UPSERT OK. ID: 1
-- age field is preserved from the existing record
```

### DELETE

```sql
DELETE FROM users WHERE id = '99'
-- DELETE OK
```

## Raw Interface

The underlying store also accepts direct key-value commands:

| Command | Description |
|---|---|
| `SET <key> <value>` | Write a key-value pair |
| `GET <key>` | Read a value by key |
| `DELETE <key>` | Tombstone a key |
| `MERGE` | Trigger manual compaction |

## Storage Format

Each record on disk is a fixed-header binary entry:

```
[ timestamp (8B) | key_size (8B) | value_size (8B) | key | value ]
```

Deletion appends a tombstone entry with `value_size = 0`. The key is removed from the in-memory index immediately. Stale entries are reclaimed during MERGE.

## References

- [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
- [Arpit Bhayani — Understanding Bitcask](https://arpitbhayani.me/blogs/bitcask)
