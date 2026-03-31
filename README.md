

# Bitcask Key-Value Store

## Overview
This repository contains a persistent, log-structured key-value store implemented in Go. It is heavily inspired by the architecture detailed in the Riak Bitcask paper. The system utilizes an append-only storage engine to guarantee high write throughput, coupled with an in-memory hash table index to ensure low-latency, constant-time `O(1)` read operations.

Single file implementation of bitcask architecture with no external dependencies.

### References
* **Bitcask Paper:** [A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
* **Arpit Bhayani's Blog:** [Understanding Bitcask](https://arpitbhayani.me/blogs/bitcask)

## Features
* **Append-Only Storage:** All write operations (including updates and deletions) are strictly sequential appends, maximizing disk write performance.
* **In-Memory Key Directory (`keyDir`):** Maintains a mapping of active keys to their exact file ID, byte offset, and size for immediate retrieval.
* **File Rotation:** Automatically segments the active data file into immutable, read-only archives once a predefined size threshold (10MB) is reached.
* **Compaction (Merging):** Features a garbage collection mechanism that scans archival files, extracts the most recent valid state for every key, writes a dense, consolidated active file, and safely removes stale data and tombstones from the disk.
* **Durability & Recovery:** Ensures data persistence across process restarts by chronologically scanning all existing `.db` files on startup to accurately rebuild the active `keyDir` state.

## Getting Started

### Prerequisites
* Go 1.18 or higher installed on your system.

### Running the Application
To start the interactive command-line interface, navigate to the project directory and execute:

```bash
go run main.go
```

### Available Commands
The CLI supports the following operations:

* **`SET <key> <value>`**
  Inserts a new key-value pair or updates an existing key.
* **`GET <key>`**
  Retrieves the current value associated with the specified key.
* **`DELETE <key>`**
  Appends a tombstone record to logically delete the key from the active index.
* **`MERGE`**
  Manually triggers the compaction process to reclaim disk space and consolidate fragmented archival files.

## Architecture Notes
Because this system relies on an append-only mechanism, updating a value does not overwrite the previous data on disk. Instead, the new value is appended to the end of the active file, and the in-memory pointer is updated. The `MERGE` command is strictly necessary to prevent unbounded disk usage over long periods of operation.

## Benchmarks
Performance tests were executed on an Intel Core i5-10300H @ 2.50GHz running Linux (WSL). Note that running file I/O operations across a mounted filesystem (like `/mnt/d/` in WSL) introduces significant overhead compared to native execution.

```text
goos: linux
goarch: amd64
pkg: [github.com/PratikkJadhav/KVStore.git](https://github.com/PratikkJadhav/KVStore.git)
cpu: Intel(R) Core(TM) i5-10300H CPU @ 2.50GHz
BenchmarkSet-8              5875            451677 ns/op           0.07 MB/s
BenchmarkGet-8              2598            916466 ns/op
PASS
ok      [github.com/PratikkJadhav/KVStore.git](https://github.com/PratikkJadhav/KVStore.git)    85.969s
```

## Testing Disclaimer
Disclaimer: This test file was not written by me, I don't know testing as of now. In the future I am planning to learn testing, but for this project I used AI to write the test file for testing.
