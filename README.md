# LogDB

A high-performance log database implementation in Python designed for efficient storage and querying of log entries. LogDB uses memory-mapped files for optimal I/O performance and implements an indexing system for fast data retrieval.

## Features

- **Memory-Mapped File Operations**: Utilizes `mmap` for efficient file I/O operations
- **Buffered Writing**: Implements in-memory buffering with configurable flush intervals to optimize write performance
- **Thread-Safe Operations**: All operations are thread-safe with a background flush thread
- **Intelligent Indexing**: Maintains an index system for fast querying of log entries
- **Automatic File Management**: Handles file creation, resizing, and management automatically
- **Configurable Settings**: Customizable index intervals, buffer sizes, and flush intervals

## Installation

```bash
git clone https://github.com/naimulh247/logdb
cd logdb
```

## Usage

```python
from log_db import LogDB

# Initialize the database
db = LogDB(
    db_path="logs.db",          # Path to the database file
    index_interval=1000,        # Index every 1000th entry
    buffer_size=10000,          # Buffer size before flush
    flush_interval=0.1          # Flush interval in seconds
)

# Write log entries
log_entry = {
    "timestamp": "2023-11-15T10:00:00",
    "level": "INFO",
    "message": "Application started"
}
db.write(log_entry)

# Query logs
results = db.query("level:INFO")
```

## Build Package for Distribution
Sorry working on it :(

## Configuration

The `LogDB` class accepts the following parameters:

- `db_path` (str): Path to the database file
- `index_interval` (int, optional): Number of entries between each index entry. Default: 1000
- `buffer_size` (int, optional): Size of memory buffer before flush. Default: 10000
- `flush_interval` (float, optional): Time in seconds between buffer flushes. Default: 0.1

## Performance Considerations

- The memory-mapped file implementation provides near-native I/O performance
- Buffered writing/batched flushing reduces disk I/O operations
- Index system enables fast querying without full file scans
- Automatic file management handles growth efficiently

## Thread Safety

All operations in LogDB are thread-safe. The implementation uses:
- Lock-based synchronization for critical sections
- Background thread for buffer flushing
- Thread-safe queue for managing write operations

## Use Cases

LogDB is ideal for:
- Proof of concept and prototyping
- High-throughput logging systems
- Applications requiring fast log querying
- Systems with concurrent log writers
- Scenarios needing persistent log storage with quick access

## Limitations and Not Recommended For

LogDB is not suitable for:
- **Critical Financial Data**: While efficient, LogDB prioritizes performance over ACID compliance and should not be used for financial transactions or critical business data requiring strict consistency, there is a high potential for data loss
- **Large-Scale Analytics**: The indexing system is optimized for quick lookups but not for complex analytical queries or aggregations
- **Write-Heavy Production Systems**: Although performant, the memory-mapped approach may not be optimal for extremely write-heavy production systems with millions of writes per second
- **Long-Term Archival**: The database is designed for active log storage and querying, not for long-term archival where compression and storage efficiency are priorities
- **Distributed Systems**: LogDB is a single-node solution and doesn't provide built-in support for distributed operations or replication. Its ideal use case is local development and testing on a single machine.