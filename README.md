# ChronoBase

ChronoBase is a high-performance, embedded time-series database written in Go, designed for efficient storage and retrieval of log and metric data. It features a modular architecture with configurable storage policies, compression, and automatic data management.

## Features

- **Time-Series Optimized**: Purpose-built for time-series data with efficient timestamp-based indexing
- **Configurable Storage**: Flexible tablet-based storage with automatic data management
- **Data Compression**: Built-in GZIP compression support for optimal storage efficiency
- **Automatic Data Management**: 
  - Configurable TTL (Time-To-Live) for data retention
  - Background merging of tablets for storage optimization
  - Periodic cleanup of expired data
- **Concurrent Operations**: Thread-safe design supporting parallel reads and writes
- **Block-Based Storage**: Efficient block-based storage format for better I/O performance
- **JSON Configuration**: Easy configuration via JSON files

## Getting Started

### Prerequisites

- Go 1.21 or higher
- Git

### Installation

```bash
git clone https://github.com/naimulh247/ChronoBase.git
cd ChronoBase
go build
```

### Basic Usage

```go
// Create and configure database
cfg := config.DefaultConfig()
database, err := db.OpenDB(cfg)
if err != nil {
    log.Fatal(err)
}
defer database.Close()

// Create a table
err = database.CreateTable("metrics", db.TableOptions{
    TTL: 24 * time.Hour,
    PrimaryKeys: []string{"id"},
})

// Insert data
row := storage.Row{
    Key:       []byte("metric1"),
    Timestamp: time.Now(),
    Data:      []byte("example data"),
}
err = database.Insert("metrics", row)

// Query data
startTime := time.Now().Add(-time.Hour)
endTime := time.Now()
results, err := database.Query("metrics", startTime, endTime)
```

## Configuration

ChronoBase can be configured via a JSON configuration file. Example configuration:

```json
{
    "data_dir": "./data",
    "max_tablet_size": 268435456,
    "block_size": 65536,
    "flush_interval": "10m",
    "merge_interval": "1m",
    "compression_enabled": true,
    "compression_type": "gzip",
    "compression_level": 6,
    "max_memory_usage": 1073741824,
    "max_open_files": 1000,
    "buffer_pool_size": 134217728,
    "concurrent_merges": 2,
    "query_concurrency": 4,
    "maintenance_interval": "1h",
    "max_file_age": "720h",
    "backup_interval": "24h",
    "backup_retention": 7,
    "log_level": "info",
    "log_file": "db.log",
    "enable_metrics": true
}
```

## Architecture

ChronoBase uses a tablet-based storage architecture:
- Data is organized into tables
- Each table contains multiple tablets
- Tablets store data in compressed blocks
- In-memory buffer for recent writes
- Background processes for merging and cleanup

### Key Components:
- **Table Manager**: Handles table creation and management
- **Tablet Storage**: Manages data storage in tablets
- **Block Engine**: Efficient block-based storage format
- **Index Manager**: Maintains time-based indices
- **Compression Engine**: Handles data compression/decompression

## Work in Progress

This project is under active development. Upcoming features include:
- Advanced querying capabilities
- Additional compression algorithms
- Replication support
- Metrics and monitoring
- Enhanced performance optimizations
- HTTP API interface

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
