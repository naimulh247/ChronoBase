package db

import (
	"time"

	"github.com/naimulh247/ChronoBase/internal/storage"
)

// TableOptions defines options for table creation
type TableOptions struct {
    TTL         time.Duration
    PrimaryKeys []string
}

// Query defines query parameters
type Query struct {
    StartTime time.Time
    EndTime   time.Time
    Limit     int
    Ascending bool
}

// RowIterator is an interface for iterating over query results
type RowIterator interface {
    Next() bool
    Row() storage.Row
    Error() error
    Close() error
}