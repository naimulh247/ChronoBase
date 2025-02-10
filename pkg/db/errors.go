package db

import "errors"

// Common errors
var (
    ErrDatabaseClosed     = errors.New("database is closed")
    ErrTableNotFound      = errors.New("table not found")
    ErrTableExists        = errors.New("table already exists")
    ErrInvalidTableName   = errors.New("invalid table name")
    ErrTableClosed        = errors.New("table is closed")
    ErrInvalidKey         = errors.New("invalid key")
    ErrKeyNotFound        = errors.New("key not found")
    ErrInvalidTimestamp   = errors.New("invalid timestamp")
    ErrInvalidTimeRange   = errors.New("invalid time range")
    ErrInvalidDataSize    = errors.New("invalid data size")
    ErrTabletTooLarge     = errors.New("tablet too large")
    ErrInvalidTablet      = errors.New("invalid tablet")
    ErrTabletNotFound     = errors.New("tablet not found")
    ErrMergeInProgress    = errors.New("merge in progress")
    ErrFlushInProgress    = errors.New("flush in progress")
    ErrCompactionInProgress = errors.New("compaction in progress")
    ErrIOError            = errors.New("io error")
    ErrCorruptData        = errors.New("corrupt data")
    ErrInvalidConfig      = errors.New("invalid configuration")
)

// TableError represents a table-specific error
type TableError struct {
    TableName string
    Err       error
}

// Error implements the error interface
func (e *TableError) Error() string {
    return "table " + e.TableName + ": " + e.Err.Error()
}

// Unwrap returns the underlying error
func (e *TableError) Unwrap() error {
    return e.Err
}

// TabletError represents a tablet-specific error
type TabletError struct {
    TabletID string
    Err      error
}

// Error implements the error interface
func (e *TabletError) Error() string {
    return "tablet " + e.TabletID + ": " + e.Err.Error()
}

// Unwrap returns the underlying error
func (e *TabletError) Unwrap() error {
    return e.Err
}

// NewTableError creates a new table error
func NewTableError(tableName string, err error) error {
    if err == nil {
        return nil
    }
    return &TableError{TableName: tableName, Err: err}
}

// NewTabletError creates a new tablet error
func NewTabletError(tabletID string, err error) error {
    if err == nil {
        return nil
    }
    return &TabletError{TabletID: tabletID, Err: err}
}

// IsTableNotFound returns true if the error indicates a table not found condition
func IsTableNotFound(err error) bool {
    return errors.Is(err, ErrTableNotFound)
}

// IsTableExists returns true if the error indicates a table already exists
func IsTableExists(err error) bool {
    return errors.Is(err, ErrTableExists)
}

// IsKeyNotFound returns true if the error indicates a key not found condition
func IsKeyNotFound(err error) bool {
    return errors.Is(err, ErrKeyNotFound)
}

// IsCorruptData returns true if the error indicates corrupt data
func IsCorruptData(err error) bool {
    return errors.Is(err, ErrCorruptData)
}

// IsIOError returns true if the error is an IO error
func IsIOError(err error) bool {
    return errors.Is(err, ErrIOError)
}