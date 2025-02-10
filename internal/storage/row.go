package storage

import "time"

// Row represents a single row in the db
type Row struct {
	Key       []byte
	Timestamp time.Time
	Data      []byte
}

// RowBatch is a batch of rows for bulk operations
type RowBatch struct {
	Rows []Row
}

// NewRow  creates a new row
func NewRow(key []byte, timestamp time.Time, data []byte) Row {
	return Row{
		Key:       key,
		Timestamp: timestamp,
		Data:      data,
	}
}

// NewRowBatch creates a new batch of rows
func NewRowBatch(capacity int) *RowBatch {
	return &RowBatch{
		Rows: make([]Row, 0, capacity),
	}
}

// Add adds a row to the batch
func (b *RowBatch) Add(row Row) {
	b.Rows = append(b.Rows, row)
}

// Size returns the number of rows in the batch
func (b *RowBatch) size() int {
	return len(b.Rows)
}