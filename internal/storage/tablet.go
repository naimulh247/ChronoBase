package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Tablet represents a collection of rows either in memory or on disk.
type Tablet struct {
	ID        string
	MinTS     time.Time
	MaxTS     time.Time
	Rows      []Row
	IsOnDisk  bool
	FilePath  string
	BlockSize int64
	mu        sync.RWMutex
}

// NewTablet creates a new tablet.
func NewTablet(id string, blockSize int64) *Tablet {
	return &Tablet{
		ID:        id,
		BlockSize: blockSize,
		Rows:      make([]Row, 0, 1000), // Pre-allocate some capacity
		mu:        sync.RWMutex{},
	}
}

// AddRow adds a row to the tablet
func (t *Tablet) AddRow(row Row) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Ensure Rows slice is initialized
	if t.Rows == nil {
		t.Rows = make([]Row, 0, 1000)
	}

	t.Rows = append(t.Rows, row)

	// update the timestamp range
	if t.MinTS.IsZero() || row.Timestamp.Before(t.MinTS) {
		t.MinTS = row.Timestamp
	}
	if t.MaxTS.IsZero() || row.Timestamp.After(t.MaxTS) {
		t.MaxTS = row.Timestamp
	}

}

// FlushToDisk writes the tablet to disk
// FlushToDisk writes the tablet to disk
func (t *Tablet) FlushToDisk(dir string) error {
	log.Printf("FlushToDisk: Starting for tablet %s", t.ID)

	// Add timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a done channel for the flush operation
	done := make(chan error, 1)

	go func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		if t.IsOnDisk {
			done <- fmt.Errorf("tablet %s is already on disk", t.ID)
			return
		}

		filename := filepath.Join(dir, fmt.Sprintf("%s.tab", t.ID))
		log.Printf("FlushToDisk: Writing to file %s", filename)

		err := writeTabletToDisk(t, filename)
		if err != nil {
			done <- fmt.Errorf("failed to write tablet to disk: %w", err)
			return
		}

		t.IsOnDisk = true
		t.FilePath = filename
		done <- nil
	}()

	// Wait for either completion or timeout
	select {
	case err := <-done:
		if err == nil {
			log.Printf("FlushToDisk: Successfully flushed tablet %s to disk", t.ID)
		}
		return err
	case <-ctx.Done():
		return fmt.Errorf("flush operation timed out after 30 seconds")
	}
}

// Query returns rows within the given timestamp range
func (t *Tablet) Query(startTime, endTime time.Time) ([]Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	log.Printf("Tablet %s: Querying from %v to %v", t.ID, startTime, endTime)
	log.Printf("Tablet %s: Time range is MinTS=%v, MaxTS=%v", t.ID, t.MinTS, t.MaxTS)

	if t.IsOnDisk {
		log.Printf("Tablet %s: Querying from disk", t.ID)
		return queryDiskTablet(t, startTime, endTime)
	}

	log.Printf("Tablet %s: Querying from memory", t.ID)
	return queryMemoryTablet(t, startTime, endTime), nil
}

// Sort sorts the rows in the tablet
func (t *Tablet) Sort() {
	// No need for mutex since the tablet is already locked during flush
	sort.Slice(t.Rows, func(i, j int) bool {
		if t.Rows[i].Timestamp.Equal(t.Rows[j].Timestamp) {
			return string(t.Rows[i].Key) < string(t.Rows[j].Key)
		}
		return t.Rows[i].Timestamp.Before(t.Rows[j].Timestamp)
	})
}

// Size returns the number of rows in the tablet
func (t *Tablet) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return len(t.Rows)
}

// Delete removes the tablet from disk
func (t *Tablet) Delete() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.IsOnDisk {
		return nil
	}

	if err := os.Remove(t.FilePath); err != nil {
		return fmt.Errorf("failed to delete tablet file: %w", err)
	}

	t.IsOnDisk = false
	t.FilePath = ""
	return nil
}

// LoadMetadata loads the tablet's metadata from disk
func (t *Tablet) LoadMetadata() error {
	file, err := os.Open(t.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open tablet file: %w", err)
	}
	defer file.Close()

	// Read the block to get time range
	block, err := readBlock(file, 0) // Read first block
	if err != nil {
		return fmt.Errorf("failed to read first block: %w", err)
	}

	if len(block) > 0 {
		t.MinTS = block[0].Timestamp
		t.MaxTS = block[0].Timestamp

		// Find min and max timestamps
		for _, row := range block {
			if row.Timestamp.Before(t.MinTS) {
				t.MinTS = row.Timestamp
			}
			if row.Timestamp.After(t.MaxTS) {
				t.MaxTS = row.Timestamp
			}
		}
	}

	log.Printf("Loaded tablet %s metadata: MinTS=%v, MaxTS=%v",
		t.ID, t.MinTS, t.MaxTS)
	return nil
}

/* Disk operation functions:
- writeTabletToDisk()
- writeBlock()
- readBlock()
- queryDiskTablet()
- queryMemoryTablet()
*/

// writeTabletToDisk writes the tablet to disk with block-based organization
func writeTabletToDisk(tablet *Tablet, filename string) error {
	log.Printf("writeTabletToDisk: Creating file %s", filename)
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("writeTabletToDisk: Failed to create file: %v", err)
		return fmt.Errorf("failed to create tablet file: %v", err)
	}
	defer file.Close()

	// Add row count logging
	log.Printf("writeTabletToDisk: Number of rows before sorting: %d", len(tablet.Rows))

	// Sort rows before writing
	log.Println("writeTabletToDisk: Starting row sort")
	tablet.Sort()
	log.Println("writeTabletToDisk: Row sort completed")

	// Write blocks
	var blockOffsets []int64
	var lastKeys [][]byte
	currentBlock := make([]Row, 0)
	currentBlockSize := int64(0)

	log.Printf("writeTabletToDisk: Processing %d rows", len(tablet.Rows))
	for _, row := range tablet.Rows {
		rowSize := estimateRowSize(row)

		if currentBlockSize+rowSize > tablet.BlockSize && len(currentBlock) > 0 {
			// Write current block
			log.Println("writeTabletToDisk: Writing block")
			offset, err := writeBlock(file, currentBlock)
			if err != nil {
				log.Printf("writeTabletToDisk: Failed to write block: %v", err)
				return err
			}

			// Update index information
			blockOffsets = append(blockOffsets, offset)
			lastKeys = append(lastKeys, currentBlock[len(currentBlock)-1].Key)

			// Reset block
			currentBlock = currentBlock[:0]
			currentBlockSize = 0
		}

		currentBlock = append(currentBlock, row)
		currentBlockSize += rowSize
	}

	// Write final block if not empty
	if len(currentBlock) > 0 {
		log.Println("writeTabletToDisk: Writing final block")
		offset, err := writeBlock(file, currentBlock)
		if err != nil {
			log.Printf("writeTabletToDisk: Failed to write final block: %v", err)
			return err
		}
		blockOffsets = append(blockOffsets, offset)
		lastKeys = append(lastKeys, currentBlock[len(currentBlock)-1].Key)
	}

	// Write index
	log.Println("writeTabletToDisk: Writing index")
	indexOffset, err := writeIndex(file, blockOffsets, lastKeys)
	if err != nil {
		log.Printf("writeTabletToDisk: Failed to write index: %v", err)
		return err
	}

	// Write index offset at the end
	log.Println("writeTabletToDisk: Writing index offset")
	if err := binary.Write(file, binary.BigEndian, indexOffset); err != nil {
		log.Printf("writeTabletToDisk: Failed to write index offset: %v", err)
		return fmt.Errorf("failed to write index offset: %v", err)
	}

	log.Println("writeTabletToDisk: Successfully wrote tablet to disk")
	return nil
}

// writeBlock writes a block of rows to disk
func writeBlock(file *os.File, rows []Row) (int64, error) {
	offset, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, fmt.Errorf("failed to get current offset: %w", err)
	}

	// serialize and compress block data
	serialized := serializeRows(rows)
	compressed := compressBlock(serialized)

	// Write block size
	if err := binary.Write(file, binary.BigEndian, int64(len(compressed))); err != nil {
		return 0, fmt.Errorf("failed to write block size: %w", err)
	}

	// Write compressed block data
	if _, err := file.Write(compressed); err != nil {
		return 0, fmt.Errorf("failed to write block data: %w", err)
	}

	return offset, nil
}

// readBlock reads a block of rows from disk
func readBlock(file *os.File, offset int64) ([]Row, error) {
	if _, err := file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, fmt.Errorf("failed to seek to block: %w", err)
	}

	// Read block size
	var blockSize int64
	if err := binary.Read(file, binary.BigEndian, &blockSize); err != nil {
		return nil, fmt.Errorf("failed to read block size: %w", err)
	}

	// Read compressed block data
	compressed := make([]byte, blockSize)
	if _, err := file.Read(compressed); err != nil {
		return nil, fmt.Errorf("failed to read block data: %w", err)
	}

	// Decompress and deserialize block data
	serialized := decompressBlock(compressed)
	return deserializeRows(serialized), nil
}

// queryDiskTablet reads the tablet from disk and returns rows within the given timestamp range
func queryDiskTablet(t *Tablet, startTime, endTime time.Time) ([]Row, error) {
	log.Printf("Opening tablet file: %s", t.FilePath)
	file, err := os.Open(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open tablet file: %w", err)
	}
	defer file.Close()

	// Read the index
	log.Println("Reading tablet index")
	index, err := readTabletIndex(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read tablet index: %w", err)
	}

	var results []Row

	// Find relevant blocks
	startBlock := 0                  // Always start from first block for small datasets
	endBlock := index.BlockCount - 1 // Read all blocks

	log.Printf("Reading blocks from %d to %d", startBlock, endBlock)

	// Read blocks
	for i := startBlock; i <= endBlock; i++ {
		log.Printf("Reading block %d at offset %d", i, index.BlockOffsets[i])
		block, err := readBlock(file, index.BlockOffsets[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read block %d: %w", i, err)
		}

		// Filter rows by time range
		for _, row := range block {
			log.Printf("Checking row with timestamp %v against range [%v, %v]",
				row.Timestamp, startTime, endTime)
			if (row.Timestamp.Equal(startTime) || row.Timestamp.After(startTime)) &&
				(row.Timestamp.Equal(endTime) || row.Timestamp.Before(endTime)) {
				log.Printf("Row matched: Key=%s, Time=%v", string(row.Key), row.Timestamp)
				results = append(results, row)
			}
		}
	}

	log.Printf("Found %d matching rows in disk tablet", len(results))
	return results, nil
}

// queryMemoryTablet queries an in-memory tablet and returns rows within the given timestamp range
func queryMemoryTablet(t *Tablet, startTime, endTime time.Time) []Row {
	var results []Row
	for _, row := range t.Rows {
		if !row.Timestamp.Before(startTime) && !row.Timestamp.After(endTime) {
			results = append(results, row)
		}
	}
	return results
}

/* Auxiliary functions:
- estimateRowSize()
- serializeRows()
- deserializeRows()
- compressBlock()
- decompressBlock()
*/

// estimateRowSize estimates the size of a row in bytes
func estimateRowSize(row Row) int64 {
	return int64(len(row.Key) + len(row.Data) + 16) // 16 bytes for timestamp
}

// serializeRows serializes rows into a byte slice
func serializeRows(rows []Row) []byte {
	var result []byte
	for _, row := range rows {
		// write key length
		keyLen := uint32(len(row.Key))
		result = binary.BigEndian.AppendUint32(result, keyLen)

		// write key
		result = append(result, row.Key...)

		// write timestamp
		ts := row.Timestamp.UnixNano()
		result = binary.BigEndian.AppendUint64(result, uint64(ts))

		// write data length
		dataLen := uint32(len(row.Data))
		result = binary.BigEndian.AppendUint32(result, dataLen)

		// write data
		result = append(result, row.Data...)
	}
	return result
}

// deserializeRows deserializes a byte slice into rows
func deserializeRows(data []byte) []Row {
	var rows []Row
	offset := 0

	for offset < len(data) {
		// Read key length
		keyLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		// Read key
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Read timestamp
		ts := binary.BigEndian.Uint64(data[offset:])
		offset += 8

		// Read data length
		dataLen := binary.BigEndian.Uint32(data[offset:])
		offset += 4

		// Read data
		rowData := make([]byte, dataLen)
		copy(rowData, data[offset:offset+int(dataLen)])
		offset += int(dataLen)

		rows = append(rows, Row{
			Key:       key,
			Timestamp: time.Unix(0, int64(ts)),
			Data:      rowData,
		})
	}
	return rows
}
