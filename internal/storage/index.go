package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"
)

// TabletIndex represents the index structure for a tablet
type TabletIndex struct {
	BlockOffsets []int64   // Offset of each block in the file
	LastKeys     [][]byte  // Last key in each block
	BlockCount   int       // Number of blocks
	MinTS        time.Time // Minimum timestamp in the tablet
	MaxTS        time.Time // Maximum timestamp in the tablet
}

// writeIndex writes the tablet index to disk
func writeIndex(file *os.File, blockOffsets []int64, lastKeys [][]byte) (int64, error) {
	// Get current position as index offset
	indexOffset, err := file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, fmt.Errorf("failed to get index offset: %w", err)
	}

	// Write block count
	blockCount := int64(len(blockOffsets))
	if err := binary.Write(file, binary.BigEndian, blockCount); err != nil {
		return 0, fmt.Errorf("failed to write block count: %w", err)
	}

	// Write block offsets
	for _, offset := range blockOffsets {
		if err := binary.Write(file, binary.BigEndian, offset); err != nil {
			return 0, fmt.Errorf("failed to write block offset: %w", err)
		}
	}

	// Write last keys
	for _, key := range lastKeys {
		// Write key length
		keyLen := int64(len(key))
		if err := binary.Write(file, binary.BigEndian, keyLen); err != nil {
			return 0, fmt.Errorf("failed to write key length: %w", err)
		}

		// Write key data
		if _, err := file.Write(key); err != nil {
			return 0, fmt.Errorf("failed to write key data: %w", err)
		}
	}

	return indexOffset, nil
}

// readTabletIndex reads the tablet index from disk
func readTabletIndex(file *os.File) (*TabletIndex, error) {
    // Get file size
    fileInfo, err := file.Stat()
    if err != nil {
        return nil, fmt.Errorf("failed to get file info: %w", err)
    }
    fileSize := fileInfo.Size()
    log.Printf("File size: %d bytes", fileSize)

    // Read index offset from end of file
    if _, err := file.Seek(-8, os.SEEK_END); err != nil {
        return nil, fmt.Errorf("failed to seek to index offset: %w", err)
    }

    var indexOffset int64
    if err := binary.Read(file, binary.BigEndian, &indexOffset); err != nil {
        return nil, fmt.Errorf("failed to read index offset: %w", err)
    }
    log.Printf("Index starts at offset: %d", indexOffset)

    // Validate index offset
    if indexOffset < 0 || indexOffset >= fileSize-8 {
        return nil, fmt.Errorf("invalid index offset: %d", indexOffset)
    }

    // Seek to index start
    if _, err := file.Seek(indexOffset, os.SEEK_SET); err != nil {
        return nil, fmt.Errorf("failed to seek to index start: %w", err)
    }

    var blockCount int64
    if err := binary.Read(file, binary.BigEndian, &blockCount); err != nil {
        return nil, fmt.Errorf("failed to read block count: %w", err)
    }
    log.Printf("Block count: %d", blockCount)

    if blockCount <= 0 {
        return nil, fmt.Errorf("invalid block count: %d", blockCount)
    }

    index := &TabletIndex{
        BlockOffsets: make([]int64, blockCount),
        LastKeys:     make([][]byte, blockCount),
        BlockCount:   int(blockCount),
    }

    // Read block offsets
    for i := int64(0); i < blockCount; i++ {
        if err := binary.Read(file, binary.BigEndian, &index.BlockOffsets[i]); err != nil {
            return nil, fmt.Errorf("failed to read block offset %d: %w", i, err)
        }
        log.Printf("Block %d offset: %d", i, index.BlockOffsets[i])
    }

    // Read last keys
    for i := int64(0); i < blockCount; i++ {
        var keyLen int64
        if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
            return nil, fmt.Errorf("failed to read key length %d: %w", i, err)
        }

        key := make([]byte, keyLen)
        if _, err := io.ReadFull(file, key); err != nil {
            return nil, fmt.Errorf("failed to read key data %d: %w", i, err)
        }
        index.LastKeys[i] = key
        log.Printf("Block %d last key: %s", i, string(key))
    }

    return index, nil
}


// func readTabletIndex(file *os.File) (*TabletIndex, error) {
// 	// Read index offset from the end of file
// 	if _, err := file.Seek(-8, os.SEEK_END); err != nil {
// 		return nil, fmt.Errorf("failed to seek to index offset: %w", err)
// 	}

// 	var indexOffset int64
// 	if err := binary.Read(file, binary.BigEndian, &indexOffset); err != nil {
// 		return nil, fmt.Errorf("failed to read index offset: %w", err)
// 	}

// 	// Seek to start of index
// 	if _, err := file.Seek(indexOffset, os.SEEK_SET); err != nil {
// 		return nil, fmt.Errorf("failed to seek to index start: %w", err)
// 	}

// 	// Read block count
// 	var blockCount int64
// 	if err := binary.Read(file, binary.BigEndian, &blockCount); err != nil {
// 		return nil, fmt.Errorf("failed to read block count: %w", err)
// 	}

// 	index := &TabletIndex{
// 		BlockOffsets: make([]int64, blockCount),
// 		LastKeys:     make([][]byte, blockCount),
// 		BlockCount:   int(blockCount),
// 		MinTS:        time.Time{},
// 		MaxTS:        time.Time{},
// 	}

// 	// Read block offsets
// 	for i := 0; i < int(blockCount); i++ {
// 		if err := binary.Read(file, binary.BigEndian, &index.BlockOffsets[i]); err != nil {
// 			return nil, fmt.Errorf("failed to read block offset: %w", err)
// 		}
// 	}

// 	// Read last keys
// 	for i := 0; i < int(blockCount); i++ {
// 		// Read key length
// 		var keyLen int64
// 		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
// 			return nil, fmt.Errorf("failed to read key length: %w", err)
// 		}

// 		// Read key data
// 		key := make([]byte, keyLen)
// 		if _, err := file.Read(key); err != nil {
// 			return nil, fmt.Errorf("failed to read key data: %w", err)
// 		}
// 		index.LastKeys[i] = key
// 	}

// 	return index, nil
// }

// findBlockForTime finds the block that might contain data for the given timestamp
func findBlockForTime(index *TabletIndex, targetTime time.Time) int {
    if index.BlockCount == 0 {
        return 0
    }

    // Binary search through blocks
    start := 0
    end := index.BlockCount - 1  // Fix: Use BlockCount - 1 since it's zero-based

    for start <= end {
        mid := (start + end) / 2
        timestamp := extractTimestampFromKey(index.LastKeys[mid])
        
        log.Printf("Block search: start=%d, mid=%d, end=%d, timestamp=%v", 
            start, mid, end, timestamp)
        
        if timestamp.Equal(targetTime) {
            return mid
        } else if timestamp.Before(targetTime) {
            start = mid + 1
        } else {
            end = mid - 1
        }
    }

    // Return the rightmost block that could contain our time
    return start
}

// func findBlockForTime(index *TabletIndex, targetTime time.Time) int {
// 	// Binary search through blocks
// 	return sort.Search(index.BlockCount, func(i int) bool {
// 		// Since LastKeys contains the last key in each block,
// 		// compare with the timestamp embedded in that key
// 		timestamp := extractTimestampFromKey(index.LastKeys[i])
// 		return !timestamp.Before(targetTime)
// 	})
// }

// findBlockForKey finds the block that might contain the given key
func findBlockForKey(index *TabletIndex, targetKey []byte) int {
	// Binary search through blocks
	return sort.Search(index.BlockCount, func(i int) bool {
		return string(index.LastKeys[i]) >= string(targetKey)
	})
}

// extractTimestampFromKey extracts the timestamp from a serialized key
// This implementation assumes the timestamp is the last component of the key
func extractTimestampFromKey(key []byte) time.Time {
	if len(key) < 8 {
		return time.Time{}
	}

	// Assume the last 8 bytes are the timestamp
	ts := binary.BigEndian.Uint64(key[len(key)-8:])
	return time.Unix(0, int64(ts))
}

// createTabletIndex creates a new index for a tablet
func createTabletIndex() *TabletIndex {
	return &TabletIndex{
		BlockOffsets: make([]int64, 0),
		LastKeys:     make([][]byte, 0),
		BlockCount:   0,
	}
}

// addBlock adds a new block to the index
func (idx *TabletIndex) addBlock(offset int64, lastKey []byte) {
	idx.BlockOffsets = append(idx.BlockOffsets, offset)
	idx.LastKeys = append(idx.LastKeys, lastKey)
	idx.BlockCount++
}

// updateTimeRange updates the time range of the index based on a row
func (idx *TabletIndex) updateTimeRange(timestamp time.Time) {
	if idx.MinTS.IsZero() || timestamp.Before(idx.MinTS) {
		idx.MinTS = timestamp
	}
	if idx.MaxTS.IsZero() || timestamp.After(idx.MaxTS) {
		idx.MaxTS = timestamp
	}
}

// containsTime checks if a timestamp falls within the index's time range
func (idx *TabletIndex) containsTime(timestamp time.Time) bool {
	return !timestamp.Before(idx.MinTS) && !timestamp.After(idx.MaxTS)
}

// getBlockRange returns the range of blocks that might contain data for the given time range
func (idx *TabletIndex) getBlockRange(startTime, endTime time.Time) (int, int) {
	startBlock := findBlockForTime(idx, startTime)
	endBlock := findBlockForTime(idx, endTime)

	// If end block is at the end, include the last block
	if endBlock >= idx.BlockCount {
		endBlock = idx.BlockCount - 1
	}

	return startBlock, endBlock
}
