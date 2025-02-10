package storage

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/naimulh247/ChronoBase/internal/utils"
)

// Table represents a collection of tablets
type Table struct {
    name           string
    dataDir        string
    inMemoryTablet *Tablet
    diskTablets    []*Tablet
    ttl            time.Duration
    maxTabletSize  int64
    MinTS          time.Time    // Add this field
    MaxTS          time.Time    // Add this field
    
    merging        bool
    flushing       bool
    mu             sync.RWMutex
    closed         bool
}


// TableStats holds statistics about a table
type TableStats struct {
    Name           string
    DiskTablets    int
    InMemoryRows   int
    TotalDiskRows  int64
    DiskSize       int64
    LastMergeTime  time.Time
    LastFlushTime  time.Time
    OldestData     time.Time
    NewestData     time.Time
}

// NewTable creates a new table
func NewTable(name string, dataDir string, ttl time.Duration) (*Table, error) {
    log.Printf("Creating new table: %s", name)
    if name == "" {
        return nil, fmt.Errorf("table name cannot be empty")
    }

    tablePath := filepath.Join(dataDir, name)
    if err := utils.EnsureDir(tablePath); err != nil {
        return nil, fmt.Errorf("failed to create table directory: %w", err)
    }

    table := &Table{
        name:          name,
        dataDir:       tablePath,
        ttl:           ttl,
        diskTablets:   make([]*Tablet, 0),
        maxTabletSize: 64 * 1024 * 1024, // 64MB default
        MinTS:         time.Time{},       // Initialize to zero time
        MaxTS:         time.Time{},       // Initialize to zero time
    }

    // Create initial in-memory tablet
    table.inMemoryTablet = NewTablet(
        fmt.Sprintf("%s_%d", name, time.Now().UnixNano()),
        64*1024, // 64KB blocks
    )

    log.Printf("Table %s created successfully", name)
    return table, nil
}


// Insert adds a row to the table
func (t *Table) Insert(row Row) error {
    log.Printf("Attempting to insert row into table %s", t.name)
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    // Check if table is properly initialized
    if t.inMemoryTablet == nil {
        return fmt.Errorf("table not properly initialized")
    }

    // Check TTL
    if t.ttl > 0 && time.Since(row.Timestamp) > t.ttl {
        log.Printf("Row expired, dropping: %v", row)
        return nil // Silently drop expired data
    }

    log.Printf("Adding row to in-memory tablet")
    // Add to in-memory tablet
    if t.inMemoryTablet == nil {
        log.Printf("Creating new in-memory tablet")
        t.inMemoryTablet = NewTablet(
            fmt.Sprintf("%s_%d", t.name, time.Now().UnixNano()),
            64*1024, // 64KB blocks
        )
    }
    t.inMemoryTablet.AddRow(row)

    // Check if tablet needs to be flushed
    if t.shouldFlush() {
        log.Printf("Tablet needs to be flushed")
        if err := t.flushInMemoryTablet(); err != nil {
            return fmt.Errorf("failed to flush tablet: %w", err)
        }
    }

    log.Printf("Row inserted successfully")
    return nil
}

// Query executes a query on the table
func (t *Table) Query(startTime, endTime time.Time) ([]Row, error) {
    t.mu.RLock()
    defer t.mu.RUnlock()

    log.Printf("Querying table %s from %v to %v", t.name, startTime, endTime)

    if t.closed {
        return nil, fmt.Errorf("table is closed")
    }

    var results []Row

    // Query in-memory tablet
    log.Printf("Querying in-memory tablet")
    inMemResults, err := t.inMemoryTablet.Query(startTime, endTime)
    if err != nil {
        return nil, fmt.Errorf("failed to query in-memory tablet: %w", err)
    }
    results = append(results, inMemResults...)
    log.Printf("Found %d results in memory", len(inMemResults))

    // Query disk tablets
    log.Printf("Querying %d disk tablets", len(t.diskTablets))
    for _, tablet := range t.diskTablets {
        if tablet.MaxTS.Before(startTime) || tablet.MinTS.After(endTime) {
            log.Printf("Skipping tablet %s (time range: %v to %v)", 
                tablet.ID, tablet.MinTS, tablet.MaxTS)
            continue
        }

        diskResults, err := tablet.Query(startTime, endTime)
        if err != nil {
            return nil, fmt.Errorf("failed to query disk tablet: %w", err)
        }
        results = append(results, diskResults...)
        log.Printf("Found %d results in disk tablet %s", 
            len(diskResults), tablet.ID)
    }

    log.Printf("Total results found: %d", len(results))
    return results, nil
}

// Flush forces a flush of the in-memory tablet
func (t *Table) Flush() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    if t.flushing {
        return fmt.Errorf("flush already in progress")
    }

    return t.flushInMemoryTablet()
}

func (t *Table) AddDiskTablet(tablet *Tablet) {
    t.mu.Lock()
    defer t.mu.Unlock()
    
    t.diskTablets = append(t.diskTablets, tablet)
    
    // Update table's time range
    if t.MinTS.IsZero() || tablet.MinTS.Before(t.MinTS) {
        t.MinTS = tablet.MinTS
    }
    if t.MaxTS.IsZero() || tablet.MaxTS.After(t.MaxTS) {
        t.MaxTS = tablet.MaxTS
    }
}


// flushInMemoryTablet writes the in-memory tablet to disk
func (t *Table) flushInMemoryTablet() error {
    log.Printf("Starting flush of in-memory tablet for table %s", t.name)
    
    if len(t.inMemoryTablet.Rows) == 0 {
        log.Println("No rows to flush, skipping")
        return nil
    }

    t.flushing = true
    defer func() { 
        t.flushing = false 
        log.Println("Flush completed")
    }()

    // Create new tablet for disk
    log.Println("Creating disk tablet")
    diskTablet := t.inMemoryTablet

    // Create new in-memory tablet
    log.Println("Creating new in-memory tablet")
    t.inMemoryTablet = NewTablet(
        fmt.Sprintf("%s_%d", t.name, time.Now().UnixNano()),
        diskTablet.BlockSize,
    )

    // Flush to disk
    log.Printf("Flushing tablet to disk in directory: %s", t.dataDir)
    if err := diskTablet.FlushToDisk(t.dataDir); err != nil {
        log.Printf("Error flushing to disk: %v", err)
        return fmt.Errorf("failed to flush to disk: %w", err)
    }

    // Add to disk tablets
    log.Println("Adding tablet to disk tablets list")
    t.diskTablets = append(t.diskTablets, diskTablet)

    log.Printf("Successfully flushed tablet for table %s", t.name)
    return nil
}


// Merge triggers a merge operation on disk tablets
func (t *Table) Merge() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    if t.merging {
        return fmt.Errorf("merge already in progress")
    }

    log.Printf("Starting merge for table %s with %d tablets", 
        t.name, len(t.diskTablets))

    t.merging = true
    defer func() { t.merging = false }()

    return t.mergeCandidates()
}

// mergeCandidates finds and merges suitable tablet pairs
func (t *Table) mergeCandidates() error {
    if len(t.diskTablets) < 2 {
        return nil
    }

    // Find merge candidates
    for i := 0; i < len(t.diskTablets)-1; i++ {
        t1 := t.diskTablets[i]
        t2 := t.diskTablets[i+1]

        if t.shouldMergeTablets(t1, t2) {
            if err := t.mergeTablets(t1, t2); err != nil {
                return err
            }
            // Start over since tablet list has changed
            return t.mergeCandidates()
        }
    }

    return nil
}

// shouldMergeTablets determines if two tablets should be merged
func (t *Table) shouldMergeTablets(t1, t2 *Tablet) bool {
    // Check if tablets are adjacent in time
    timeAdjacent := t1.MaxTS.Add(time.Second).After(t2.MinTS)
    
    // Check if combined size is reasonable
    estimatedCombinedSize := int64(len(t1.Rows) + len(t2.Rows))
    sizeOK := estimatedCombinedSize <= t.maxTabletSize*2
    
    return timeAdjacent && sizeOK
}

// mergeTablets merges two tablets into a new one
func (t *Table) mergeTablets(t1, t2 *Tablet) error {
    // Create merged tablet
    mergedTablet := NewTablet(
        fmt.Sprintf("%s_merged_%d", t.name, time.Now().UnixNano()),
        t1.BlockSize,
    )

    // Combine rows
    mergedTablet.Rows = append(mergedTablet.Rows, t1.Rows...)
    mergedTablet.Rows = append(mergedTablet.Rows, t2.Rows...)

    // Sort rows
    mergedTablet.Sort()

    // Write to disk
    if err := mergedTablet.FlushToDisk(t.dataDir); err != nil {
        return err
    }

    // Update tablet list
    var newTablets []*Tablet
    for _, tablet := range t.diskTablets {
        if tablet != t1 && tablet != t2 {
            newTablets = append(newTablets, tablet)
        }
    }
    newTablets = append(newTablets, mergedTablet)
    t.diskTablets = newTablets

    // Clean up old tablets
    t1.Delete()
    t2.Delete()

    return nil
}

// Cleanup removes expired data
func (t *Table) Cleanup() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    if t.ttl <= 0 {
        return nil
    }

    cutoff := time.Now().Add(-t.ttl)

    // Clean up disk tablets
    var remainingTablets []*Tablet
    for _, tablet := range t.diskTablets {
        if tablet.MaxTS.After(cutoff) {
            remainingTablets = append(remainingTablets, tablet)
        } else {
            tablet.Delete()
        }
    }
    t.diskTablets = remainingTablets

    // Clean up in-memory tablet
    var remainingRows []Row
    for _, row := range t.inMemoryTablet.Rows {
        if row.Timestamp.After(cutoff) {
            remainingRows = append(remainingRows, row)
        }
    }
    t.inMemoryTablet.Rows = remainingRows

    return nil
}

// Close closes the table
func (t *Table) Close() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return nil
    }

    // Flush any remaining in-memory data
    if err := t.flushInMemoryTablet(); err != nil {
        return fmt.Errorf("failed to flush in-memory tablet: %w", err)
    }

    t.closed = true
    return nil
}

// GetStats returns statistics about the table
func (t *Table) GetStats() TableStats {
    t.mu.RLock()
    defer t.mu.RUnlock()

    stats := TableStats{
        Name:         t.name,
        DiskTablets:  len(t.diskTablets),
        InMemoryRows: len(t.inMemoryTablet.Rows),
    }

    for _, tablet := range t.diskTablets {
        stats.TotalDiskRows += int64(len(tablet.Rows))
        size, _ := utils.GetFileSize(tablet.FilePath)
        stats.DiskSize += size

        if tablet.MinTS.Before(stats.OldestData) || stats.OldestData.IsZero() {
            stats.OldestData = tablet.MinTS
        }
        if tablet.MaxTS.After(stats.NewestData) {
            stats.NewestData = tablet.MaxTS
        }
    }

    return stats
}

// shouldFlush determines if the in-memory tablet should be flushed
func (t *Table) shouldFlush() bool {
    return len(t.inMemoryTablet.Rows) >= int(t.maxTabletSize)
}

// Delete removes all table data
func (t *Table) Delete() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    // Delete all disk tablets
    for _, tablet := range t.diskTablets {
        if err := tablet.Delete(); err != nil {
            return fmt.Errorf("failed to delete tablet: %w", err)
        }
    }

    // Clear in-memory data
    t.diskTablets = nil
    t.inMemoryTablet.Rows = nil

    // Remove table directory
    if err := utils.DeleteFilesWithSuffix(t.dataDir, ".tab"); err != nil {
        return fmt.Errorf("failed to delete table files: %w", err)
    }

    return nil
}

// Backup creates a backup of the table
func (t *Table) Backup(backupDir string) error {
    t.mu.RLock()
    defer t.mu.RUnlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    // Ensure backup directory exists
    if err := utils.EnsureDir(backupDir); err != nil {
        return fmt.Errorf("failed to create backup directory: %w", err)
    }

    // First flush any in-memory data
    if err := t.Flush(); err != nil {
        return fmt.Errorf("failed to flush before backup: %w", err)
    }

    // Copy all tablet files
    for _, tablet := range t.diskTablets {
        destPath := filepath.Join(backupDir, filepath.Base(tablet.FilePath))
        if err := utils.CopyFile(tablet.FilePath, destPath); err != nil {
            return fmt.Errorf("failed to backup tablet: %w", err)
        }
    }

    return nil
}

// Restore restores the table from a backup
func (t *Table) Restore(backupDir string) error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    // Delete existing data
    if err := t.Delete(); err != nil {
        return fmt.Errorf("failed to clear existing data: %w", err)
    }

    // Restore tablet files
    files, err := utils.ListFilesWithSuffix(backupDir, ".tab")
    if err != nil {
        return fmt.Errorf("failed to list backup files: %w", err)
    }

    for _, file := range files {
        destPath := filepath.Join(t.dataDir, filepath.Base(file))
        if err := utils.CopyFile(file, destPath); err != nil {
            return fmt.Errorf("failed to restore tablet: %w", err)
        }

        // Create tablet object
        tablet := NewTablet(filepath.Base(file), 64*1024) // Default block size
        tablet.FilePath = destPath
        tablet.IsOnDisk = true

        t.diskTablets = append(t.diskTablets, tablet)
    }

    return nil
}

// CompactAll forces compaction of all tablets
func (t *Table) CompactAll() error {
    t.mu.Lock()
    defer t.mu.Unlock()

    if t.closed {
        return fmt.Errorf("table is closed")
    }

    // First flush any in-memory data
    if err := t.flushInMemoryTablet(); err != nil {
        return fmt.Errorf("failed to flush in-memory tablet: %w", err)
    }

    // Keep merging until no more merges are possible
    for {
        mergePerformed := false
        for i := 0; i < len(t.diskTablets)-1; i++ {
            t1 := t.diskTablets[i]
            t2 := t.diskTablets[i+1]

            if t.shouldMergeTablets(t1, t2) {
                if err := t.mergeTablets(t1, t2); err != nil {
                    return fmt.Errorf("failed to merge tablets: %w", err)
                }
                mergePerformed = true
                break
            }
        }

        if !mergePerformed {
            break
        }
    }

    return nil
}

// Name returns the table name
func (t *Table) Name() string {
    return t.name
}

// SetMaxTabletSize sets the maximum tablet size
func (t *Table) SetMaxTabletSize(size int64) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.maxTabletSize = size
}

// SetTTL sets the time-to-live for data in the table
func (t *Table) SetTTL(ttl time.Duration) {
    t.mu.Lock()
    defer t.mu.Unlock()
    t.ttl = ttl
}
