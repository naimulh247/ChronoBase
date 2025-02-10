package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/naimulh247/logdb/internal/config"
	"github.com/naimulh247/logdb/internal/storage"
	"github.com/naimulh247/logdb/internal/utils"
)

// DB represents the database instance
type DB struct {
    config     *config.Config
    tables     map[string]*storage.Table
    background *backgroundTasks
    mu         sync.RWMutex
    closed     bool
}

// backgroundTasks manages background operations
type backgroundTasks struct {
    ctx           context.Context
    cancel        context.CancelFunc
    flushTicker   *time.Ticker
    mergeTicker   *time.Ticker
    cleanupTicker *time.Ticker
    wg            sync.WaitGroup
}

// OpenDB creates or opens a database with the given configuration
// func OpenDB(cfg *config.Config) (*DB, error) {
//     if err := cfg.Validate(); err != nil {
//         return nil, fmt.Errorf("invalid configuration: %w", err)
//     }

//     // Ensure data directory exists
//     if err := utils.EnsureDir(cfg.DataDir); err != nil {
//         return nil, fmt.Errorf("failed to create data directory: %w", err)
//     }

//     ctx, cancel := context.WithCancel(context.Background())
//     db := &DB{
//         config: cfg,
//         tables: make(map[string]*storage.Table),
//         background: &backgroundTasks{
//             ctx:           ctx,
//             cancel:        cancel,
//             flushTicker:   time.NewTicker(cfg.FlushInterval.Duration),
//             mergeTicker:   time.NewTicker(cfg.MergeInterval.Duration),
//             cleanupTicker: time.NewTicker(cfg.MaintenanceInterval.Duration),
//         },
//     }

//     // Start background tasks
//     if err := db.startBackgroundTasks(); err != nil {
//         db.Close()
//         return nil, fmt.Errorf("failed to start background tasks: %w", err)
//     }

//     // Load existing tables
//     if err := db.loadExistingTables(); err != nil {
//         db.Close()
//         return nil, fmt.Errorf("failed to load existing tables: %w", err)
//     }

//     return db, nil
// }

func OpenDB(cfg *config.Config) (*DB, error) {
    log.Println("Validating configuration...")
    if err := cfg.Validate(); err != nil {
        return nil, fmt.Errorf("invalid configuration: %w", err)
    }

    log.Printf("Ensuring data directory exists: %s", cfg.DataDir)
    if err := utils.EnsureDir(cfg.DataDir); err != nil {
        return nil, fmt.Errorf("failed to create data directory: %w", err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    log.Println("Creating database instance...")
    db := &DB{
        config: cfg,
        tables: make(map[string]*storage.Table),
        background: &backgroundTasks{
            ctx:           ctx,
            cancel:        cancel,
            flushTicker:   time.NewTicker(cfg.FlushInterval.Duration),
            mergeTicker:   time.NewTicker(cfg.MergeInterval.Duration),
            cleanupTicker: time.NewTicker(cfg.MaintenanceInterval.Duration),
        },
    }

    log.Println("Starting background tasks...")
    if err := db.startBackgroundTasks(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to start background tasks: %w", err)
    }

    log.Println("Loading existing tables...")
    if err := db.loadExistingTables(); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to load existing tables: %w", err)
    }

    log.Println("Database opened successfully")
    return db, nil
}

// CreateTable creates a new table
func (db *DB) CreateTable(name string, options TableOptions) error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if db.closed {
        return ErrDatabaseClosed
    }

    if _, exists := db.tables[name]; exists {
        return ErrTableExists
    }

    table, err := storage.NewTable(name, db.config.DataDir, options.TTL)
    if err != nil {
        return fmt.Errorf("failed to create table: %w", err)
    }

    db.tables[name] = table
    return nil
}

// GetTable returns a table by name
func (db *DB) GetTable(name string) (*storage.Table, error) {
    db.mu.RLock()
    defer db.mu.RUnlock()

    if db.closed {
        return nil, ErrDatabaseClosed
    }

    table, exists := db.tables[name]
    if !exists {
        return nil, fmt.Errorf("table %s does not exist", name)
    }

    return table, nil
}

// DropTable removes a table
func (db *DB) DropTable(name string) error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if db.closed {
        return ErrDatabaseClosed
    }

    table, exists := db.tables[name]
    if !exists {
        return fmt.Errorf("table %s does not exist", name)
    }

    if err := table.Close(); err != nil {
        return fmt.Errorf("failed to close table: %w", err)
    }

    if err := table.Delete(); err != nil {
        return fmt.Errorf("failed to delete table data: %w", err)
    }

    delete(db.tables, name)
    return nil
}

// Insert inserts a row into a table
func (db *DB) Insert(tableName string, row storage.Row) error {
    table, err := db.GetTable(tableName)
    if err != nil {
        return err
    }

    return table.Insert(row)
}

// Query executes a query on a table
func (db *DB) Query(tableName string, startTime, endTime time.Time) ([]storage.Row, error) {
    table, err := db.GetTable(tableName)
    if err != nil {
        return nil, err
    }

    return table.Query(startTime, endTime)
}

// Close closes the database
func (db *DB) Close() error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if db.closed {
        return nil
    }

    // Stop background tasks
    if db.background != nil {
        db.background.cancel()
        db.background.flushTicker.Stop()
        db.background.mergeTicker.Stop()
        db.background.cleanupTicker.Stop()
        db.background.wg.Wait()
    }

    // Close all tables
    var lastErr error
    for name, table := range db.tables {
        if err := table.Close(); err != nil {
            lastErr = fmt.Errorf("failed to close table %s: %w", name, err)
        }
    }

    db.closed = true
    return lastErr
}

// startBackgroundTasks starts the background maintenance tasks
func (db *DB) startBackgroundTasks() error {
    bg := db.background
    bg.wg.Add(3)

    // Periodic flush task
    go func() {
        defer bg.wg.Done()
        for {
            select {
            case <-bg.ctx.Done():
                return
            case <-bg.flushTicker.C:
                if db.closed {
                    return
                }
                db.flushAllTables()
            }
        }
    }()

    // Periodic merge task
    go func() {
        defer bg.wg.Done()
        for {
            select {
            case <-bg.ctx.Done():
                return
            case <-bg.mergeTicker.C:
                if db.closed {
                    return
                }
                db.mergeAllTables()
            }
        }
    }()

    // Periodic cleanup task
    go func() {
        defer bg.wg.Done()
        for {
            select {
            case <-bg.ctx.Done():
                return
            case <-bg.cleanupTicker.C:
                if db.closed {
                    return
                }
                db.cleanupAllTables()
            }
        }
    }()

    return nil
}

// flushAllTables flushes all tables to disk
func (db *DB) flushAllTables() {
    db.mu.RLock()
    defer db.mu.RUnlock()

    for _, table := range db.tables {
        if err := table.Flush(); err != nil {
            // Log error but continue with other tables
            fmt.Printf("Error flushing table: %v\n", err)
        }
    }
}

// mergeAllTables triggers merges for all tables
func (db *DB) mergeAllTables() {
    db.mu.RLock()
    defer db.mu.RUnlock()

    for _, table := range db.tables {
        if err := table.Merge(); err != nil {
            // Log error but continue with other tables
            fmt.Printf("Error merging table: %v\n", err)
        }
    }
}

// cleanupAllTables removes expired data from all tables
func (db *DB) cleanupAllTables() {
    db.mu.RLock()
    defer db.mu.RUnlock()

    for _, table := range db.tables {
        if err := table.Cleanup(); err != nil {
            // Log error but continue with other tables
            fmt.Printf("Error cleaning up table: %v\n", err)
        }
    }
}

// loadExistingTables loads existing tables from disk
func (db *DB) loadExistingTables() error {
    log.Printf("Loading existing tables from %s", db.config.DataDir)
    
    // List directories in data dir (each directory is a table)
    entries, err := os.ReadDir(db.config.DataDir)
    if err != nil {
        if os.IsNotExist(err) {
            return nil // No tables yet
        }
        return fmt.Errorf("failed to read data directory: %w", err)
    }

    for _, entry := range entries {
        if !entry.IsDir() {
            continue
        }
        tableName := entry.Name()
        tablePath := filepath.Join(db.config.DataDir, tableName)
        
        log.Printf("Found table directory: %s", tableName)

        // Create table object
        table, err := storage.NewTable(tableName, db.config.DataDir, 24*time.Hour)
        if err != nil {
            return fmt.Errorf("failed to create table %s: %w", tableName, err)
        }

        // Load tablets for this table
        tabletFiles, err := utils.ListFilesWithSuffix(tablePath, ".tab")
        if err != nil {
            return fmt.Errorf("failed to list tablet files for table %s: %w", tableName, err)
        }

        log.Printf("Found %d tablet files for table %s", len(tabletFiles), tableName)

        // Load each tablet
        for _, tabletPath := range tabletFiles {
            tabletID := strings.TrimSuffix(filepath.Base(tabletPath), ".tab")
            
            tablet := storage.NewTablet(tabletID, db.config.BlockSize)
            tablet.FilePath = tabletPath
            tablet.IsOnDisk = true

            // Load tablet metadata
            if err := tablet.LoadMetadata(); err != nil {
                return fmt.Errorf("failed to load tablet metadata %s: %w", tabletPath, err)
            }

            log.Printf("Loaded tablet %s with time range %v to %v", 
                tabletID, tablet.MinTS, tablet.MaxTS)

            table.AddDiskTablet(tablet)
        }

        db.tables[tableName] = table
    }

    log.Printf("Loaded %d tables", len(db.tables))
    return nil
}

// func (db *DB) loadExistingTables() error {
//     tableFiles, err := utils.ListFilesWithSuffix(db.config.DataDir, ".tab")
//     if err != nil {
//         return fmt.Errorf("failed to list table files: %w", err)
//     }

//     for _, tablePath := range tableFiles {
//         tableName := filepath.Base(tablePath)
//         tableName = strings.TrimSuffix(tableName, ".tab")
        
//         table, err := storage.NewTable(tableName, db.config.DataDir, 24*time.Hour) // Default TTL
//         if err != nil {
//             return fmt.Errorf("failed to load table %s: %w", tablePath, err)
//         }
//         db.tables[tableName] = table
//     }

//     return nil
// }



// Backup creates a backup of the database
func (db *DB) Backup(backupDir string) error {
    db.mu.RLock()
    defer db.mu.RUnlock()

    if db.closed {
        return ErrDatabaseClosed
    }

    // Ensure backup directory exists
    if err := utils.EnsureDir(backupDir); err != nil {
        return fmt.Errorf("failed to create backup directory: %w", err)
    }

    // Backup each table
    for name, table := range db.tables {
        tablePath := fmt.Sprintf("%s/%s", backupDir, name)
        if err := table.Backup(tablePath); err != nil {
            return fmt.Errorf("failed to backup table %s: %w", name, err)
        }
    }

    return nil
}

// Restore restores the database from a backup
func (db *DB) Restore(backupDir string) error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if db.closed {
        return ErrDatabaseClosed
    }

    // Close existing tables
    for _, table := range db.tables {
        if err := table.Close(); err != nil {
            return fmt.Errorf("failed to close existing table: %w", err)
        }
    }

    // Clear tables map
    db.tables = make(map[string]*storage.Table)

    // Restore each table
    tableFiles, err := utils.ListFilesWithSuffix(backupDir, ".tab")
    if err != nil {
        return fmt.Errorf("failed to list backup files: %w", err)
    }

    for _, tablePath := range tableFiles {
        tableName := filepath.Base(tablePath)
        tableName = strings.TrimSuffix(tableName, ".tab")
        
        table, err := storage.NewTable(tableName, db.config.DataDir, 24*time.Hour) // Default TTL
        if err != nil {
            return fmt.Errorf("failed to restore table: %w", err)
        }
        
        if err := table.Restore(filepath.Join(backupDir, tableName)); err != nil {
            return fmt.Errorf("failed to restore table data: %w", err)
        }
        
        db.tables[tableName] = table
    }

    return nil
}
