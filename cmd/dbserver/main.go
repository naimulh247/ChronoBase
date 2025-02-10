package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	// "os/signal"
	// "syscall"
	"time"

	"github.com/naimulh247/logdb/internal/config"
	"github.com/naimulh247/logdb/internal/storage"
	"github.com/naimulh247/logdb/pkg/db"
)

// func main() {
// 	// Parse command line flags
// 	configPath := flag.String("config", "config/dbconfig.json", "path to config file")
// 	flag.Parse()

// 	var cfg *config.Config
// 	var err error

// 	// Try to load configuration
// 	cfg, err = config.LoadConfig(*configPath)
// 	if err != nil {
// 		log.Printf("Warning: Failed to load config file: %v", err)
// 		log.Println("Using default configuration...")
// 		cfg = config.DefaultConfig()
// 	}

// 	// Ensure data directory exists
// 	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
// 		log.Fatalf("Failed to create data directory: %v", err)
// 	}

// 	// Create database
// 	database, err := db.OpenDB(cfg)
// 	if err != nil {
// 		log.Fatalf("Failed to open database: %v", err)
// 	}
// 	defer database.Close()

// 	// Create example table
// 	if err = database.CreateTable("metrics", db.TableOptions{
//         TTL:         24 * time.Hour,
//         PrimaryKeys: []string{"id"},
//     }); err != nil {
//         log.Fatalf("Failed to create table: %v", err)
//     }

// 	// Example insert
// 	row := storage.Row{
// 		Key:       []byte("test1"),
// 		Timestamp: time.Now(),
// 		Data:      []byte("example data"),
// 	}
// 	if err := database.Insert("metrics", row); err != nil {
// 		log.Printf("Failed to insert data: %v", err)
// 	}

// 	// Example query
// 	startTime := time.Now().Add(-time.Hour)
// 	endTime := time.Now()
// 	results, err := database.Query("metrics", startTime, endTime)
// 	if err != nil {
// 		log.Printf("Failed to query data: %v", err)
// 	}
// 	log.Printf("Found %d results", len(results))

// 	log.Println("Server started. Press Ctrl+C to exit.")

// 	// Handle shutdown gracefully
// 	// sigChan := make(chan os.Signal, 1)
// 	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
// 	// <-sigChan

// 	// log.Println("Shutting down...")

//     log.Println("Server started. Will exit in 5 seconds...")
//     time.Sleep(5 * time.Second)

//     // Clean shutdown
//     database.Close()

// }

func main() {
	log.Println("Starting database server...")

	// Parse command line flags
	configPath := flag.String("config", "config/dbconfig.json", "path to config file")
	flag.Parse()

	var cfg *config.Config
	var err error

	// Try to load configuration
	log.Printf("Loading configuration from: %s", *configPath)
	cfg, err = config.LoadConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Failed to load config file: %v", err)
		log.Println("Using default configuration...")
		cfg = config.DefaultConfig()
	}
	log.Printf("Configuration loaded: %+v", cfg)

	// Ensure data directory exists
	log.Printf("Creating data directory: %s", cfg.DataDir)
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Create database
	log.Println("Opening database...")
	database, err := db.OpenDB(cfg)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	// Create example table if it doesn't exist
	log.Println("Ensuring metrics table exists...")
	err = database.CreateTable("metrics", db.TableOptions{
		TTL:         24 * time.Hour,
		PrimaryKeys: []string{"id"},
	})
	if err != nil {
		if !errors.Is(err, db.ErrTableExists) {
			log.Fatalf("Failed to create table: %v", err)
		}
		log.Println("Table metrics already exists, continuing...")
	}

	// Example insert
	log.Println("Inserting test data...")
	now := time.Now() // Use the same timestamp for consistency
	row := storage.Row{
		Key:       []byte("test1"),
		Timestamp: now,
		Data:      []byte("example data"),
	}
	if err := database.Insert("metrics", row); err != nil {
		log.Printf("Failed to insert data: %v", err)
	}
	log.Println("Insert completed")

	// Example query
	log.Println("Querying data...")
	startTime := now.Add(-time.Hour) // One hour before our insert
	endTime := now.Add(time.Hour)    // One hour after our insert
	log.Printf("Querying time range: %v to %v", startTime, endTime)

	results, err := database.Query("metrics", startTime, endTime)
	if err != nil {
		log.Printf("Failed to query data: %v", err)
	}
	log.Printf("Found %d results", len(results))
	if len(results) > 0 {
		for i, result := range results {
			log.Printf("Result %d: Key=%s, Timestamp=%v, Data=%s",
				i,
				string(result.Key),
				result.Timestamp,
				string(result.Data))
		}
	}

	// Add timeout for automatic shutdown
	shutdownTimeout := time.After(5 * time.Second)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Server started. Will automatically shut down in 5 seconds...")

	// Wait for either timeout or interrupt
	select {
	case <-shutdownTimeout:
		log.Println("Shutdown timeout reached")
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
	}

	log.Println("Shutting down...")
	if err := database.Close(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
	log.Println("Server stopped")
}
