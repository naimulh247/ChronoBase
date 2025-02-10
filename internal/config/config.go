package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config represents the complete configuration for ChronoBase
type Config struct {
	// Basic settings
	DataDir       string   `json:"data_dir"`
	MaxTabletSize int64    `json:"max_tablet_size"`
	BlockSize     int64    `json:"block_size"`
	FlushInterval Duration `json:"flush_interval"`
	MergeInterval Duration `json:"merge_interval"`

	// Compression settings
	CompressionEnabled bool   `json:"compression_enabled"`
	CompressionType    string `json:"compression_type"`
	CompressionLevel   int    `json:"compression_level"`

	// Performance settings
	MaxMemoryUsage   int64 `json:"max_memory_usage"`
	MaxOpenFiles     int   `json:"max_open_files"`
	BufferPoolSize   int64 `json:"buffer_pool_size"`
	ConcurrentMerges int   `json:"concurrent_merges"`
	QueryConcurrency int   `json:"query_concurrency"`

	// Maintenance settings
	MaintenanceInterval Duration `json:"maintenance_interval"`
	MaxFileAge          Duration `json:"max_file_age"`
	BackupInterval      Duration `json:"backup_interval"`
	BackupRetention     int      `json:"backup_retention"`

	// Logging settings
	LogLevel      string `json:"log_level"`
	LogFile       string `json:"log_file"`
	EnableMetrics bool   `json:"enable_metrics"`
}

// Duration is a wrapper around time.Duration for JSON marshaling
type Duration struct {
	time.Duration
}

// MarshalJSON implements json.Marshaler
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements json.Unmarshaler
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid duration")
	}
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		// Basic settings
		DataDir:       "./data",
		MaxTabletSize: 256 * 1024 * 1024, // 256MB
		BlockSize:     64 * 1024,         // 64KB
		FlushInterval: Duration{10 * time.Minute},
		MergeInterval: Duration{1 * time.Hour},

		// Compression settings
		CompressionEnabled: true,
		CompressionType:    "gzip",
		CompressionLevel:   6,

		// Performance settings
		MaxMemoryUsage:   1 * 1024 * 1024 * 1024, // 1GB
		MaxOpenFiles:     1000,
		BufferPoolSize:   128 * 1024 * 1024, // 128MB
		ConcurrentMerges: 2,
		QueryConcurrency: 4,

		// Maintenance settings
		MaintenanceInterval: Duration{1 * time.Hour},
		MaxFileAge:          Duration{30 * 24 * time.Hour}, // 30 days
		BackupInterval:      Duration{24 * time.Hour},      // Daily
		BackupRetention:     7,                             // Keep 7 backups

		// Logging settings
		LogLevel:      "info",
		LogFile:       "db.log",
		EnableMetrics: true,
	}
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig()
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	return config, nil
}

// SaveConfig saves the configuration to a JSON file
func (c *Config) SaveConfig(path string) error {
	if err := c.Validate(); err != nil {
		return err
	}

	data, err := json.MarshalIndent(c, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Basic settings validation
	if c.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if c.MaxTabletSize <= 0 {
		return fmt.Errorf("max tablet size must be positive")
	}
	if c.BlockSize <= 0 {
		return fmt.Errorf("block size must be positive")
	}
	if c.BlockSize > c.MaxTabletSize {
		return fmt.Errorf("block size cannot be larger than tablet size")
	}

	// Performance settings validation
	if c.MaxMemoryUsage <= 0 {
		return fmt.Errorf("max memory usage must be positive")
	}
	if c.MaxOpenFiles <= 0 {
		return fmt.Errorf("max open files must be positive")
	}
	if c.BufferPoolSize <= 0 {
		return fmt.Errorf("buffer pool size must be positive")
	}
	if c.ConcurrentMerges <= 0 {
		return fmt.Errorf("concurrent merges must be positive")
	}
	if c.QueryConcurrency <= 0 {
		return fmt.Errorf("query concurrency must be positive")
	}

	// Compression validation
	if c.CompressionEnabled {
		switch c.CompressionType {
		case "gzip", "lzw", "snappy":
			// Valid compression types
		default:
			return fmt.Errorf("unsupported compression type: %s", c.CompressionType)
		}
		if c.CompressionLevel < 0 || c.CompressionLevel > 9 {
			return fmt.Errorf("compression level must be between 0 and 9")
		}
	}

	// Log level validation
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
		// Valid log levels
	default:
		return fmt.Errorf("invalid log level: %s", c.LogLevel)
	}

	return nil
}

// SetDefaults sets any unset values to their defaults
// func (c *Config) SetDefaults() {
// 	defaults := DefaultConfig()

// 	if c.DataDir == "" {
// 		c.DataDir = defaults.DataDir
// 	}
// 	if c.MaxTabletSize <= 0 {
// 		c.MaxTabletSize = defaults.MaxTabletSize
// 	}
// 	if c.BlockSize <= 0 {
// 		c.BlockSize = defaults.BlockSize
// 	}
// 	if c.FlushInterval <= 0 {
// 		c.FlushInterval = defaults.FlushInterval
// 	}
// 	if c.MergeInterval <= 0 {
// 		c.MergeInterval = defaults.MergeInterval
// 	}
// 	if c.MaxMemoryUsage <= 0 {
// 		c.MaxMemoryUsage = defaults.MaxMemoryUsage
// 	}
// 	if c.MaxOpenFiles <= 0 {
// 		c.MaxOpenFiles = defaults.MaxOpenFiles
// 	}
// 	if c.BufferPoolSize <= 0 {
// 		c.BufferPoolSize = defaults.BufferPoolSize
// 	}
// 	if c.ConcurrentMerges <= 0 {
// 		c.ConcurrentMerges = defaults.ConcurrentMerges
// 	}
// 	if c.QueryConcurrency <= 0 {
// 		c.QueryConcurrency = defaults.QueryConcurrency
// 	}
// 	if c.LogLevel == "" {
// 		c.LogLevel = defaults.LogLevel
// 	}
// }

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	clone := *c
	return &clone
}

// String returns a string representation of the configuration
func (c *Config) String() string {
	data, _ := json.MarshalIndent(c, "", "    ")
	return string(data)
}

// GetConfigPath returns the default configuration file path
func GetConfigPath() string {
	if path := os.Getenv("CHRONOBASE_CONFIG"); path != "" {
		return path
	}
	return "config/dbconfig.json"
}
