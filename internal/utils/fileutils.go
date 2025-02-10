package utils

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
    "time"
)

// FileMode defines the default file permissions
const (
    DefaultDirMode  = 0755
    DefaultFileMode = 0644
)

// EnsureDir ensures a directory exists, creating it if necessary
func EnsureDir(path string) error {
    if path == "" {
        return fmt.Errorf("directory path cannot be empty")
    }

    info, err := os.Stat(path)
    if err == nil {
        if !info.IsDir() {
            return fmt.Errorf("path exists but is not a directory: %s", path)
        }
        return nil
    }

    if !os.IsNotExist(err) {
        return fmt.Errorf("failed to check directory: %w", err)
    }

    return os.MkdirAll(path, DefaultDirMode)
}

// CopyFile copies a file from src to dst
func CopyFile(src, dst string) error {
    sourceFile, err := os.Open(src)
    if err != nil {
        return fmt.Errorf("failed to open source file: %w", err)
    }
    defer sourceFile.Close()

    // Create destination directory if it doesn't exist
    if err := EnsureDir(filepath.Dir(dst)); err != nil {
        return err
    }

    destFile, err := os.Create(dst)
    if err != nil {
        return fmt.Errorf("failed to create destination file: %w", err)
    }
    defer destFile.Close()

    if _, err := io.Copy(destFile, sourceFile); err != nil {
        return fmt.Errorf("failed to copy file contents: %w", err)
    }

    return nil
}

// MoveFile moves a file from src to dst
func MoveFile(src, dst string) error {
    // Ensure destination directory exists
    if err := EnsureDir(filepath.Dir(dst)); err != nil {
        return err
    }

    // Try atomic rename first
    err := os.Rename(src, dst)
    if err == nil {
        return nil
    }

    // If rename failed (e.g., cross-device), fall back to copy and delete
    if err := CopyFile(src, dst); err != nil {
        return err
    }

    return os.Remove(src)
}

// SafeWriteFile writes data to a temporary file and then atomically moves it to the target location
func SafeWriteFile(filename string, data []byte) error {
    // Create temporary file
    tmpFile := fmt.Sprintf("%s.tmp.%d", filename, time.Now().UnixNano())
    
    if err := os.WriteFile(tmpFile, data, DefaultFileMode); err != nil {
        os.Remove(tmpFile) // Clean up on error
        return fmt.Errorf("failed to write temporary file: %w", err)
    }

    // Move temporary file to target location
    if err := MoveFile(tmpFile, filename); err != nil {
        os.Remove(tmpFile) // Clean up on error
        return fmt.Errorf("failed to move temporary file: %w", err)
    }

    return nil
}

// ListFilesWithSuffix returns a list of files in the given directory with the specified suffix
func ListFilesWithSuffix(dir, suffix string) ([]string, error) {
    var files []string

    entries, err := os.ReadDir(dir)
    if err != nil {
        return nil, fmt.Errorf("failed to read directory: %w", err)
    }

    for _, entry := range entries {
        if entry.IsDir() {
            continue
        }
        if strings.HasSuffix(entry.Name(), suffix) {
            files = append(files, filepath.Join(dir, entry.Name()))
        }
    }

    return files, nil
}

// DeleteFilesWithSuffix deletes all files in the given directory with the specified suffix
func DeleteFilesWithSuffix(dir, suffix string) error {
    files, err := ListFilesWithSuffix(dir, suffix)
    if err != nil {
        return err
    }

    for _, file := range files {
        if err := os.Remove(file); err != nil {
            return fmt.Errorf("failed to delete file %s: %w", file, err)
        }
    }

    return nil
}

// GetFileSize returns the size of a file in bytes
func GetFileSize(path string) (int64, error) {
    info, err := os.Stat(path)
    if err != nil {
        return 0, fmt.Errorf("failed to get file info: %w", err)
    }
    return info.Size(), nil
}

// FileExists checks if a file exists
func FileExists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil {
        return true, nil
    }
    if os.IsNotExist(err) {
        return false, nil
    }
    return false, fmt.Errorf("failed to check file existence: %w", err)
}

// TouchFile creates an empty file if it doesn't exist or updates its timestamp if it does
func TouchFile(path string) error {
    exists, err := FileExists(path)
    if err != nil {
        return err
    }

    if exists {
        now := time.Now()
        return os.Chtimes(path, now, now)
    }

    file, err := os.Create(path)
    if err != nil {
        return fmt.Errorf("failed to create file: %w", err)
    }
    return file.Close()
}

// CreateTempFile creates a temporary file with the given prefix and suffix
func CreateTempFile(dir, prefix, suffix string) (string, error) {
    if err := EnsureDir(dir); err != nil {
        return "", err
    }

    tmpFile := filepath.Join(dir, fmt.Sprintf("%s%d%s", prefix, time.Now().UnixNano(), suffix))
    file, err := os.Create(tmpFile)
    if err != nil {
        return "", fmt.Errorf("failed to create temporary file: %w", err)
    }
    
    if err := file.Close(); err != nil {
        os.Remove(tmpFile)
        return "", fmt.Errorf("failed to close temporary file: %w", err)
    }

    return tmpFile, nil
}

// CleanupTempFiles removes temporary files older than the specified duration
func CleanupTempFiles(dir string, pattern string, maxAge time.Duration) error {
    entries, err := os.ReadDir(dir)
    if err != nil {
        return fmt.Errorf("failed to read directory: %w", err)
    }

    now := time.Now()
    for _, entry := range entries {
        if entry.IsDir() {
            continue
        }

        if !strings.Contains(entry.Name(), pattern) {
            continue
        }

        info, err := entry.Info()
        if err != nil {
            continue
        }

        if now.Sub(info.ModTime()) > maxAge {
            path := filepath.Join(dir, entry.Name())
            if err := os.Remove(path); err != nil {
                return fmt.Errorf("failed to remove old temp file %s: %w", path, err)
            }
        }
    }

    return nil
}

// LockFile represents a file lock
type LockFile struct {
    path string
    file *os.File
}

// CreateLock creates a file lock
func CreateLock(path string) (*LockFile, error) {
    if err := EnsureDir(filepath.Dir(path)); err != nil {
        return nil, err
    }

    file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, DefaultFileMode)
    if err != nil {
        return nil, fmt.Errorf("failed to create lock file: %w", err)
    }

    return &LockFile{
        path: path,
        file: file,
    }, nil
}

// Release releases the file lock
func (l *LockFile) Release() error {
    if l.file != nil {
        l.file.Close()
        if err := os.Remove(l.path); err != nil {
            return fmt.Errorf("failed to remove lock file: %w", err)
        }
        l.file = nil
    }
    return nil
}