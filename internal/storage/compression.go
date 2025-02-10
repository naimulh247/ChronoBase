package storage

import (
	"bytes"
	"compress/gzip"
	"compress/lzw"
	"io"
	"runtime"
	"sync"
)

// CompressionType represents the type of compression to use
type CompressionType int

const (
    // NoCompression indicates no compression should be used
    NoCompression CompressionType = iota
    // GzipCompression indicates gzip compression should be used
    GzipCompression
    // LZWCompression indicates LZW compression should be used
    LZWCompression
)

var (
    gzipPool = sync.Pool{
        New: func() interface{} {
            buf := new(bytes.Buffer)
            buf.Grow(1024) // Pre-allocate some space
            return buf
        
        },
    }
)

// init initializes the gzip pool
func init() {
    runtime.SetFinalizer(gzipPool.Get().(*bytes.Buffer), func(buf *bytes.Buffer) {
        buf.Reset()
    })
}

// compressBlock compresses a block of data using the specified compression type
func compressBlock(data []byte) []byte {
    // Get compression type from configuration or environment
    compressionType := GzipCompression

    switch compressionType {
    case NoCompression:
        return data
    case GzipCompression:
        return compressGzip(data)
    case LZWCompression:
        return compressLZW(data)
    default:
        return data
    }
}

// decompressBlock decompresses a block of data
func decompressBlock(data []byte) []byte {
    // We need to determine which compression was used
    // One way is to add a header byte to the compressed data
    if len(data) == 0 {
        return data
    }

    // we'll always use Gzip
    // In a production system,  need to store the compression type with the data
    return decompressGzip(data)
}

// compressGzip compresses data using gzip
func compressGzip(data []byte) []byte {
    buf := gzipPool.Get().(*bytes.Buffer)
    buf.Reset()
    defer gzipPool.Put(buf)

    gw, err := gzip.NewWriterLevel(buf, gzip.BestSpeed)
    if err != nil {
        // If we can't create the writer, return uncompressed data
        return data
    }

    if _, err := gw.Write(data); err != nil {
        return data
    }

    if err := gw.Close(); err != nil {
        return data
    }

    // Make a copy of the compressed data before returning the buffer to the pool
    result := make([]byte, buf.Len())
    copy(result, buf.Bytes())
    return result
}

// decompressGzip decompresses gzip compressed data
func decompressGzip(data []byte) []byte {
    buf := gzipPool.Get().(*bytes.Buffer)
    buf.Reset()
    defer gzipPool.Put(buf)

    gr, err := gzip.NewReader(bytes.NewReader(data))
    if err != nil {
        return data
    }
    defer gr.Close()

    if _, err := io.Copy(buf, gr); err != nil {
        return data
    }

    result := make([]byte, buf.Len())
    copy(result, buf.Bytes())
    return result
}

// compressLZW compresses data using LZW compression
func compressLZW(data []byte) []byte {
    buf := new(bytes.Buffer)
    w := lzw.NewWriter(buf, lzw.LSB, 8)
    
    if _, err := w.Write(data); err != nil {
        return data
    }
    
    if err := w.Close(); err != nil {
        return data
    }
    
    return buf.Bytes()
}

// decompressLZW decompresses LZW compressed data
func decompressLZW(data []byte) []byte {
    buf := new(bytes.Buffer)
    r := lzw.NewReader(bytes.NewReader(data), lzw.LSB, 8)
    
    if _, err := io.Copy(buf, r); err != nil {
        return data
    }
    
    if err := r.Close(); err != nil {
        return data
    }
    
    return buf.Bytes()
}

// CompressionStats holds statistics about compression
type CompressionStats struct {
    OriginalSize     int64
    CompressedSize   int64
    CompressionRatio float64
    CompressionType  CompressionType
}

// getCompressionStats returns statistics about the compression
func getCompressionStats(original, compressed []byte) CompressionStats {
    originalSize := int64(len(original))
    compressedSize := int64(len(compressed))
    
    var ratio float64
    if originalSize > 0 {
        ratio = float64(compressedSize) / float64(originalSize)
    }
    
    return CompressionStats{
        OriginalSize:     originalSize,
        CompressedSize:   compressedSize,
        CompressionRatio: ratio,
        CompressionType:  GzipCompression, // Currently hardcoded
    }
}

// shouldCompress determines if a block should be compressed based on its content and size
func shouldCompress(data []byte) bool {
    // Don't compress very small blocks
    if len(data) < 64 {
        return false
    }

    // Don't compress if the data appears to be already compressed
    compressibility := estimateCompressibility(data)
    return compressibility > 0.3
}

// estimateCompressibility provides a rough estimate of data compressibility
func estimateCompressibility(data []byte) float64 {
    if len(data) == 0 {
        return 0
    }

    // Take a sample of the data to estimate entropy
    sampleSize := 1024
    if len(data) < sampleSize {
        sampleSize = len(data)
    }

    // Count byte frequency
    freq := make(map[byte]int)
    for i := 0; i < sampleSize; i++ {
        freq[data[i]]++
    }

    // Calculate Shannon entropy
    entropy := 0.0
    sampleSizeFloat := float64(sampleSize)
    for _, count := range freq {
        p := float64(count) / sampleSizeFloat
        entropy -= p * float64(count) // Simple entropy estimation
    }

    // Normalize to 0-1 range
    maxEntropy := float64(len(freq))
    if maxEntropy == 0 {
        return 0
    }

    // Return estimated compressibility
    return 1.0 - (entropy / maxEntropy)
}