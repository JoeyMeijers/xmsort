package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/shirou/gopsutil/mem"
)

// Safeclose closes a io.Closer and logs an error if something fails
func SafeClose(c io.Closer) error {
	if err := c.Close(); err != nil {
		LogWarning("warning: error closing: %v", err)
		return err
	}
	return nil
}

func SafeFlush(f interface{ Flush() error }) error{
	if err := f.Flush(); err != nil {
		LogWarning("warning: flush failed: %v", err)
		return err
	}
	return nil
}

func SafeRemove(path string) error {
	if err := os.Remove(path); err != nil {
		LogWarning("warning: could not remove file %s: %v", path, err)
		return err
	}
	return nil
}

func SafeRemoveAll(path string) error {
	if err := os.RemoveAll(path); err != nil {
		LogWarning("warning: could not remove %s: %v", path, err)
		return err
	}
	return nil
}

// writeChunk writes a chunk of lines to a file.
func WriteChunk(lines []string, index int, tempDir string) (string, error) {
	filename := filepath.Join(tempDir, fmt.Sprintf("chunk_%d.txt", index))
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer SafeClose(file)

	writer := bufio.NewWriter(file)
	newline := GetNewline()
	for _, line := range lines {
		_, err := writer.WriteString(line + newline)
		if err != nil {
			return "", err
		}
	}
	SafeFlush(writer)
	return filename, nil
}

// calculateChunkSize calculates the chunk size based on the average line size and available memory.
func CalculateChunkSize(averageLineSize int) int {
	v, _ := mem.VirtualMemory()

	// Available memory in bytes
	availableMemory := v.Available

	// Reserve a smaller percentage of the available memory for your process
	reservedMemory := availableMemory / 20 // 5% of available memory

	// Calculate the chunk size in number of lines
	chunkSize := int(reservedMemory / uint64(averageLineSize))

	LogInfo("Reserved memory for chunks: %.2f MB", float64(reservedMemory)/1e6)

	// Ensure the chunk size is not too large or too small
	if chunkSize > MAX_CHUNK_SIZE {
		LogWarning("Chunk size too large, reducing to %v records per chunk", MAX_CHUNK_SIZE)
		chunkSize = MAX_CHUNK_SIZE
	} else if chunkSize < MIN_CHUNK_SIZE {
		LogWarning("Chunk size too small, increasing to %v records per chunk to avoid overhead", MIN_CHUNK_SIZE)
		chunkSize = MIN_CHUNK_SIZE
	}

	return chunkSize
}

// estimateAverageLineSize estimates the average line size based on a sample from the file.
func EstimateAverageLineSize(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return 0 // Fallback
	}
	defer SafeClose(file)

	reader := bufio.NewReader(file)
	var totalSize int
	var count int

	for count < 100 { // Sample n lines
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		totalSize += len(line)
		count++
	}

	if count == 0 {
		return 0
	}
	return totalSize / count
}

// GetNewline returns platform-native newline string.
func GetNewline() string {
	if runtime.GOOS == "windows" {
		return "\r\n"
	}
	return "\n"
}


func MakeTempDir(tempDir string) (string, error) {
	if tempDir == "" {
		dir, err := os.MkdirTemp("", "sort_chunks")
		if err != nil {
			return "", err
		}
		return dir, nil
	}
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		return "", err
	}
	return tempDir, nil
}