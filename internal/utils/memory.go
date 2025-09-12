package utils

import (
	"bufio"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/mem"
)

const defaultFraction = 0.5

// calculateChunkSize calculates the chunk size based on the average line size and available memory.
func CalculateChunkSize(averageLineSize int, memoryLimit uint64) int {
	var reservedMemory uint64
	if memoryLimit > 0 {
		reservedMemory = memoryLimit
		LogInfo("Using provided memory limit for chunks: %.2f MB", float64(reservedMemory)/1e6)
	} else {
		v, _ := mem.VirtualMemory()
		var usable uint64
		if runtime.GOOS == "windows" {
			if v.Available < v.Free {
				usable = v.Available
			} else {
				usable = v.Free
			}
		} else {
			usable = v.Available
		}
		reservedMemory = uint64(float64(usable) * defaultFraction)
		LogInfo("Using system memory (%.0f%%) for chunks: %.2f MB", defaultFraction*100, float64(reservedMemory)/1e6)
	}

	// Calculate the chunk size in number of lines
	chunkSize := int(reservedMemory / uint64(averageLineSize))

	// Ensure the chunk size is not too large or too small
	if chunkSize > MAX_CHUNK_SIZE {
		LogInfo("Chunk size too large, reducing to %v records per chunk", MAX_CHUNK_SIZE)
		chunkSize = MAX_CHUNK_SIZE
	} else if chunkSize < MIN_CHUNK_SIZE {
		LogInfo("Chunk size too small, increasing to %v records per chunk to avoid overhead", MIN_CHUNK_SIZE)
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

// ParseMemoryString converts a memory size string like "512M", "2G", "1K", or "123" into a uint64 representing bytes.
// Supported suffixes are none (bytes), K (kilobytes), M (megabytes), G (gigabytes), case-insensitive.
// Returns an error if the format is invalid.
func ParseMemoryString(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty memory string")
	}

	s = strings.TrimSpace(s)
	length := len(s)
	if length == 0 {
		return 0, errors.New("empty memory string")
	}

	lastChar := s[length-1]
	var multiplier uint64 = 1
	numPart := s

	switch {
	case lastChar == 'K' || lastChar == 'k':
		multiplier = 1024
		numPart = s[:length-1]
	case lastChar == 'M' || lastChar == 'm':
		multiplier = 1024 * 1024
		numPart = s[:length-1]
	case lastChar == 'G' || lastChar == 'g':
		multiplier = 1024 * 1024 * 1024
		numPart = s[:length-1]
	default:
		// no suffix, multiplier = 1
	}

	numPart = strings.TrimSpace(numPart)
	if numPart == "" {
		return 0, errors.New("invalid memory string: missing numeric part")
	}

	value, err := strconv.ParseUint(numPart, 10, 64)
	if err != nil {
		return 0, errors.New("invalid memory string: " + err.Error())
	}

	return value * multiplier, nil
}
