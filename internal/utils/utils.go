package utils

import (
	"bufio"
	"os"
)

const MAX_CHUNK_SIZE = 1_000_000
const MIN_CHUNK_SIZE = 5_000
const MAX_OPEN_FILES = 128 // Safe limit for Windows and other platforms

// estimateLineCount estimates the number of lines in a file by sampling up to 200 lines.
func EstimateLineCount(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return 1000000 // fallback
	}
	defer SafeClose(file)

	reader := bufio.NewReader(file)
	var totalSize, lines int
	for lines < 200 {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		totalSize += len(line)
		lines++
	}
	if lines == 0 {
		return 1000000
	}
	avg := float64(totalSize) / float64(lines)

	fi, err := os.Stat(filename)
	if err != nil {
		return 1000000
	}

	return int(float64(fi.Size()) / avg)
}

// getMaxOpenFiles returns a safe number of files that can be opened concurrently.
func GetMaxOpenFiles() int {
	return MAX_OPEN_FILES
}

func RemoveDuplicates(lines []string) []string {
	if len(lines) == 0 {
		return lines
	}
	result := []string{lines[0]}
	for i := 1; i < len(lines); i++ {
		if lines[i] != lines[i-1] {
			result = append(result, lines[i])
		}
	}
	return result
}

