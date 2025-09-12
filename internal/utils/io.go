package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
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