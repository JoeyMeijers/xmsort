package utils

import (
	"io"
	"os"
)

// Safeclose closes a io.Closer and logs an error if something fails
func SafeClose(c io.Closer) {
	if err := c.Close(); err != nil {
		LogWarning("warning: error closing: %v", err)
	}
}

func SafeFlush(f interface{ Flush() error }) {
	if err := f.Flush(); err != nil {
		LogWarning("warning: flush failed: %v", err)
	}
}

func SafeRemove(path string) {
	if err := os.Remove(path); err != nil {
		LogWarning("warning: could not remove file %s: %v", path, err)
	}
}

func SafeRemoveAll(path string) {
	if err := os.RemoveAll(path); err != nil {
		LogWarning("warning: could not remove %s: %v", path, err)
	}
}
