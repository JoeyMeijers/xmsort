package utils_test

import (
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEstimateLineCount_SmallFile(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "testfile-*.txt")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	content := "line1\nline2\nline3\n"
	_, err = tmpfile.WriteString(content)
	require.NoError(t, err)

	count := utils.EstimateLineCount(tmpfile.Name())
	assert.GreaterOrEqual(t, count, 3)
}

func TestEstimateLineCount_FileNotFound(t *testing.T) {
	count := utils.EstimateLineCount("non_existent_file.txt")
	assert.Equal(t, 1_000_000, count)
}

func TestEstimateLineCount_EmptyFile(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "emptyfile-*.txt")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	count := utils.EstimateLineCount(tmpfile.Name())
	assert.Equal(t, 1_000_000, count)
}

func TestGetMaxOpenFiles(t *testing.T) {
	assert.Equal(t, 128, utils.GetMaxOpenFiles())
}
