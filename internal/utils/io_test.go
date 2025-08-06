package utils_test

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/utils"
	"github.com/stretchr/testify/require"
)

func init() {
	utils.OverrideLogger(log.New(io.Discard, "", 0))
}

func TestWriteChunk_CreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	lines := []string{"a", "b", "c"}

	filename, err := utils.WriteChunk(lines, 0, tmpDir)
	require.NoError(t, err)

	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.Equal(t, "a\nb\nc\n", string(data))
}

func TestEstimateAverageLineSize(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile, err := os.CreateTemp(tmpDir, "testfile-*.txt")
	require.NoError(t, err)

	content := "short\nmedium line\naveryverylonglinewithtext\n"
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	tmpFile.Close()

	avg := utils.EstimateAverageLineSize(tmpFile.Name())
	require.True(t, avg > 0)
}

func TestCalculateChunkSize_ReturnsWithinBounds(t *testing.T) {
	size := utils.CalculateChunkSize(100)
	require.GreaterOrEqual(t, size, utils.MIN_CHUNK_SIZE)
	require.LessOrEqual(t, size, utils.MAX_CHUNK_SIZE)
}
