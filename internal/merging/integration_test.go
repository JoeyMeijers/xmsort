package merging

import (
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/stretchr/testify/assert"
)

func TestMergeChunksIntegration(t *testing.T) {
	chunk1 := createTempFile(t, "orange,2\nbanana,5\n")
	chunk2 := createTempFile(t, "apple,1\n")
	defer os.Remove(chunk1)
	defer os.Remove(chunk2)

	outputFile := chunk1 + "_out.txt"
	defer os.Remove(outputFile)

	keys := []sorting.SortKey{
		{Start: 0, Length: 6, Numeric: false, Asc: false},
	}
	err := MergeChunks(outputFile, []string{chunk1, chunk2}, keys, ",")
	assert.NoError(t, err)

	data, err := os.ReadFile(outputFile)
	assert.NoError(t, err)
	lines := string(data)
	assert.Contains(t, lines, "apple")
	assert.Contains(t, lines, "banana")
	assert.Contains(t, lines, "orange")
}
