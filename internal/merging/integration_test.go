package merging

import (
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/stretchr/testify/assert"
)

func TestMultiLevelMergeIntegration(t *testing.T) {
	chunk1 := createTempFile(t, "orange,2\nbanana,5\n")
	chunk2 := createTempFile(t, "apple,1\n")
	defer os.Remove(chunk1)
	defer os.Remove(chunk2)

	outputFile, err := os.CreateTemp("", "merged_output_*.txt")
	assert.NoError(t, err)
	outputFilePath := outputFile.Name()
	outputFile.Close()
	defer os.Remove(outputFilePath)

	keys := []sorting.SortKey{
		{Start: 0, Length: 6, Numeric: false, Asc: false},
	}
	err = MultiLevelMerge(outputFilePath, []string{chunk1, chunk2}, keys, ",", 2, "")
	assert.NoError(t, err)

	data, err := os.ReadFile(outputFilePath)
	assert.NoError(t, err)
	lines := string(data)
	assert.Contains(t, lines, "apple,1")
	assert.Contains(t, lines, "banana,5")
	assert.Contains(t, lines, "orange,2")
}
