package merging

import (
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/stretchr/testify/assert"
)

func createTempFile(t *testing.T, content string) string {
	t.Helper()
	tempfile, err := os.CreateTemp("", "chunk-*.txt")
	assert.NoError(t, err)
	_, err = tempfile.WriteString(content)
	assert.NoError(t, err)
	assert.NoError(t, tempfile.Close())
	return tempfile.Name()
}

func TestMultiWayMerge(t *testing.T) {
	content1 := "apple,10\nbanana,5\n"
	content2 := "carrot,3\n"
	file1 := createTempFile(t, content1)
	file2 := createTempFile(t, content2)
	defer os.Remove(file1)
	defer os.Remove(file2)

	keys := []sorting.SortKey{
		{Start: 0, Length: 5, Numeric: false, Asc: true},
	}

	outputFile, err := os.CreateTemp("", "merged-*.txt")
	assert.NoError(t, err)
	outputFilePath := outputFile.Name()
	assert.NoError(t, outputFile.Close())
	defer os.Remove(outputFilePath)

	files := []string{file1, file2}

	err = multiWayMerge(outputFilePath, files, keys, ",")
	assert.NoError(t, err)

	// Read and verify output
	data, err := os.ReadFile(outputFilePath)
	assert.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	expectedLines := []string{"apple,10", "banana,5", "carrot,3"}
	for _, line := range expectedLines {
		assert.Contains(t, lines, line)
	}
	// Check that lines are sorted by the key (first 5 characters ascending)
	sortedLines := make([]string, len(lines))
	copy(sortedLines, lines)
	sort.Slice(sortedLines, func(i, j int) bool {
		return sortedLines[i] < sortedLines[j]
	})
	assert.Equal(t, sortedLines, lines)
}

func TestMultiLevelMerge(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var chunkFiles []string
	var originalLines []string

	// Generate 5 chunks each with 50 lines, shuffled order
	for chunkIdx := 0; chunkIdx < 5; chunkIdx++ {
		var lines []string
		for i := 0; i < 50; i++ {
			// Generate lines like "itemX-Y,Z" where X is chunkIdx, Y is line number in chunk, Z is a number
			lineNum := i + chunkIdx*50
			line := "item" + strconv.Itoa(chunkIdx) + "-" + strconv.Itoa(lineNum) + "," + strconv.Itoa(lineNum%100)
			lines = append(lines, line)
		}
		// Shuffle lines in this chunk
		rand.Shuffle(len(lines), func(i, j int) {
			lines[i], lines[j] = lines[j], lines[i]
		})
		// Add to overall lines for verification
		originalLines = append(originalLines, lines...)
		content := strings.Join(lines, "\n") + "\n"
		f := createTempFile(t, content)
		defer os.Remove(f)
		chunkFiles = append(chunkFiles, f)
	}

	// Sort numerically on the part after the comma (the second field)
	keys := []sorting.SortKey{
		{Start: 0, Length: 0, Numeric: true, Asc: true}, // let CompareLines pick the numeric field
	}

	outputFile, err := os.CreateTemp("", "merged-*.txt")
	assert.NoError(t, err)
	outputFilePath := outputFile.Name()
	assert.NoError(t, outputFile.Close())
	defer os.Remove(outputFilePath)

	// Use batch size 2 to force multiple merge levels
	err = MultiLevelMerge(outputFilePath, chunkFiles, keys, ",", 2, "")
	assert.NoError(t, err)

	// Read and verify output
	data, err := os.ReadFile(outputFilePath)
	assert.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	// Check that all original lines are present
	assert.ElementsMatch(t, originalLines, lines)

	// Verify that lines are sorted according to the actual sort keys
	for i := 1; i < len(lines); i++ {
		comp := sorting.CompareLines(lines[i-1], lines[i], keys, ",", false, "")
		assert.LessOrEqual(t, comp, 0, "lines not sorted according to keys: %q > %q", lines[i-1], lines[i])
	}
}
