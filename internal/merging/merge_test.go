package merging

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/cheggaaa/pb/v3"
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

func TestOpenChunkFiles(t *testing.T) {
	chunk1 := createTempFile(t, "apple,10\n5\n")
	chunk2 := createTempFile(t, "carrot,3\n")
	defer os.Remove(chunk1)
	defer os.Remove(chunk2)

	chunks := []string{chunk1, chunk2}
	keys := []sorting.SortKey{
		{Start: 0, Length: 5, Numeric: false, Asc: false},
	}

	readers, files, items, err := openChunkFiles(chunks, keys, ",")
	assert.NoError(t, err)
	assert.Len(t, readers, 2)
	assert.Len(t, files, 2)
	assert.Len(t, items, 2)

	for _, f := range files {
		f.Close()
	}

}

func TestMergeHeapToOutput(t *testing.T) {
	content1 := "apple,10\nbanana,5"
	content2 := "carrot,3\n"

	file1 := createTempFile(t, content1)
	file2 := createTempFile(t, content2)

	keys := []sorting.SortKey{
		{Start: 0, Length: 5, Numeric: false, Asc: true},
	}
	readers, files, items, err := openChunkFiles([]string{file1, file2}, keys, ",")
	assert.NoError(t, err)

	var builder strings.Builder
	writer := bufio.NewWriter(&builder)
	bar := pb.New(3)
	bar.Start()
	err = mergeHeapToOutput(writer, readers, files, items, bar, []string{file1, file2})
	assert.NoError(t, err)

	assert.Contains(t, builder.String(), "apple")
	assert.Contains(t, builder.String(), "banana")
	assert.Contains(t, builder.String(), "carrot")

}
