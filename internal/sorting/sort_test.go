package sorting_test

import (
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/joeymeijers/xmsort/internal/utils"
	"github.com/stretchr/testify/assert"
)

func TestCompareLines_StringAscending(t *testing.T) {
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: false, Asc: true}}
	a := "apple"
	b := "banana"
	result := sorting.CompareLines(a, b, keys, "", false, "Z")
	assert.True(t, result) // "apple" < "banana"
}

func TestCompareLines_NumericDescending(t *testing.T) {
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: true, Asc: false}}
	a := "100"
	b := "50"
	result := sorting.CompareLines(a, b, keys, "", false, "Z")
	assert.True(t, result) // 100 > 50
}

func TestCompareLines_EmptyNumbersZero(t *testing.T) {
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: true, Asc: true}}
	a := ""
	b := "5"
	// EmptyNumbers = "ZERO", dus "" wordt als "0" behandeld
	result := sorting.CompareLines(a, b, keys, "", false, "ZERO")
	assert.True(t, result) // 0 < 5
}

func TestCompareLines_EmptyNumbersError(t *testing.T) {
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: true, Asc: true}}
	a := ""
	b := "5"
	// EmptyNumbers = "ERROR", dus "" veroorzaakt een panic
	assert.Panics(t, func() {
		sorting.CompareLines(a, b, keys, "", false, "ERROR")
	})
}

func TestExtractField_WithDelimiter(t *testing.T) {
	key := sorting.SortKey{Start: 1, Length: 0}
	line := "apple,banana,carrot"
	field := sorting.ExtractField(line, key, ",", false)
	assert.Equal(t, "banana", field)
}

func TestExtractField_FixedWidth(t *testing.T) {
	key := sorting.SortKey{Start: 6, Length: 3}
	line := "apple banana"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "ban", field)
}

func TestExtractField_TooShort(t *testing.T) {
	key := sorting.SortKey{Start: 5, Length: 3}
	line := "abc"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "", field)
}

func TestSortLines(t *testing.T) {
	lines := []string{"zebra", "apple", "monkey"}
	expected := []string{"apple", "monkey", "zebra"}
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: false, Asc: true}}

	sorting.SortLines(lines, keys, "", false, "Z")
	assert.Equal(t, expected, lines)
}

func TestProcessChunk(t *testing.T) {
	lines := []string{"zebra", "apple", "monkey"}
	keys := []sorting.SortKey{{Start: 0, Length: 0, Numeric: false, Asc: true}}

	tmpDir := t.TempDir()
	chunkFile, err := sorting.ProcessChunk(lines, 0, keys, tmpDir, "", false, true, "Z")
	assert.NoError(t, err)
	assert.FileExists(t, chunkFile)

	content, err := os.ReadFile(chunkFile)
	assert.NoError(t, err)
	assert.Contains(t, string(content), "apple")
	assert.Contains(t, string(content), "zebra")
}

func TestExtractField_Delimited_Normal(t *testing.T) {
	key := sorting.SortKey{Start: 1, Length: 0}
	line := "apple,banana,carrot"
	field := sorting.ExtractField(line, key, ",", false)
	assert.Equal(t, "banana", field)
}

func TestExtractField_Delimiter_Sliced(t *testing.T) {
	key := sorting.SortKey{Start: 6, Length: 3}
	line := "apple,banana"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "ban", field)
}

func TestExtractField_FixedWidth_Length(t *testing.T) {
	key := sorting.SortKey{Start: 6, Length: 3}
	line := "apple banana"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "ban", field)
}

func TestExtractField_FixedWidth_ToEnd(t *testing.T) {
	key := sorting.SortKey{Start: 6, Length: 0}
	line := "apple banana"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "banana", field)
}

func TestExtractField_Delimited_ColumnOutOfBounds(t *testing.T) {
	key := sorting.SortKey{Start: 5, Length: 0}
	line := "a,b"
	field := sorting.ExtractField(line, key, ",", false)
	assert.Equal(t, "", field)
}

func TestExtractField_FixedWidth_StartOutOfBounds(t *testing.T) {
	key := sorting.SortKey{Start: 100, Length: 5}
	line := "short"
	field := sorting.ExtractField(line, key, "", false)
	assert.Equal(t, "", field)
}

func TestRemoveDuplicates(t *testing.T) {
	lines := []string{
		"apple",
		"apple",
		"banana",
		"banana",
		"banana",
		"cherry",
		"date",
		"date",
	}
	expected := []string{
		"apple",
		"banana",
		"cherry",
		"date",
	}
	result := utils.RemoveDuplicates(lines)
	assert.Equal(t, expected, result)
}
