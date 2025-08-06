package sorting

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortKey_String(t *testing.T) {
	key := SortKey{Start: 3, Length: 5, Numeric: true, Asc: false}
	expected := "start=3, len=5, numeric, desc"
	assert.Equal(t, expected, key.String())
}

func TestSortKeySlice_Set_Valid(t *testing.T) {
	var keys SortKeySlice
	err := keys.Set("2,5,true,true")
	assert.NoError(t, err)
	assert.Len(t, keys, 1)
	assert.Equal(t, SortKey{Start: 2, Length: 5, Numeric: true, Asc: true}, keys[0])
}

func TestSortKeySlice_Set_InvalidFormat(t *testing.T) {
	var keys SortKeySlice
	err := keys.Set("bad_input")
	assert.Error(t, err)
}

func TestSortKeySlice_String(t *testing.T) {
	keys := SortKeySlice{
		{Start: 0, Length: 3, Numeric: false, Asc: true},
		{Start: 5, Length: 2, Numeric: true, Asc: false},
	}
	expected := "start=0, len=3, ascii, asc; start=5, len=2, numeric, desc"
	assert.Equal(t, expected, keys.String())
}

func TestFieldKey_String(t *testing.T) {
	key := FieldKey{Field: 1, Numeric: false, Asc: false}
	expected := "field=1, ascii, desc"
	assert.Equal(t, expected, key.String())
}

func TestFieldKeySlice_Set_Valid(t *testing.T) {
	var keys FieldKeySlice
	err := keys.Set("2,true,false")
	assert.NoError(t, err)
	assert.Equal(t, FieldKey{Field: 2, Numeric: true, Asc: false}, keys[0])
}

func TestConvertFieldKeysToSortKeys(t *testing.T) {
	fk := FieldKeySlice{
		{Field: 1, Numeric: true, Asc: false},
	}
	sk := ConvertFieldKeysToSortKeys(fk)
	assert.Equal(t, SortKey{Start: 1, Length: 0, Numeric: true, Asc: false}, sk[0])
}
