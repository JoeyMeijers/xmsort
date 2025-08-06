package sorting

import (
	"fmt"
	"strings"
)

// SortKey definieert een sorteerregel op basis van startpositie, lengte, type en volgorde.
type SortKey struct {
	Start   int
	Length  int
	Numeric bool
	Asc     bool
}

func (s SortKey) String() string {
	order := "asc"
	if !s.Asc {
		order = "desc"
	}
	typ := "ascii"
	if s.Numeric {
		typ = "numeric"
	}
	return fmt.Sprintf("start=%d, len=%d, %s, %s", s.Start, s.Length, typ, order)
}

// SortKeySlice voor meerdere SortKeys, implementeert flag.Value
type SortKeySlice []SortKey

func (s *SortKeySlice) String() string {
	keys := []string{}
	for _, key := range *s {
		keys = append(keys, key.String())
	}
	return strings.Join(keys, "; ")
}

func (s *SortKeySlice) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 4 {
		return fmt.Errorf("invalid sortkey format: %s, expected format: start,length,numeric,asc", value)
	}

	var key SortKey
	_, err := fmt.Sscanf(parts[0], "%d", &key.Start)
	if err != nil {
		return fmt.Errorf("invalid start value: %s", parts[0])
	}

	_, err = fmt.Sscanf(parts[1], "%d", &key.Length)
	if err != nil {
		return fmt.Errorf("invalid length value: %s", parts[1])
	}

	key.Numeric = parts[2] == "true"
	key.Asc = parts[3] == "true"

	*s = append(*s, key)
	return nil
}

// FieldKey definieert een sorteersleutel op basis van veldindex ipv startpositie
type FieldKey struct {
	Field   int
	Numeric bool
	Asc     bool
}

func (k FieldKey) String() string {
	order := "asc"
	if !k.Asc {
		order = "desc"
	}
	typ := "ascii"
	if k.Numeric {
		typ = "numeric"
	}
	return fmt.Sprintf("field=%d, %s, %s", k.Field, typ, order)
}

type FieldKeySlice []FieldKey

func (f *FieldKeySlice) String() string {
	keys := []string{}
	for _, key := range *f {
		keys = append(keys, key.String())
	}
	return strings.Join(keys, "; ")
}

func (f *FieldKeySlice) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 3 {
		return fmt.Errorf("invalid keyfield format: %s, expected format: field,numeric,asc", value)
	}

	var key FieldKey
	_, err := fmt.Sscanf(parts[0], "%d", &key.Field)
	if err != nil {
		return fmt.Errorf("invalid field value: %s", parts[0])
	}

	key.Numeric = parts[1] == "true"
	key.Asc = parts[2] == "true"

	*f = append(*f, key)
	return nil
}

// Helper om FieldKeySlice om te zetten naar SortKeySlice
func ConvertFieldKeysToSortKeys(fields FieldKeySlice) SortKeySlice {
	var keys SortKeySlice
	for _, fkey := range fields {
		keys = append(keys, SortKey{
			Start:   fkey.Field,
			Length:  0, // eventueel later invullen als relevant
			Numeric: fkey.Numeric,
			Asc:     fkey.Asc,
		})
	}
	return keys
}
