package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Configuratie voor de sortering
type SortKey struct {
	Start   int
	Length  int
	Numeric bool
	Asc     bool
	Field   int
}

// SortKeySlice is een custom flag.value voor het parsen van sorteerconfiguraties
type SortKeySlice []SortKey

// String functie voor debugging
func (s *SortKeySlice) String() string {
	keys := []string{}
	for _, key := range *s {
		keys = append(keys, key.String())
	}
	return strings.Join(keys, "; ")
}

// String weergave voor debugging
func (s SortKey) String() string {
	order := "asc"
	if !s.Asc {
		order = "desc"
	}
	typ := "ascii"
	if s.Numeric {
		typ = "numeric"
	}
	return fmt.Sprintf("start=%d, len=%d, %s, %s, field=%d", s.Start, s.Length, typ, order, s.Field)
}

// Set wordt aangeroepen door flag.parse()
func (s *SortKeySlice) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 4 {
		return fmt.Errorf("invalid sortkey format: %s, expected format: start, length, numeric, asc", value)
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

// FieldKeySlice is een custom flag.value voor het parsen van keyfield-configuraties
type FieldKeySlice []SortKey

// String functie voor debugging
func (f *FieldKeySlice) String() string {
	keys := []string{}
	for _, key := range *f {
		keys = append(keys, key.String())
	}
	return strings.Join(keys, "; ")
}

// Set wordt aangeroepen door flag.parse()
func (f *FieldKeySlice) Set(value string) error {
	// Verwacht formaat: field,numeric,asc
	parts := strings.Split(value, ",")
	if len(parts) != 3 {
		return fmt.Errorf("invalid keyfield format: %s, expected format: field,numeric,asc", value)
	}
	var key SortKey
	_, err := fmt.Sscanf(parts[0], "%d", &key.Field)
	if err != nil {
		return fmt.Errorf("invalid field value: %s", parts[0])
	}
	key.Numeric = parts[1] == "true"
	key.Asc = parts[2] == "true"

	*f = append(*f, key)
	return nil
}

func parseFlags() Config {
	cfg := Config{}

	// Test file
	flag.IntVar(&cfg.TestFile, "testfile", 0, "Number or lines for test file")
	// main vars
	flag.StringVar(&cfg.InputFile, "input", "", "Input file path (required)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file path (required)")
	flag.Var(&cfg.SortKeys, "sortkey", "Sort key (format: start,length,numeric,asc). Can be repeated.")
	flag.Var(&cfg.FieldSortKeys, "keyfield", "Field key (format: field,numeric,asc). Can be repeated.")
	flag.StringVar(&cfg.Delimiter, "delimiter", "", "Delimiter for field parsing")

	flag.Parse()

	// if testfile is true then return the config
	if cfg.TestFile > 0 {
		return cfg
	}

	// Validatie: controleer of verplichte argumenten zijn ingevuld
	if cfg.InputFile == "" || cfg.OutputFile == "" {
		fmt.Println("Error: --input and --output are required arguments.")
		flag.Usage()
		os.Exit(1) // Stop het programma met een foutmelding
	}

	// Combineer FieldSortKeys met SortKeys
	for _, fkey := range cfg.FieldSortKeys {
		key := SortKey{
			Field:   fkey.Field,
			Numeric: fkey.Numeric,
			Asc:     fkey.Asc,
		}
		cfg.SortKeys = append(cfg.SortKeys, key)
	}

	// Controleer of er ten minste één sorteersleutel is opgegeven
	if len(cfg.SortKeys) == 0 {
		fmt.Println("Error: At least one --sortkey or --keyfield must be provided.")
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}

type Config struct {
	InputFile     string
	OutputFile    string
	SortKeys     SortKeySlice
	FieldSortKeys FieldKeySlice
	Delimiter    string
	TestFile     int
}
