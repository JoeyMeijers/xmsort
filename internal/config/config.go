package config

import (
	"flag"
	"fmt"
	"github.com/joeymeijers/xmsort/internal/sorting"
	"os"
)

var ExitFunc = os.Exit

type Config struct {
	InputFile     string
	OutputFile    string
	SortKeys      sorting.SortKeySlice
	FieldSortKeys sorting.FieldKeySlice
	Delimiter     string
	TestFile      int
}

func ParseFlags() Config {
	cfg := Config{}

	flag.IntVar(&cfg.TestFile, "testfile", 0, "Number or lines for test file")
	flag.StringVar(&cfg.InputFile, "input", "", "Input file path (required)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file path (required)")
	flag.Var(&cfg.SortKeys, "sortkey", "Sort key (format: start,length,numeric,asc). Can be repeated.")
	flag.Var(&cfg.FieldSortKeys, "keyfield", "Field key (format: field,numeric,asc). Can be repeated.")
	flag.StringVar(&cfg.Delimiter, "delimiter", "", "Delimiter for field parsing")

	flag.Parse()

	// Combine fieldSortKyes to SortKeys
	cfg.SortKeys = append(cfg.SortKeys, sorting.ConvertFieldKeysToSortKeys(cfg.FieldSortKeys)...)

	// Validate
	if cfg.TestFile > 0 {
		return cfg
	}

	if cfg.InputFile == "" || cfg.OutputFile == "" {
		fmt.Println("Error: --input and --output are required arguments.")
		flag.Usage()
		ExitFunc(1)
	}

	if len(cfg.SortKeys) == 0 {
		fmt.Println("Error: At least one --sortkey or --keyfield must be provided.")
		flag.Usage()
		ExitFunc(1)
	}

	return cfg
}
