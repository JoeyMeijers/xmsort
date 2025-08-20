package config

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/joeymeijers/xmsort/internal/sorting"
)

var ExitFunc = os.Exit

type Config struct {
	InputFile     string
	OutputFile    string
	SortKeys      sorting.SortKeySlice
	FieldSortKeys sorting.FieldKeySlice
	Delimiter     string
	TestFile      int

	// XsSort extra params
	RecordLength     int    // RL=nn
	RecordType       string // RT={V|F}
	TruncateSpaces   bool   // TS={Y|N}
	RemoveDuplicates bool   // RD={Y|N}
	EmptyNumbers     string // EN={Z|E}
	TempDir          string // TMP=...
	Memory           string // MEM=...
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

func ParseXSSortParams(params string) Config {
	cfg := Config{}

	parts := strings.Fields(params)
	sortKeyRegex := regexp.MustCompile(`s\d+=\((.*?)\)`)

	for _, part := range parts {
		switch {
		case strings.HasPrefix(strings.ToUpper(part), "I="):
			cfg.InputFile = strings.TrimPrefix(part, "I=")
		case strings.HasPrefix(strings.ToUpper(part), "O="):
			cfg.OutputFile = strings.TrimPrefix(part, "O=")
		case strings.HasPrefix(strings.ToUpper(part), "RL="):
			fmt.Sscanf(strings.TrimPrefix(part, "RL="), "%d", &cfg.RecordLength)
		case strings.HasPrefix(strings.ToUpper(part), "RT="):
			cfg.RecordType = strings.TrimPrefix(part, "RT=")
		case strings.HasPrefix(strings.ToUpper(part), "TS="):
			val := strings.TrimPrefix(part, "TS=")
			cfg.TruncateSpaces = (strings.ToUpper(val) == "Y" || strings.ToUpper(val) == "YES")
		case strings.HasPrefix(strings.ToUpper(part), "RD="):
			val := strings.TrimPrefix(part, "RD=")
			cfg.RemoveDuplicates = (strings.ToUpper(val) == "Y" || strings.ToUpper(val) == "YES")
		case strings.HasPrefix(strings.ToUpper(part), "EN="):
			cfg.EmptyNumbers = strings.TrimPrefix(part, "EN=") // ZERO/ERROR
		case strings.HasPrefix(strings.ToUpper(part), "TMP=") ||
			strings.HasPrefix(strings.ToUpper(part), "TEMPDIR="):
			cfg.TempDir = strings.SplitN(part, "=", 2)[1]
		case strings.HasPrefix(strings.ToUpper(part), "MEM="):
			cfg.Memory = strings.TrimPrefix(part, "MEM=")

		// Sorteersleutels
		case sortKeyRegex.MatchString(strings.ToLower(part)):
			m := sortKeyRegex.FindStringSubmatch(part)
			if len(m) > 1 {
				args := strings.Split(m[1], ",")
				var start, length int
				numeric := false
				asc := true
				for _, arg := range args {
					kv := strings.SplitN(arg, "=", 2)
					if len(kv) != 2 {
						continue
					}
					key, val := strings.ToLower(strings.TrimSpace(kv[0])), strings.ToLower(strings.TrimSpace(kv[1]))
					switch key {
					case "e":
						fmt.Sscanf(val, "%d", &start)
					case "l":
						fmt.Sscanf(val, "%d", &length)
					case "g":
						if val == "numeric" {
							numeric = true
						}
					case "v":
						if val == "d" {
							asc = false
						}
					}
				}
				cfg.SortKeys = append(cfg.SortKeys, sorting.SortKey{
					Start:   start,
					Length:  length,
					Numeric: numeric,
					Asc:     asc,
				})
			}
		}
	}

	return cfg
}
