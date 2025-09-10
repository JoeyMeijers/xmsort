package config

import (
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

func PrintXSSortUsage() {
	fmt.Println("XSSORT parameters:")
	fmt.Println("  I=<file>      Input file")
	fmt.Println("  O=<file>      Output file")
	fmt.Println("  RL=<length>   Record length")
	fmt.Println("  RT=<V|F>      Record type (Variable/Fixed)")
	fmt.Println("  TS=<Y|N>      Truncate spaces")
	fmt.Println("  RD=<Y|N>      Remove duplicates")
	fmt.Println("  EN=<Z|E>      Empty numbers (Zero/Error)")
	fmt.Println("  TMP=<dir>     Temp directory")
	fmt.Println("  MEM=<size>    Sort memory (e.g. 512M)")
	fmt.Println("  S1=(...)      Sort key definition")
	fmt.Println("    Sort key options (S1, S2, ...):")
	fmt.Println("      e=<start>       Start position (0-based)")
	fmt.Println("      l=<length>      Length of field")
	fmt.Println("      g=<type>        Collation type (ebcdic, ascii, numeric)")
	fmt.Println("      v=<A|D>         Ascending (A) or Descending (D)")
	fmt.Println("      p=<start-end>   Alternative way to specify start and length")
	fmt.Println("    Example: S1=(e=0,l=9,g=ebcdic,v=A)")
}

func HasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func ParseXSSortParams(params string) Config {
	cfg := Config{}

	// Split the single string into parts on comma followed by optional space
	var parts []string
	var current strings.Builder
	level := 0
	for _, r := range params {
		switch r {
		case '(':
			level++
		case ')':
			level--
		case ',':
			if level == 0 {
				part := strings.TrimSpace(strings.ReplaceAll(current.String(), "\r", ""))
				if part != "" {
					parts = append(parts, part)
				}
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		part := strings.TrimSpace(strings.ReplaceAll(current.String(), "\r", ""))
		if part != "" {
			parts = append(parts, part)
		}
	}

	// Iterate over each parameter individually (no splitting on spaces or commas)
	sortKeyRegex := regexp.MustCompile(`(?i)^\s*s\d+=\((.*?)\)`)

	for _, part := range parts {
		// remove leading/trailing whitespace and carriage returns
		part = strings.TrimSpace(strings.ReplaceAll(part, "\r", ""))
		if part == "" {
			continue
		}

		switch {
		case strings.HasPrefix(strings.ToUpper(part), "I="):
			cfg.InputFile = strings.TrimSpace(strings.TrimPrefix(part, "I="))
		case strings.HasPrefix(strings.ToUpper(part), "O="):
			cfg.OutputFile = strings.TrimSpace(strings.TrimPrefix(part, "O="))
		case strings.HasPrefix(strings.ToUpper(part), "RL="):
			fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(part, "RL=")), "%d", &cfg.RecordLength)
		case strings.HasPrefix(strings.ToUpper(part), "RT="):
			cfg.RecordType = strings.TrimSpace(strings.TrimPrefix(part, "RT="))
		case strings.HasPrefix(strings.ToUpper(part), "TS="):
			val := strings.TrimSpace(strings.TrimPrefix(part, "TS="))
			cfg.TruncateSpaces = (strings.ToUpper(val) == "Y" || strings.ToUpper(val) == "YES")
		case strings.HasPrefix(strings.ToUpper(part), "RD="):
			val := strings.TrimSpace(strings.TrimPrefix(part, "RD="))
			cfg.RemoveDuplicates = (strings.ToUpper(val) == "Y" || strings.ToUpper(val) == "YES")
		case strings.HasPrefix(strings.ToUpper(part), "EN="):
			cfg.EmptyNumbers = strings.TrimSpace(strings.TrimPrefix(part, "EN="))
		case strings.HasPrefix(strings.ToUpper(part), "TMP=") ||
			strings.HasPrefix(strings.ToUpper(part), "TEMPDIR="):
			cfg.TempDir = strings.TrimSpace(strings.SplitN(part, "=", 2)[1])
		case strings.HasPrefix(strings.ToUpper(part), "MEM="):
			cfg.Memory = strings.TrimSpace(strings.TrimPrefix(part, "MEM="))

		// Sorteersleutels
		case sortKeyRegex.MatchString(part):
			m := sortKeyRegex.FindStringSubmatch(part)
			if len(m) > 1 {
				args := strings.Split(m[1], ",")
				var start, length int
				numeric := false
				asc := true
				collation := ""

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
					case "p":
						var s, e int
						if _, err := fmt.Sscanf(val, "%d-%d", &s, &e); err == nil {
							start = s
							length = e - s + 1
						}
					case "g":
						collation = val
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
					Start:     start,
					Length:    length,
					Numeric:   numeric,
					Asc:       asc,
					Collation: collation,
				})
			}
		}
	}

	// Validate required parameters
	if cfg.InputFile == "" {
		fmt.Println("Error: Input file (I=...) is required.")
		PrintXSSortUsage()
		ExitFunc(1)
	}
	if cfg.OutputFile == "" {
		fmt.Println("Error: Output file (O=...) is required.")
		PrintXSSortUsage()
		ExitFunc(1)
	}
	if cfg.RecordLength == 0 {
		fmt.Println("Error: Record length (RL=...) must be specified and greater than 0.")
		PrintXSSortUsage()
		ExitFunc(1)
	}
	if len(cfg.SortKeys) == 0 {
		fmt.Println("Error: At least one sort key (S1=...) must be specified.")
		PrintXSSortUsage()
		ExitFunc(1)
	}
	return cfg
}
