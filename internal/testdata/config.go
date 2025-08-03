package testdata

import (
	"flag"
	"github.com/joeymeijers/xmsort/internal/utils"
	"os"
)

type Config struct {
	OutputFile string
	Records    int
}

func ParseFlags() Config {
	cfg := Config{}

	// Test file
	flag.StringVar(&cfg.OutputFile, "outputfile", "test_data.txt", "Output file path")
	flag.IntVar(&cfg.Records, "records", 1000, "Number or lines for test file")

	// main vars
	flag.Parse()

	if cfg.Records <= 0 {
		utils.LogError("Records must be > 0")
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}
