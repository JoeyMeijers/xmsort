package config_test

import (
	"flag"
	"os"
	"testing"

	"github.com/joeymeijers/xmsort/internal/config"
)

func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestParseFlags_ValidInput(t *testing.T) {
	resetFlags()
	os.Args = []string{
		"cmd",
		"--input=test.txt",
		"--output=out.txt",
		"--sortkey=0,5,false,true",
	}

	cfg := config.ParseFlags()

	if cfg.InputFile != "test.txt" {
		t.Errorf("expected input file to be 'test.txt', got '%s'", cfg.InputFile)
	}
	if cfg.OutputFile != "out.txt" {
		t.Errorf("expected output file to be 'out.txt', got '%s'", cfg.OutputFile)
	}
	if len(cfg.SortKeys) != 1 {
		t.Errorf("expected 1 sortkey, got %d", len(cfg.SortKeys))
	}
}

func TestParseFlags_MissingRequiredArgs(t *testing.T) {
	resetFlags()
	os.Args = []string{
		"cmd",
		"--input=test.txt",
	}

	var exitCode int
	config.ExitFunc = func(code int) {
		exitCode = code
		panic("mock exit")
	}
	defer func() {
		config.ExitFunc = os.Exit // herstel na test
		if r := recover(); r == nil {
			t.Fatal("expected panic from mock exit")
		}
		if exitCode != 1 {
			t.Errorf("expected exit code 1, got %d", exitCode)
		}
	}()

	config.ParseFlags()
	t.Fatal("expected ParseFlags to call ExitFunc")
}

func TestParseFlags_TestFileMode(t *testing.T) {
	resetFlags()
	os.Args = []string{
		"cmd",
		"--testfile=100",
	}

	cfg := config.ParseFlags()

	if cfg.TestFile != 100 {
		t.Errorf("expected TestFile=100, got %d", cfg.TestFile)
	}
}


func TestParseXSSortParams(t *testing.T) {
	params := `I=input.txt O=output.txt RL=122 RT=V TS=Y RD=N EN=Z TMP=/tmp MEM=256M s1=(e=1,l=9,g=numeric,v=a) s2=(e=23,l=30,g=ebcdic,v=d)`
	cfg := config.ParseXSSortParams(params)

	if cfg.InputFile != "input.txt" {
		t.Errorf("expected input.txt, got %s", cfg.InputFile)
	}
	if cfg.OutputFile != "output.txt" {
		t.Errorf("expected output.txt, got %s", cfg.OutputFile)
	}
	if cfg.RecordLength != 122 {
		t.Errorf("expected RL=122, got %d", cfg.RecordLength)
	}
	if !cfg.TruncateSpaces {
		t.Errorf("expected TS=Y -> true")
	}
	if cfg.RemoveDuplicates {
		t.Errorf("expected RD=N -> false")
	}
	if len(cfg.SortKeys) != 2 {
		t.Fatalf("expected 2 sort keys, got %d", len(cfg.SortKeys))
	}
	if cfg.SortKeys[0].Start != 1 || cfg.SortKeys[0].Length != 9 || !cfg.SortKeys[0].Asc {
		t.Errorf("sortkey1 parsed wrong: %+v", cfg.SortKeys[0])
	}
	if cfg.SortKeys[1].Asc {
		t.Errorf("sortkey2 should be descending")
	}
}