package config_test

import (
	"testing"

	"github.com/joeymeijers/xmsort/internal/config"
)



func TestParseXSSortParams(t *testing.T) {
	params := `I=input.txt, O=output.txt, RL=122, RT=V, TS=Y, RD=N, EN=Z, TMP=/tmp, MEM=256M, S1=(e=1,l=9,g=numeric,v=a), S2=(e=23,l=30,g=ebcdic,v=d)`
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

func TestParseXSSortParams_PStyle(t *testing.T) {
	params := `I=input.txt, O=output.txt, RL=100, S1=(P=1-9,V=A), S2=(P=20-30,V=D)`
	cfg := config.ParseXSSortParams(params)

	if cfg.RecordLength != 100 {
		t.Errorf("expected RL=100, got %d", cfg.RecordLength)
	}
	if len(cfg.SortKeys) != 2 {
		t.Fatalf("expected 2 sort keys, got %d", len(cfg.SortKeys))
	}
	if cfg.SortKeys[0].Start != 1 || cfg.SortKeys[0].Length != 9 {
		t.Errorf("sortkey1 parsed wrong: %+v", cfg.SortKeys[0])
	}
	if cfg.SortKeys[1].Start != 20 || cfg.SortKeys[1].Length != 11 || cfg.SortKeys[1].Asc {
		t.Errorf("sortkey2 parsed wrong: %+v", cfg.SortKeys[1])
	}
}

func TestParseXSSortParams_UppercaseS(t *testing.T) {
	params := `I=in.txt, O=out.txt, RL=50, S1=(e=0,l=5,g=ascii,v=a), S2=(e=10,l=5,g=numeric,v=d)`
	cfg := config.ParseXSSortParams(params)

	if len(cfg.SortKeys) != 2 {
		t.Fatalf("expected 2 sort keys, got %d", len(cfg.SortKeys))
	}
	if !cfg.SortKeys[0].Asc || cfg.SortKeys[1].Asc {
		t.Errorf("sort keys direction parsed incorrectly: %+v", cfg.SortKeys)
	}
}

func TestParseXSSortParams_MissingOptionalParams(t *testing.T) {
	params := `I=file1.txt, O=file2.txt, RL=200, S1=(e=0,l=10,g=ascii,v=a)`
	cfg := config.ParseXSSortParams(params)

	if cfg.InputFile != "file1.txt" || cfg.OutputFile != "file2.txt" {
		t.Errorf("input/output file parsed wrong: %s/%s", cfg.InputFile, cfg.OutputFile)
	}
	if !cfg.TruncateSpaces {
		// Default value
		t.Logf("TS not set, default false")
	}
	if cfg.RemoveDuplicates {
		t.Errorf("RD default should be false")
	}
	if len(cfg.SortKeys) != 1 {
		t.Fatalf("expected 1 sort key, got %d", len(cfg.SortKeys))
	}
}

func TestParseXSSortParams_InvalidSortKeyIgnored(t *testing.T) {
	params := `I=in.txt, O=out.txt, RL=50, S1=(e=0,l=5,g=ascii), S2=invalid`
	cfg := config.ParseXSSortParams(params)

	if len(cfg.SortKeys) != 1 {
		t.Fatalf("expected 1 valid sort key, got %d", len(cfg.SortKeys))
	}
}