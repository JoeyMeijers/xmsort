package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/joeymeijers/xmsort/internal/config"
	"github.com/joeymeijers/xmsort/internal/merging"
	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/joeymeijers/xmsort/internal/utils"
)

const MAX_MERGE_BATCH = 100

func main() {
	utils.SetupLogging()
	var cfg config.Config
	if len(os.Args) > 1 && (strings.HasPrefix(os.Args[1], "I=") || strings.HasPrefix(os.Args[1], "O=") || strings.HasPrefix(os.Args[1], "K=") || strings.HasPrefix(os.Args[1], "D=")) {
		cfg = config.ParseXSSortParams(strings.Join(os.Args[1:], " "))
	} else {
		cfg = config.ParseFlags()
	}

	inputFile := cfg.InputFile
	outputFile := cfg.OutputFile
	sortKeys := cfg.SortKeys
	delimiter := cfg.Delimiter
	truncateSpaces := cfg.TruncateSpaces
	removeDuplicates := cfg.RemoveDuplicates

	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		utils.LogError("Input file does not exist!")
		return
	}

	start := time.Now()
	utils.LogInfo("Go external sort")
	utils.LogInfo("Start: %v", start)
	utils.LogInfo("Input file: %v", cfg.InputFile)
	utils.LogInfo("Output file: %v", cfg.OutputFile)
	utils.LogInfo("Sort keys: %v", cfg.SortKeys)
	utils.LogInfo("Delimiter: %v", delimiter)
	utils.LogInfo("Record type: %v", cfg.RecordType)
	utils.LogInfo("Record length: %v", cfg.RecordLength)
	utils.LogInfo("Truncate spaces: %v", cfg.TruncateSpaces)
	utils.LogInfo("Remove duplicates: %v", cfg.RemoveDuplicates)
	utils.LogInfo("Empty numbers: %v", cfg.EmptyNumbers)
	utils.LogInfo("Memory: %v", cfg.Memory)
	utils.LogInfo("Temp dir (config): %v", cfg.TempDir)

	averageLineSize := utils.EstimateAverageLineSize(inputFile)
	utils.LogInfo("Estimated average line size: %v", averageLineSize)
	chunkSize := utils.CalculateChunkSize(averageLineSize)
	utils.LogInfo("Calculated chunk size: %d", chunkSize)

	tempDir, err := os.MkdirTemp("", "sort_chunks")
	if err != nil {
		utils.LogError("Error creating temp directory: %v", err)
		return
	}
	defer utils.SafeRemoveAll(tempDir)
	utils.LogInfo("Temporary directory: %s", tempDir)

	chunkFiles, err := sorting.SplitFileAndSort(
		inputFile,
		chunkSize,
		sortKeys,
		tempDir,
		delimiter,
		truncateSpaces,
		removeDuplicates,
		// cfg.RecordLength,   // <-- toegevoegd
		// cfg.RecordType,     // <-- toegevoegd
	)
	if err != nil {
		utils.LogError("Error splitting file: %v", err)
		return
	}
	utils.LogInfo("Created %d chunk files", len(chunkFiles))

	totalBatches := (len(chunkFiles) + MAX_MERGE_BATCH - 1) / MAX_MERGE_BATCH

	var (
		intermediateFiles []string
		mergeWg           sync.WaitGroup
		mergeErrOnce      sync.Once
		mergeErr          error
		mergeSem          = make(chan struct{}, runtime.NumCPU())
		intermediateMu    sync.Mutex
	)

	for i := 0; i < len(chunkFiles); i += MAX_MERGE_BATCH {
		end := min(i+MAX_MERGE_BATCH, len(chunkFiles))
		mergeWg.Add(1)
		mergeSem <- struct{}{}
		go func(i, end, batch int) {
			defer mergeWg.Done()
			defer func() { <-mergeSem }()
			intermediate := filepath.Join(tempDir, fmt.Sprintf("intermediate_%d.txt", batch))
			tmpFile := filepath.Join(tempDir, fmt.Sprintf("intermediate_%d.tmp", batch))
			utils.LogInfo("Merging batch %d/%d (%d files)", batch+1, totalBatches, end-i)
			err := merging.MergeChunks(tmpFile, chunkFiles[i:end], sortKeys, delimiter)
			if err == nil {
				if _, statErr := os.Stat(tmpFile); statErr == nil {
					err = os.Rename(tmpFile, intermediate)
				} else {
					err = fmt.Errorf("temp file missing before rename: %v", statErr)
				}
			}
			if err != nil {
				mergeErrOnce.Do(func() { mergeErr = err })
				return
			}
			intermediateMu.Lock()
			intermediateFiles = append(intermediateFiles, intermediate)
			intermediateMu.Unlock()
		}(i, end, i/MAX_MERGE_BATCH)
	}
	mergeWg.Wait()

	if mergeErr != nil {
		utils.LogError("Error in batch merge: %v", mergeErr)
		return
	}

	utils.LogInfo("Merging final batch %d/%d (%d files)", totalBatches, totalBatches, len(intermediateFiles))
	err = merging.MergeChunks(outputFile, intermediateFiles, sortKeys, delimiter)
	if err != nil {
		utils.LogError("Error merging intermediate files: %v", err)
		return
	}

	utils.LogInfo("Sorting completed in %v\n", time.Since(start))
}
