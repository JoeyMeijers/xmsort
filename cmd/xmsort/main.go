package main

import (
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/joeymeijers/xmsort/internal/config"
	"github.com/joeymeijers/xmsort/internal/merging"
	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/joeymeijers/xmsort/internal/utils"
)

func main() {

	if len(os.Args) == 1 {
		config.PrintXMSortUsage()
		os.Exit(1)
	}

	utils.SetupLogging()
	cfg := config.ParseXSSortParams(strings.Join(os.Args[1:], " "))

	inputFile := cfg.InputFile
	outputFile := cfg.OutputFile
	sortKeys := cfg.SortKeys
	delimiter := cfg.Delimiter
	truncateSpaces := cfg.TruncateSpaces
	removeDuplicates := cfg.RemoveDuplicates
	emptyNumbers := cfg.EmptyNumbers
	recordType := strings.ToUpper(cfg.RecordType)
	recordLength := cfg.RecordLength

	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		utils.LogError("Input file does not exists: %s", inputFile)
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
	memLimit, err := utils.ParseMemoryString(cfg.Memory)
	if err != nil {
		utils.LogError("Error parsing memory string: %v", err)
		os.Exit(1)
	}
	chunkSize := utils.CalculateChunkSize(averageLineSize, memLimit)
	utils.LogInfo("Calculated chunk size: %d", chunkSize)

	tempDir, err := utils.MakeTempDir(cfg.TempDir)
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
		emptyNumbers,
		recordLength,
		recordType,
	)
	if err != nil {
		utils.LogError("Error splitting file: %v", err)
		return
	}
	utils.LogInfo("Created %d chunk files", len(chunkFiles))

	if len(chunkFiles) == 1 {
		utils.LogInfo("Only one chunk created, skipping merge")
		err := os.Rename(chunkFiles[0], outputFile)
		if err != nil {
			utils.LogError("Error renaming chunk file to output file: %v", err)
			return
		}
		utils.LogInfo("Sorting completed in %v\n", time.Since(start))
		return
	}

	utils.LogInfo("Performing multi-level merge of %d chunk files...", len(chunkFiles))
	err = merging.MultiLevelMerge(outputFile, chunkFiles, sortKeys, delimiter, runtime.NumCPU(), tempDir)
	if err != nil {
		utils.LogError("Error during multi-level merge: %v", err)
		return
	}

	utils.LogInfo("Sorting completed in %v\n", time.Since(start))
}
