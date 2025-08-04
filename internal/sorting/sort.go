package sorting

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/joeymeijers/xmsort/internal/utils"
)

func CompareLines(a, b string, keys []SortKey, delimiter string) bool {
	for _, key := range keys {
		fieldA := ExtractField(a, key, delimiter)
		fieldB := ExtractField(b, key, delimiter)

		if key.Numeric {
			numA, _ := strconv.ParseFloat(fieldA, 64)
			numB, _ := strconv.ParseFloat(fieldB, 64)
			if numA != numB {
				if key.Asc {
					return numA < numB
				}
				return numA > numB
			}
		} else {
			if fieldA != fieldB {
				if key.Asc {
					return fieldA < fieldB
				}
				return fieldA > fieldB
			}
		}
	}
	return false
}

// sortLines sorts a batch of lines based on the provided sort keys.
func SortLines(lines []string, keys []SortKey, delimiter string) {
	sort.Slice(lines, func(i, j int) bool {
		return CompareLines(lines[i], lines[j], keys, delimiter)
	})
}

// extractField extracts a field from a line based on the provided sort key and delimiter.
// If delimiter is not empty, split the line and use the column as field.
// Otherwise, fall back to fixed position (Start, Length).
func ExtractField(line string, key SortKey, delimiter string) string {
	line = strings.TrimSpace(line)
	if delimiter != "" {
		cols := strings.Split(line, delimiter)
		// Interpret key.Start as the column index (0-based)
		if key.Start >= len(cols) {
			return ""
		}
		val := cols[key.Start]
		if key.Length > 0 && key.Length < len(val) {
			return val[:key.Length]
		}
		return val
	}
	// fallback: fixed position
	if key.Start >= len(line) {
		return ""
	}
	end := min(key.Start+key.Length, len(line))
	return line[key.Start:end]
}

func SplitFileAndSort(inputFile string, chunkSize int, sortKeys []SortKey, tempDir string, delimiter string) ([]string, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer utils.SafeClose(file)

	var (
		errOnce    sync.Once
		exitErr    error
		chunkFiles []string
		lines      []string
		wg         sync.WaitGroup
	)

	reader := bufio.NewReader(file)
	chunkIndex := 0
	chunkChan := make(chan string, 10)
	maxWorkers := runtime.NumCPU()
	sem := make(chan struct{}, maxWorkers)

	go func() {
		for chunkFile := range chunkChan {
			chunkFiles = append(chunkFiles, chunkFile)
		}
	}()

	// Schat totaal aantal regels met estimateLineCount
	totalLinesEstimate := utils.EstimateLineCount(inputFile)
	bar := pb.StartNew(totalLinesEstimate)
	bar.SetWriter(os.Stdout)

	totalLines := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 || totalLines == 0 {
					lines = append(lines, strings.TrimRight(line, "\r\n"))
					totalLines++
					bar.Increment()
				}
				break
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		lines = append(lines, line)
		totalLines++
		bar.Increment()

		if len(lines) >= chunkSize {
			wg.Add(1)
			sem <- struct{}{}
			go func(lines []string, chunkIndex int) {
				defer wg.Done()
				defer func() { <-sem }()
				SortLines(lines, sortKeys, delimiter)
				chunkFile, err := utils.WriteChunk(lines, chunkIndex, tempDir)
				if err != nil {
					errOnce.Do(func() { exitErr = err })
					return
				}
				chunkChan <- chunkFile
			}(lines, chunkIndex)
			lines = nil
			chunkIndex++
		}
	}

	if len(lines) > 0 {
		wg.Add(1)
		sem <- struct{}{}
		go func(lines []string, chunkIndex int) {
			defer wg.Done()
			defer func() { <-sem }()
			SortLines(lines, sortKeys, delimiter)
			chunkFile, err := utils.WriteChunk(lines, chunkIndex, tempDir)
			if err != nil {
				errOnce.Do(func() { exitErr = err })
				return
			}
			chunkChan <- chunkFile
		}(lines, chunkIndex)
	}

	wg.Wait()
	close(chunkChan)
	bar.Finish()

	if exitErr != nil {
		return nil, exitErr
	}

	utils.LogInfo("Total lines read: %d", totalLines)
	return chunkFiles, nil
}
