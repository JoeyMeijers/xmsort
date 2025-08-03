package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joeymeijers/xmsort/internal/testdata"
	"github.com/joeymeijers/xmsort/internal/utils"

	"github.com/cheggaaa/pb/v3"

	"github.com/shirou/gopsutil/mem"
)

const MAX_CHUNK_SIZE = 1_000_000
const MIN_CHUNK_SIZE = 5_000
const MAX_OPEN_FILES = 128 // Safe limit for Windows and other platforms

// estimateLineCount estimates the number of lines in a file by sampling up to 200 lines.
func estimateLineCount(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return 1000000 // fallback
	}
	defer utils.SafeClose(file)

	reader := bufio.NewReader(file)
	var totalSize, lines int
	for lines < 200 {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		totalSize += len(line)
		lines++
	}
	if lines == 0 {
		return 1000000
	}
	avg := float64(totalSize) / float64(lines)

	fi, err := os.Stat(filename)
	if err != nil {
		return 1000000
	}

	return int(float64(fi.Size()) / avg)
}

func compareLines(a, b string, keys []utils.SortKey, delimiter string) bool {
	for _, key := range keys {
		fieldA := extractField(a, key, delimiter)
		fieldB := extractField(b, key, delimiter)

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
func sortLines(lines []string, keys []utils.SortKey, delimiter string) {
	sort.Slice(lines, func(i, j int) bool {
		return compareLines(lines[i], lines[j], keys, delimiter)
	})
}

// extractField extracts a field from a line based on the provided sort key and delimiter.
// If delimiter is not empty, split the line and use the column as field.
// Otherwise, fall back to fixed position (Start, Length).
func extractField(line string, key utils.SortKey, delimiter string) string {
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

func splitFile(inputFile string, chunkSize int, sortKeys []utils.SortKey, tempDir string, delimiter string) ([]string, error) {
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
	totalLinesEstimate := estimateLineCount(inputFile)
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
				sortLines(lines, sortKeys, delimiter)
				chunkFile, err := writeChunk(lines, chunkIndex, tempDir)
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
			sortLines(lines, sortKeys, delimiter)
			chunkFile, err := writeChunk(lines, chunkIndex, tempDir)
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

// writeChunk writes a chunk of lines to a file.
func writeChunk(lines []string, index int, tempDir string) (string, error) {
	filename := filepath.Join(tempDir, fmt.Sprintf("chunk_%d.txt", index))
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer utils.SafeClose(file)

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return "", err
		}
	}
	utils.SafeFlush(writer)
	return filename, nil
}

// heapItem represents an element in the heap used for merging chunks.
type heapItem struct {
	line      string
	fileID    int
	sortKeys  []utils.SortKey
	delimiter string
}

// minHeap is a min-heap of heapItems.
type minHeap []heapItem

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return compareLines(h[i].line, h[j].line, h[i].sortKeys, h[i].delimiter)
}

func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x any) {
	*h = append(*h, x.(heapItem))
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// getMaxOpenFiles returns a safe number of files that can be opened concurrently.
func getMaxOpenFiles() int {
	return MAX_OPEN_FILES
}

func mergeChunks(outputFile string, chunkFiles []string, sortKeys []utils.SortKey, delimiter string) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer utils.SafeClose(out)

	var (
		errOnce sync.Once
		exitErr error
	)

	writer := bufio.NewWriterSize(out, 16*1024*1024)

	minHeap := &minHeap{}
	heap.Init(minHeap)

	maxOpenFiles := getMaxOpenFiles()
	readers := make([]*bufio.Reader, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	openSem := make(chan struct{}, maxOpenFiles)
	var openWg sync.WaitGroup
	var wg sync.WaitGroup

	// Schat totaalregels
	var totalExpectedLines int
	for _, path := range chunkFiles {
		totalExpectedLines += estimateLineCount(path)
	}

	bar := pb.StartNew(totalExpectedLines)
	bar.SetWriter(os.Stdout)

	heapItemChan := make(chan heapItem, len(chunkFiles))

	for i := range chunkFiles {
		openWg.Add(1)
		go func(i int) {
			openSem <- struct{}{}
			defer func() {
				<-openSem
				openWg.Done()
			}()
			f, err := os.Open(chunkFiles[i])
			if err != nil {
				errOnce.Do(func() { exitErr = err })
				return
			}
			files[i] = f
			readers[i] = bufio.NewReader(f)
			line, err := readers[i].ReadString('\n')
			if err != nil && err != io.EOF {
				errOnce.Do(func() { exitErr = err })
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if err != io.EOF || len(line) > 0 {
				heapItemChan <- heapItem{line: line, fileID: i, sortKeys: sortKeys, delimiter: delimiter}
			}
		}(i)
	}

	go func() {
		openWg.Wait()
		close(heapItemChan)
	}()

	for item := range heapItemChan {
		heap.Push(minHeap, item)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for minHeap.Len() > 0 {
			item := heap.Pop(minHeap).(heapItem)
			_, err := writer.WriteString(item.line + "\n")
			if err != nil {
				errOnce.Do(func() { exitErr = err })
				return
			}
			bar.Increment()

			line, err := readers[item.fileID].ReadString('\n')
			if err != nil && err != io.EOF {
				errOnce.Do(func() { exitErr = err })
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if err != io.EOF || len(line) > 0 {
				heap.Push(minHeap, heapItem{line: line, fileID: item.fileID, sortKeys: sortKeys, delimiter: delimiter})
			} else if err == io.EOF {
				utils.SafeClose(files[item.fileID])

			}
		}
		utils.SafeFlush(writer)
		bar.Finish()
	}()

	wg.Wait()

	if exitErr != nil {
		return exitErr
	}

	for _, file := range chunkFiles {
		utils.SafeRemove(file)
	}

	// utils.LogInfo("Output written to: %s", outputFile)

	return nil
}

// calculateChunkSize calculates the chunk size based on the average line size and available memory.
func calculateChunkSize(averageLineSize int) int {
	v, _ := mem.VirtualMemory()

	// Available memory in bytes
	availableMemory := v.Available

	// Reserve a smaller percentage of the available memory for your process
	reservedMemory := availableMemory / 20 // 5% of available memory

	// Calculate the chunk size in number of lines
	chunkSize := int(reservedMemory / uint64(averageLineSize))

	utils.LogInfo("Reserved memory for chunks: %.2f MB", float64(reservedMemory)/1e6)

	// Ensure the chunk size is not too large or too small
	if chunkSize > MAX_CHUNK_SIZE {
		utils.LogWarning("Chunk size too large, reducing to %v records per chunk", MAX_CHUNK_SIZE)
		chunkSize = MAX_CHUNK_SIZE
	} else if chunkSize < MIN_CHUNK_SIZE {
		utils.LogWarning("Chunk size too small, increasing to %v records per chunk to avoid overhead", MIN_CHUNK_SIZE)
		chunkSize = MIN_CHUNK_SIZE
	}

	return chunkSize
}

// estimateAverageLineSize estimates the average line size based on a sample from the file.
func estimateAverageLineSize(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		return 0 // Fallback
	}
	defer utils.SafeClose(file)

	reader := bufio.NewReader(file)
	var totalSize int
	var count int

	for count < 100 { // Sample n lines
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		totalSize += len(line)
		count++
	}

	if count == 0 {
		return 0
	}
	return totalSize / count
}

// main is the entry point of the program.
func main() {
	utils.SetupLogging()
	config := utils.ParseFlags()

	if config.TestFile > 0 {
		utils.LogInfo("Generating test file with %d lines", config.TestFile)
		testdata.GenerateTestFile(config.TestFile)
		return
	}

	inputFile := config.InputFile
	outputFile := config.OutputFile
	sortKeys := config.SortKeys
	delimiter := config.Delimiter

	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		utils.LogError("Input file does not exist!")
		return
	}

	start := time.Now()
	utils.LogInfo("Go external sort")
	utils.LogInfo("Start: %v", start)
	utils.LogInfo("Input file: %v", config.InputFile)
	utils.LogInfo("Output file: %v", config.OutputFile)
	utils.LogInfo("Sort keys: %v", config.SortKeys)
	utils.LogInfo("Delimiter: %v", delimiter)

	averageLineSize := estimateAverageLineSize(inputFile)
	utils.LogInfo("Estimated average line size: %v", averageLineSize)
	chunkSize := calculateChunkSize(averageLineSize)
	utils.LogInfo("Calculated chunk size: %d", chunkSize)

	tempDir, err := os.MkdirTemp("", "sort_chunks")
	if err != nil {
		utils.LogError("Error creating temp directory: %v", err)
		return
	}
	defer utils.SafeRemoveAll(tempDir)
	utils.LogInfo("Temporary directory: %s", tempDir)

	chunkFiles, err := splitFile(inputFile, chunkSize, sortKeys, tempDir, delimiter)
	if err != nil {
		utils.LogError("Error splitting file: %v", err)
		return
	}
	utils.LogInfo("Created %d chunk files", len(chunkFiles))

	const MAX_MERGE_BATCH = 100
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
			err := mergeChunks(tmpFile, chunkFiles[i:end], sortKeys, delimiter)
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
	err = mergeChunks(outputFile, intermediateFiles, sortKeys, delimiter)
	if err != nil {
		utils.LogError("Error merging intermediate files: %v", err)
		return
	}

	utils.LogInfo("Sorting completed in %v\n", time.Since(start))
}
