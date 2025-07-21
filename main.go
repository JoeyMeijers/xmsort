package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"xmsort/testdata"

	"github.com/shirou/gopsutil/mem"
)

const MAX_CHUNK_SIZE = 1_000_000
const MIN_CHUNK_SIZE = 5_000

func compareLines(a, b string, keys []SortKey) bool {
	for _, key := range keys {
		fieldA := extractField(a, key)
		fieldB := extractField(b, key)

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
func sortLines(lines []string, keys []SortKey) {
	sort.Slice(lines, func(i, j int) bool {
		return compareLines(lines[i], lines[j], keys)
	})
}

// extractField extracts a field from a line based on the provided sort key.
func extractField(line string, key SortKey) string {
	line = strings.TrimSpace(line) // Prevent extra newlines
	if key.Start >= len(line) {
		return ""
	}
	end := key.Start + key.Length
	if end > len(line) {
		end = len(line)
	}
	return line[key.Start:end]
}

// splitFile splits a large file into smaller chunks based on the chunk size and sort keys.
func splitFile(inputFile string, chunkSize int, sortKeys []SortKey, tempDir string) ([]string, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunkFiles []string
	reader := bufio.NewReader(file)
	var lines []string
	chunkIndex := 0
	var wg sync.WaitGroup
	chunkChan := make(chan string, 10)
	errChan := make(chan error, 1)

	maxWorkers := runtime.NumCPU()
	sem := make(chan struct{}, maxWorkers)

	go func() {
		for chunkFile := range chunkChan {
			chunkFiles = append(chunkFiles, chunkFile)
		}
	}()

	totalLines := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// Voeg altijd de laatste regel toe, ook als deze leeg is
				if len(line) > 0 || totalLines == 0 {
					lines = append(lines, strings.TrimRight(line, "\r\n"))
					totalLines++
				}
				break
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		lines = append(lines, line)
		totalLines++
		if len(lines) >= chunkSize {
			wg.Add(1)
			sem <- struct{}{} // acquire
			go func(lines []string, chunkIndex int) {
				defer wg.Done()
				defer func() { <-sem }() // release
				sortLines(lines, sortKeys)
				chunkFile, err := writeChunk(lines, chunkIndex, tempDir)
				if err != nil {
					errChan <- err
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
			sortLines(lines, sortKeys)
			chunkFile, err := writeChunk(lines, chunkIndex, tempDir)
			if err != nil {
				errChan <- err
				return
			}
			chunkChan <- chunkFile
		}(lines, chunkIndex)
	}

	wg.Wait()
	close(chunkChan)

	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	logInfo("Total lines read: %d", totalLines)
	return chunkFiles, nil
}

// writeChunk writes a chunk of lines to a file.
func writeChunk(lines []string, index int, tempDir string) (string, error) {
	filename := fmt.Sprintf("%s/chunk_%d.txt", tempDir, index)
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return "", err
		}
	}
	writer.Flush()
	return filename, nil
}

// heapItem represents an element in the heap used for merging chunks.
type heapItem struct {
	line     string
	fileID   int
	sortKeys []SortKey
}

// minHeap is a min-heap of heapItems.
type minHeap []heapItem

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return compareLines(h[i].line, h[j].line, h[i].sortKeys)
}

func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// getMaxOpenFiles returns a safe number of files that can be opened concurrently.
func getMaxOpenFiles() int {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		// Fallback to a safe default if we can't get the limit
		return 64
	}
	// Gebruik een ruime marge, bijvoorbeeld 80% van de limiet, maar nooit meer dan 512
	max := int(rLimit.Cur * 8 / 10)
	if max > 512 {
		max = 512
	}
	if max < 16 {
		max = 16
	}
	return max
}

// mergeChunks merges sorted chunks into a single output file.
func mergeChunks(outputFile string, chunkFiles []string, sortKeys []SortKey, tempDir string) error {
	tempOutputFile := fmt.Sprintf("%s/output.tmp", tempDir)
	out, err := os.Create(tempOutputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriterSize(out, 16*1024*1024)

	minHeap := &minHeap{}
	heap.Init(minHeap)

	maxOpenFiles := getMaxOpenFiles() // <-- Gebruik dynamisch limiet
	readers := make([]*bufio.Reader, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	openSem := make(chan struct{}, maxOpenFiles)
	var openWg sync.WaitGroup
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	totalLines := 0

	// Declare heapItemChan before goroutines use it
	heapItemChan := make(chan heapItem, len(chunkFiles))

	// Open de eerste regel van elk chunk-bestand, maar nooit meer dan maxOpenFiles tegelijk
	for i := 0; i < len(chunkFiles); i++ {
		openWg.Add(1)
		go func(i int) {
			openSem <- struct{}{}
			defer func() {
				<-openSem
				openWg.Done()
			}()
			f, err := os.Open(chunkFiles[i])
			if err != nil {
				errChan <- err
				return
			}
			files[i] = f
			readers[i] = bufio.NewReader(f)
			line, err := readers[i].ReadString('\n')
			if err != nil && err != io.EOF {
				errChan <- err
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if err != io.EOF || len(line) > 0 {
				heapItemChan <- heapItem{line: line, fileID: i, sortKeys: sortKeys}
			}
		}(i)
	}

	// Verzamel heapItems uit goroutines
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
				errChan <- err
				return
			}
			totalLines++
			line, err := readers[item.fileID].ReadString('\n')
			if err != nil && err != io.EOF {
				errChan <- err
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if err != io.EOF || len(line) > 0 {
				heap.Push(minHeap, heapItem{line: line, fileID: item.fileID, sortKeys: sortKeys})
			} else if err == io.EOF {
				files[item.fileID].Close()
			}
		}
		writer.Flush()
	}()

	wg.Wait()

	select {
	case err := <-errChan:
		return err
	default:
	}

	// Verwijder tijdelijke bestanden
	for _, file := range chunkFiles {
		os.Remove(file)
	}

	logInfo("Total lines written: %d", totalLines)

	// Verplaats het tijdelijke output bestand naar de uiteindelijke locatie
	out.Close()
	err = os.Rename(tempOutputFile, outputFile)
	if err != nil {
		return err
	}

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

	logInfo("Reserved memory for chunks: %.2f MB", float64(reservedMemory)/1e6)

	// Ensure the chunk size is not too large or too small
	if chunkSize > MAX_CHUNK_SIZE {
		logWarning("Chunk size too large, reducing to %v records per chunk", MAX_CHUNK_SIZE)
		chunkSize = MAX_CHUNK_SIZE
	} else if chunkSize < MIN_CHUNK_SIZE {
		logWarning("Chunk size too small, increasing to %v records per chunk to avoid overhead", MIN_CHUNK_SIZE)
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
	defer file.Close()

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
	setupLogging()
	config := parseFlags()

	// Check if test file generation is requested
	if config.TestFile > 0 {
		logInfo("Generating test file with %d lines", config.TestFile)
		testdata.GenerateTestFile(config.TestFile)
		return
	}

	inputFile := config.InputFile
	outputFile := config.OutputFile
	sortKeys := config.SortKeys
	// Check if input file exists
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		logError("Input file does not exist!")
		return
	}

	start := time.Now()
	logInfo("Go external sort")
	logInfo("Start: %v", start)
	logInfo("Input file: %v", config.InputFile)
	logInfo("Output file: %v", config.OutputFile)
	logInfo("Sort keys: %v", config.SortKeys)

	var (
		errOnce sync.Once
		exitErr error
	)

	// Dynamically calculate the chunk size
	averageLineSize := estimateAverageLineSize(inputFile)
	logInfo("Estimated average line size: %v", averageLineSize)
	chunkSize := calculateChunkSize(averageLineSize)
	logInfo("Calculated chunk size: %d", chunkSize)

	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "sort_chunks")
	if err != nil {
		logError("Error creating temp directory: %v", err)
		return
	}
	// Defer ensures that the function is executed after the current function exits
	defer os.RemoveAll(tempDir) // Remove the temporary directory after completion
	logInfo("Temporary directory: %s", tempDir)

	chunkFiles, err := splitFile(inputFile, chunkSize, sortKeys, tempDir)
	if err != nil {
		logError("Error splitting file: %v", err)
		return
	}
	logInfo("Number of chunk files: %v", len(chunkFiles))

	err = mergeChunks(outputFile, chunkFiles, sortKeys, tempDir)
	if err != nil {
		logError("Error merging chunks: %v", err)
	}
	logInfo("Sorting completed in %v\n", time.Since(start))
}
