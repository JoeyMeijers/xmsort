package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Configuratie voor de sortering
type SortKey struct {
	Start   int
	Length  int
	Numeric bool
	Asc     bool
}

type Config struct {
	SortKeys  []SortKey
	ChunkSize int
}

// Sorteerfunctie voor een batch regels
func sortLines(lines []string, keys []SortKey) {
	sort.Slice(lines, func(i, j int) bool {
		for _, key := range keys {
			fieldA := extractField(lines[i], key)
			fieldB := extractField(lines[j], key)

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
	})
}

// Extracteert een veld uit een regel
func extractField(line string, key SortKey) string {
	line = strings.TrimSpace(line) // Voorkom extra newlines
	if key.Start >= len(line) {
		return ""
	}
	end := key.Start + key.Length
	if end > len(line) {
		end = len(line)
	}
	return line[key.Start:end]
}

// Splits het grote bestand in kleinere chunks
func splitFile(inputFile string, chunkSize int, sortKeys []SortKey) ([]string, error) {
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
				if len(line) > 0 {
					lines = append(lines, line)
					totalLines++
				}
				break
			}
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			lines = append(lines, line)
			totalLines++
		}
		if len(lines) >= chunkSize {
			wg.Add(1)
			go func(lines []string, chunkIndex int) {
				defer wg.Done()
				sortLines(lines, sortKeys)
				chunkFile, err := writeChunk(lines, chunkIndex)
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
		go func(lines []string, chunkIndex int) {
			defer wg.Done()
			sortLines(lines, sortKeys)
			chunkFile, err := writeChunk(lines, chunkIndex)
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

	fmt.Printf("Total lines read: %d\n", totalLines)
	return chunkFiles, nil
}

// Schrijft een chunk naar een bestand
func writeChunk(lines []string, index int) (string, error) {
	filename := fmt.Sprintf("chunk_%d.txt", index)
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

// Heap-element voor het mergen van chunks
type heapItem struct {
	line     string
	fileID   int
	sortKeys []SortKey
}

type minHeap []heapItem

func (h minHeap) Len() int { return len(h) }
func (h minHeap) Less(i, j int) bool {
	for _, key := range h[i].sortKeys {
		fieldA := extractField(h[i].line, key)
		fieldB := extractField(h[j].line, key)

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

// Merge sort implementatie met heap
func mergeChunks(outputFile string, chunkFiles []string, sortKeys []SortKey) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriter(out)

	minHeap := &minHeap{}
	heap.Init(minHeap)

	files := make([]*os.File, len(chunkFiles))
	readers := make([]*bufio.Reader, len(chunkFiles))
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	totalLines := 0

	for i, file := range chunkFiles {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		files[i] = f
		readers[i] = bufio.NewReader(f)
		line, err := readers[i].ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			heap.Push(minHeap, heapItem{line: line, fileID: i, sortKeys: sortKeys})
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for minHeap.Len() > 0 {
			item := heap.Pop(minHeap).(heapItem)
			writer.WriteString(item.line + "\n")
			totalLines++
			line, err := readers[item.fileID].ReadString('\n')
			if err != nil && err != io.EOF {
				errChan <- err
				return
			}
			line = strings.TrimSpace(line)
			if len(line) > 0 {
				heap.Push(minHeap, heapItem{line: line, fileID: item.fileID, sortKeys: sortKeys})
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

	fmt.Printf("Total lines written: %d\n", totalLines)
	return nil
}

func main() {
	start := time.Now()
	fmt.Println("Go external sort")
	fmt.Printf("Start: %v\n", start)
	inputFile := "test_data_m.txt"
	outputFile := "sorted_output.txt"
	chunkSize := 1_000 // Aantal regels per chunk
	sortKeys := []SortKey{
		{Start: 0, Length: 4, Numeric: true, Asc: false},
		{Start: 5, Length: 10, Numeric: false, Asc: true},
		{Start: 15, Length: 10, Numeric: false, Asc: false},
	}

	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		fmt.Println("Inputbestand bestaat niet!")
		return
	}

	chunkFiles, err := splitFile(inputFile, chunkSize, sortKeys)
	if err != nil {
		fmt.Println("Error splitting file:", err)
		return
	}
	fmt.Println("Aantal chunk bestanden:", len(chunkFiles))

	err = mergeChunks(outputFile, chunkFiles, sortKeys)
	if err != nil {
		fmt.Println("Error merging chunks:", err)
	}
	fmt.Printf("Sorting completed in %v\n", time.Since(start))
}
