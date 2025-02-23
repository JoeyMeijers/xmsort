package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"
	"strconv"
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
	scanner := bufio.NewScanner(file)
	var lines []string
	chunkIndex := 0

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) >= chunkSize {
			sortLines(lines, sortKeys)
			chunkFile, err := writeChunk(lines, chunkIndex)
			if err != nil {
				return nil, err
			}
			chunkFiles = append(chunkFiles, chunkFile)
			lines = nil
			chunkIndex++
		}
	}

	if len(lines) > 0 {
		sortLines(lines, sortKeys)
		chunkFile, err := writeChunk(lines, chunkIndex)
		if err != nil {
			return nil, err
		}
		chunkFiles = append(chunkFiles, chunkFile)
	}

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
	line   string
	fileID int
}

type minHeap []heapItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].line < h[j].line }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
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
func mergeChunks(outputFile string, chunkFiles []string) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriter(out)

	minHeap := &minHeap{}
	heap.Init(minHeap)

	files := make([]*os.File, len(chunkFiles))
	scanners := make([]*bufio.Scanner, len(chunkFiles))

	for i, file := range chunkFiles {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		files[i] = f
		scanners[i] = bufio.NewScanner(f)
		if scanners[i].Scan() {
			heap.Push(minHeap, heapItem{line: scanners[i].Text(), fileID: i})
		}
	}

	for minHeap.Len() > 0 {
		item := heap.Pop(minHeap).(heapItem)
		writer.WriteString(item.line + "\n")
		if scanners[item.fileID].Scan() {
			heap.Push(minHeap, heapItem{line: scanners[item.fileID].Text(), fileID: item.fileID})
		}
	}

	writer.Flush()
	for _, f := range files {
		f.Close()
	}
	return nil
}

func main() {
	fmt.Println("Go external sort")
	inputFile := "test_data_s.txt"
	outputFile := "sorted_output.txt"
	chunkSize := 100_000 // Aantal regels per chunk
	sortKeys := []SortKey{
		{Start: 0, Length: 4, Numeric: true, Asc: false},
		{Start: 5, Length: 10, Numeric: false, Asc: true},
	}

	chunkFiles, err := splitFile(inputFile, chunkSize, sortKeys)
	if err != nil {
		fmt.Println("Error splitting file:", err)
		return
	}

	err = mergeChunks(outputFile, chunkFiles)
	if err != nil {
		fmt.Println("Error merging chunks:", err)
	}
}
