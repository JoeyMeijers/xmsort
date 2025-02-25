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

	"github.com/shirou/gopsutil/mem"
)

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
        go func(lines []string, chunkIndex int) {
            defer wg.Done()
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

// Schrijft een chunk naar een bestand
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

    readers := make([]*bufio.Reader, len(chunkFiles))
    files := make([]*os.File, len(chunkFiles))
    var wg sync.WaitGroup
    errChan := make(chan error, 1)

    totalLines := 0

    // Open the first line of each chunk file
    for i := 0; i < len(chunkFiles); i++ {
        f, err := os.Open(chunkFiles[i])
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
    return nil
}

func calculateChunkSize(averageLineSize int) int {
    v, _ := mem.VirtualMemory()

    // Vrij geheugen in bytes
    availableMemory := v.Available

    // Reserveer x% van het vrije geheugen voor je proces
    reservedMemory := availableMemory / 10

    // Bereken de chunk size in aantal regels
    chunkSize := int(reservedMemory / uint64(averageLineSize))

    // Zorg dat de chunk size niet te groot wordt
    if chunkSize > 500_000 {
        chunkSize = 500_000
    } else if chunkSize < 10_000 {
        chunkSize = 10_000 // Minimum chunksize om overhead te voorkomen
    }

    return chunkSize
}

// Schat de gemiddelde regelgrootte op basis van een sample uit het bestand
func estimateAverageLineSize(filename string) int {
	file, err := os.Open(filename)
    if err != nil {
        return 0 // Fallback
    }
    defer file.Close()

    reader := bufio.NewReader(file)
    var totalSize int
    var count int

    for count < 100 { // Sample n regels
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

func main() {
	// Settup logging
	setupLogging()
    config := parseFlags()
    inputFile := config.InputFile
    outputFile := config.OutputFile
    sortKeys := config.SortKeys
    // check if input file exists
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

    // Dynamisch berekenen van de chunk size
    averageLineSize := estimateAverageLineSize(inputFile)
    logInfo("Estimated average line size: %v", averageLineSize)
    chunkSize := calculateChunkSize(averageLineSize)
    logInfo("Calculated chunk size: %d", chunkSize)

    if _, err := os.Stat(inputFile); os.IsNotExist(err) {
        logError("Inputbestand bestaat niet!")
        return
    }

    // Maak een tijdelijke directory aan
    tempDir, err := os.MkdirTemp("", "sort_chunks")
    if err != nil {
        logError("Error creating temp directory: %v", err)
        return
    }
    // Defer zorgt ervoor dat de functie wordt uitgevoerd na het verlaten van de huidige functie
    defer os.RemoveAll(tempDir) // Verwijder de tijdelijke directory na afloop
    println("Tijdelijke directory:", tempDir)

    chunkFiles, err := splitFile(inputFile, chunkSize, sortKeys, tempDir)
    if err != nil {
        logError("Error splitting file: %v", err)
        return
    }
    logInfo("Aantal chunk bestanden: %v", len(chunkFiles))

    err = mergeChunks(outputFile, chunkFiles, sortKeys)
    if err != nil {
        logError("Error merging chunks: %v", err)
    }
    logInfo("Sorting completed in %v\n", time.Since(start))
}
