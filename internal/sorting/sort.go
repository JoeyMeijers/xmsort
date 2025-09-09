package sorting

import (
	"bufio"
	"fmt"
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


func EBCDICToASCII(s string) string {
	b := []byte(s)
	for i := range b {
		b[i] = ebcdicToAscii[b[i]]
	}
	return string(b)
}

func CompareLines(a, b string, keys []SortKey, delimiter string, truncateSpaces bool, emptyNumbers string) bool {
    for _, key := range keys {
        fieldA := ExtractField(a, key, delimiter, truncateSpaces)
        fieldB := ExtractField(b, key, delimiter, truncateSpaces)

        if key.Numeric {
            if fieldA == "" || fieldB == "" {
                if strings.ToUpper(emptyNumbers) == "ERROR" {
                    panic(fmt.Sprintf("Empty numeric field encountered: '%s' vs '%s'", fieldA, fieldB))
                } else {
                    if fieldA == "" {
                        fieldA = "0"
                    }
                    if fieldB == "" {
                        fieldB = "0"
                    }
                }
            }
            numA, _ := strconv.ParseFloat(fieldA, 64)
            numB, _ := strconv.ParseFloat(fieldB, 64)
            if numA == numB {
                continue
            }
            if key.Asc {
                return numA < numB
            }
            return numA > numB
        } else {
            if fieldA == fieldB {
                continue
            }
            if key.Asc {
                return fieldA < fieldB
            }
            return fieldA > fieldB
        }
    }
    return false
}

// sortLines sorts a batch of lines based on the provided sort keys.
func SortLines(lines []string, keys []SortKey, delimiter string, truncateSpaces bool, emptyNumbers string) {
	sort.Slice(lines, func(i, j int) bool {
		return CompareLines(lines[i], lines[j], keys, delimiter, truncateSpaces, emptyNumbers)
	})
}

// extractField extracts a field from a line based on the provided sort key and delimiter.
// If delimiter is not empty, split the line and use the column as field.
// Otherwise, fall back to fixed position (Start, Length).
func ExtractField(line string, key SortKey, delimiter string, truncateSpaces bool) string {
	line = strings.TrimRight(line, "\r\n")
	var val string
	if delimiter != "" {
		cols := strings.Split(line, delimiter)
		if key.Start >= len(cols) {
			return ""
		}
		val = cols[key.Start]
		if key.Length > 0 && key.Length < len(val) {
			val = val[:key.Length]
		}
	} else {
		if key.Start >= len(line) {
			return ""
		}
		if key.Length <= 0 {
			val = line[key.Start:]
		} else {
			end := min(key.Start+key.Length, len(line))
			val = line[key.Start:end]
		}
	}
	if truncateSpaces {
		val = strings.TrimSpace(val)
	}
	return val
}

func ProcessChunk(lines []string, chunkIndex int, sortKeys []SortKey, tempDir, delimiter string, truncateSpaces bool, removeDuplicates bool, emptyNumbers string) (string, error) {
	SortLines(lines, sortKeys, delimiter, truncateSpaces, emptyNumbers)
	if removeDuplicates {
		lines = utils.RemoveDuplicates(lines)
	}
	return utils.WriteChunk(lines, chunkIndex, tempDir)
}

func SplitFileAndSort(
    inputFile string,
    chunkSize int,
    sortKeys []SortKey,
    tempDir string,
    delimiter string,
    truncateSpaces bool,
    removeDuplicates bool,
    emptyNumbers string,
    recordLength int,
    recordType string,
) ([]string, error) {
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

    chunkIndex := 0
    chunkChan := make(chan string, 10)
    maxWorkers := runtime.NumCPU()
    sem := make(chan struct{}, maxWorkers)

    go func() {
        for chunkFile := range chunkChan {
            chunkFiles = append(chunkFiles, chunkFile)
        }
    }()

    totalLinesEstimate := utils.EstimateLineCount(inputFile)
    bar := pb.StartNew(totalLinesEstimate)
    bar.SetWriter(os.Stdout)

    totalLines := 0

    flushChunk := func(lines []string, chunkIndex int) {
        wg.Add(1)
        sem <- struct{}{}
        go func(lines []string, chunkIndex int) {
            defer wg.Done()
            defer func() { <-sem }()
            chunkFile, err := ProcessChunk(lines, chunkIndex, sortKeys, tempDir, delimiter, truncateSpaces, removeDuplicates, emptyNumbers)
            if err != nil {
                errOnce.Do(func() { exitErr = err })
                return
            }
            chunkChan <- chunkFile
        }(lines, chunkIndex)
    }

    if strings.ToUpper(recordType) == "F" && recordLength > 0 {
        // Fixed-width records
        buf := make([]byte, recordLength)
        for {
            n, err := file.Read(buf)
            if n > 0 {
                line := string(buf[:n])
                lines = append(lines, strings.TrimRight(line, "\r\n"))
                totalLines++
                bar.Increment()
                if len(lines) >= chunkSize {
                    flushChunk(lines, chunkIndex)
                    lines = nil
                    chunkIndex++
                }
            }
            if err == io.EOF {
                break
            }
            if err != nil {
                return nil, err
            }
        }
    } else {
        // Variable-length records (default)
        reader := bufio.NewReader(file)
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
                flushChunk(lines, chunkIndex)
                lines = nil
                chunkIndex++
            }
        }
    }

    if len(lines) > 0 {
        flushChunk(lines, chunkIndex)
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
