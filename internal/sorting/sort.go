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

var ebcdicToAscii = [256]byte{
	0x00, 0x01, 0x02, 0x03, 0x9c, 0x09, 0x86, 0x7f,
	0x97, 0x8d, 0x8e, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x9d, 0x85, 0x08, 0x87,
	0x18, 0x19, 0x92, 0x8f, 0x1c, 0x1d, 0x1e, 0x1f,
	0x80, 0x81, 0x82, 0x83, 0x84, 0x0a, 0x17, 0x1b,
	0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x05, 0x06, 0x07,
	0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04,
	0x98, 0x99, 0x9a, 0x9b, 0x14, 0x15, 0x9e, 0x1a,
	0x20, 0xa0, 0xe2, 0xe4, 0xe0, 0xe1, 0xe3, 0xe5,
	0xe7, 0xf1, 0x5b, 0x2e, 0x3c, 0x28, 0x2b, 0x7c,
	0x26, 0xe9, 0xea, 0xeb, 0xe8, 0xed, 0xee, 0xef,
	0xec, 0xdf, 0x21, 0x24, 0x2a, 0x29, 0x3b, 0x5e,
	0x2d, 0x2f, 0xc2, 0xc4, 0xc0, 0xc1, 0xc3, 0xc5,
	0xc7, 0xd1, 0x7b, 0x2c, 0x25, 0x5f, 0x3e, 0x3f,
	0xf8, 0xc9, 0xca, 0xcb, 0xc8, 0xcd, 0xce, 0xcf,
	0xcc, 0x60, 0x3a, 0x23, 0x40, 0x27, 0x3d, 0x22,
	0xd8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67,
	0x68, 0x69, 0xab, 0xbb, 0xf0, 0xfd, 0xfe, 0xb1,
	0xb0, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70,
	0x71, 0x72, 0xaa, 0xba, 0xe6, 0xb8, 0xc6, 0xa4,
	0xb5, 0x7e, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
	0x79, 0x7a, 0xa1, 0xbf, 0xd0, 0x5b, 0xde, 0xae,
	0xac, 0xa3, 0xa5, 0xb7, 0xa9, 0xa7, 0xb6, 0xbc,
	0xbd, 0xbe, 0xdd, 0xa8, 0xaf, 0x5d, 0xb4, 0xd7,
	0x7c, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
	0x48, 0x49, 0xad, 0xf4, 0xf6, 0xf2, 0xf3, 0xf5,
	0x5c, 0xf7, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55,
	0x56, 0x57, 0x58, 0x59, 0x5a, 0xb2, 0xd4, 0xd6,
	0xd2, 0xd3, 0xd5, 0x30, 0x31, 0x32, 0x33, 0x34,
	0x35, 0x36, 0x37, 0x38, 0x39, 0xb3, 0xdb, 0xdc,
	0xd9, 0xda, 0x9f, 0x9f, 0x9f, 0x9f, 0x9f, 0x9f,
	0x9f, 0x9f, 0x9f, 0x9f, 0x9f, 0x9f, 0x9f, 0x9f,
}

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
