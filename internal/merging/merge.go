package merging

import (
	"bufio"
	"container/heap"
	"io"
	"os"
	"strings"

	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/joeymeijers/xmsort/internal/utils"
)

// heapItem represents an element in the heap used for merging chunks.
type heapItem struct {
	line      string
	fileID    int
	sortKeys  []sorting.SortKey
	delimiter string
}

// minHeap is a min-heap of heapItems.
type minHeap []heapItem

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return sorting.CompareLines(h[i].line, h[j].line, h[i].sortKeys, h[i].delimiter, false, "")
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

// multiWayMerge merges up to maxOpenFiles chunk files into a single output file.
func multiWayMerge(outputFile string, chunkFiles []string, sortKeys []sorting.SortKey, delimiter string) error {
	readers := make([]*bufio.Reader, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	for i, fpath := range chunkFiles {
		f, err := os.Open(fpath)
		if err != nil {
			for j := 0; j < i; j++ {
				utils.SafeClose(files[j])
			}
			return err
		}
		files[i] = f
		readers[i] = bufio.NewReader(f)
	}

	out, err := os.Create(outputFile)
	if err != nil {
		for _, f := range files {
			utils.SafeClose(f)
		}
		return err
	}
	writer := bufio.NewWriterSize(out, 16*1024*1024)

	h := &minHeap{}
	heap.Init(h)

	for i, r := range readers {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			for _, f := range files {
				utils.SafeClose(f)
			}
			utils.SafeClose(out)
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if err != io.EOF || len(line) > 0 {
			heap.Push(h, heapItem{
				line:      line,
				fileID:    i,
				sortKeys:  sortKeys,
				delimiter: delimiter,
			})
		}
	}

	newline := utils.GetNewline()

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		_, err := writer.WriteString(item.line + newline)
		if err != nil {
			for _, f := range files {
				utils.SafeClose(f)
			}
			utils.SafeClose(out)
			return err
		}

		line, err := readers[item.fileID].ReadString('\n')
		if err != nil && err != io.EOF {
			for _, f := range files {
				utils.SafeClose(f)
			}
			utils.SafeClose(out)
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if err != io.EOF || len(line) > 0 {
			heap.Push(h, heapItem{
				line:      line,
				fileID:    item.fileID,
				sortKeys:  sortKeys,
				delimiter: delimiter,
			})
		}
	}

	for _, f := range files {
		utils.SafeClose(f)
	}
	err = writer.Flush()
	if err != nil {
		utils.SafeClose(out)
		return err
	}
	utils.SafeClose(out)
	return nil
}

// MultiLevelMerge merges chunk files in batches of maxOpenFiles until only one output file remains.
// If tempDir is empty, os.TempDir() is used for intermediate files.
func MultiLevelMerge(outputFile string, chunkFiles []string, sortKeys []sorting.SortKey, delimiter string, maxOpenFiles int, tempDir string) error {
	if len(chunkFiles) == 0 {
		// No chunks to merge, create empty output file
		out, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		return utils.SafeClose(out)
	}

	// If only one chunk, just rename it to outputFile
	if len(chunkFiles) == 1 {
		return os.Rename(chunkFiles[0], outputFile)
	}

	currentFiles := make([]string, len(chunkFiles))
	copy(currentFiles, chunkFiles)

	for len(currentFiles) > 1 {
		var nextLevelFiles []string
		for i := 0; i < len(currentFiles); i += maxOpenFiles {
			end := i + maxOpenFiles
			if end > len(currentFiles) {
				end = len(currentFiles)
			}
			batch := currentFiles[i:end]

			tempFile, err := os.CreateTemp(tempDir, "xmsort_merge_*.txt")
			if err != nil {
				for _, f := range nextLevelFiles {
					utils.SafeRemove(f)
				}
				return err
			}
			tempFilePath := tempFile.Name()
			utils.SafeClose(tempFile)

			err = multiWayMerge(tempFilePath, batch, sortKeys, delimiter)
			if err != nil {
				utils.SafeRemove(tempFilePath)
				for _, f := range nextLevelFiles {
					utils.SafeRemove(f)
				}
				return err
			}

			// Remove merged input chunks
			for _, f := range batch {
				utils.SafeRemove(f)
			}

			nextLevelFiles = append(nextLevelFiles, tempFilePath)
		}
		currentFiles = nextLevelFiles
	}

	// Rename the final merged file to outputFile
	return os.Rename(currentFiles[0], outputFile)
}
