package merging

import (
	"bufio"
	"container/heap"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/joeymeijers/xmsort/internal/sorting"
	"github.com/joeymeijers/xmsort/internal/utils"
)

func MergeChunks(outputFile string, chunkFiles []string, sortKeys []sorting.SortKey, delimiter string) error {
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

	maxOpenFiles := utils.GetMaxOpenFiles()
	readers := make([]*bufio.Reader, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	openSem := make(chan struct{}, maxOpenFiles)
	var openWg sync.WaitGroup
	var wg sync.WaitGroup

	// Schat totaalregels
	var totalExpectedLines int
	for _, path := range chunkFiles {
		totalExpectedLines += utils.EstimateLineCount(path)
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
	return sorting.CompareLines(h[i].line, h[j].line, h[i].sortKeys, h[i].delimiter)
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
