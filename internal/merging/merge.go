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

// heapItem represents an element in the heap used for merging chunks.
type heapItem struct {
	line      string
	fileID    int
	sortKeys  []sorting.SortKey
	delimiter string
	truncateSpaces bool
	emptyNumbers   string
}

// minHeap is a min-heap of heapItems.
type minHeap []heapItem

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return sorting.CompareLines(h[i].line, h[j].line, h[i].sortKeys, h[i].delimiter, h[i].truncateSpaces, h[i].emptyNumbers)
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

func openChunkFiles(
	chunkFiles []string,
	sortKeys []sorting.SortKey,
	delimiter string,
) ([]*bufio.Reader, []*os.File, []heapItem, error) {
	readers := make([]*bufio.Reader, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	openSem := make(chan struct{}, utils.GetMaxOpenFiles())
	var openWg sync.WaitGroup
	var errOnce sync.Once
	var exitErr error

	itemChan := make(chan heapItem, len(chunkFiles))

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
				itemChan <- heapItem{
					line:      line,
					fileID:    i,
					sortKeys:  sortKeys,
					delimiter: delimiter,
				}
			}
		}(i)
	}

	go func() {
		openWg.Wait()
		close(itemChan)
	}()

	var initialHeapItems []heapItem
	for item := range itemChan {
		initialHeapItems = append(initialHeapItems, item)
	}

	if exitErr != nil {
		return nil, nil, nil, exitErr
	}

	return readers, files, initialHeapItems, nil
}

func mergeHeapToOutput(
	writer *bufio.Writer,
	readers []*bufio.Reader,
	files []*os.File,
	initialItems []heapItem,
	bar *pb.ProgressBar,
) error {
	var errOnce sync.Once
	var exitErr error

	h := &minHeap{}
	heap.Init(h)
	for _, item := range initialItems {
		heap.Push(h, item)
	}

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		_, err := writer.WriteString(item.line + "\n")
		if err != nil {
			errOnce.Do(func() { exitErr = err })
			break
		}
		bar.Increment()

		line, err := readers[item.fileID].ReadString('\n')
		if err != nil && err != io.EOF {
			errOnce.Do(func() { exitErr = err })
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if err != io.EOF || len(line) > 0 {
			heap.Push(h, heapItem{
				line:      line,
				fileID:    item.fileID,
				sortKeys:  item.sortKeys,
				delimiter: item.delimiter,
			})
		} else if err == io.EOF {
			utils.SafeClose(files[item.fileID])
		}
	}

	utils.SafeFlush(writer)
	bar.Finish()

	return exitErr
}

func MergeChunks(outputFile string, chunkFiles []string, sortKeys []sorting.SortKey, delimiter string) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer utils.SafeClose(out)

	totalLines := 0
	for _, f := range chunkFiles {
		totalLines += utils.EstimateLineCount(f)
	}
	bar := pb.StartNew(totalLines)
	bar.SetWriter(os.Stdout)

	readers, files, initialItems, err := openChunkFiles(chunkFiles, sortKeys, delimiter)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(out, 16*1024*1024)
	err = mergeHeapToOutput(writer, readers, files, initialItems, bar)
	if err != nil {
		return err
	}

	for _, f := range chunkFiles {
		utils.SafeRemove(f)
	}

	return nil
}
