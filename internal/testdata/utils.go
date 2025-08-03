package testdata

import (
	"github.com/joeymeijers/xmsort/internal/utils"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

const CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const SPACE_IDX = 10
const N_NUMS = 4
const N_CHARS = 25

func GenerateTestFile(n int, outputPath string) {
	f, err := os.Create(outputPath)
	if err != nil {
		panic(err)
	}
	defer utils.SafeClose(f)

	numWorkers := runtime.NumCPU()
	lines := make(chan string, 1000)

	var wg sync.WaitGroup
	var writerWg sync.WaitGroup
	writerWg.Add(1)

	// Writer goroutine
	go func() {
		defer writerWg.Done()
		for line := range lines {
			if _, err := f.WriteString(line); err != nil {
				panic(err)
			}
		}
	}()

	// Worker goroutines
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(w int) {
			defer wg.Done()
			seed := time.Now().UnixNano() + int64(w*1_000_000)
			r := rand.New(rand.NewSource(seed))
			for i := w; i < n; i += numWorkers {
				lines <- generateLine(r)
			}
		}(w)
	}

	wg.Wait()
	close(lines)
	writerWg.Wait()
}

func generateLine(r *rand.Rand) string {
	b := make([]byte, N_CHARS)
	for i := range b {
		switch {
		case i < N_NUMS:
			b[i] = byte(r.Intn(10) + '0')
		case i == N_NUMS || (i-N_NUMS)%SPACE_IDX == 0:
			b[i] = ' '
		default:
			b[i] = CHARSET[r.Intn(len(CHARSET))]
		}
	}
	return string(b) + "\n"
}
