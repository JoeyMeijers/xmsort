package testdata

import (
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

func GenerateTestFile(n int) {
	f, err := os.Create("test_data.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	numWorkers := runtime.NumCPU()
	lines := make(chan string, 1000)
	var wg sync.WaitGroup

	// Writer goroutine
	go func() {
		for line := range lines {
			f.WriteString(line)
		}
	}()

	// Worker goroutines
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + seed))
			for i := w; i < n; i += numWorkers {
				lines <- randomStringWithRand(N_CHARS, r)
			}
		}(int64(w))
	}

	wg.Wait()
	close(lines)
}

func randomStringWithRand(length int, r *rand.Rand) string {
	b := make([]byte, length)
	for i := range b {
		if i < N_NUMS {
			b[i] = byte(r.Intn(10) + '0')
			continue
		}
		if i == N_NUMS {
			b[i] = ' '
			continue
		}
		if (i-N_NUMS)%SPACE_IDX == 0 {
			b[i] = ' '
			continue
		}
		b[i] = CHARSET[r.Intn(len(CHARSET))]
	}
	return string(b) + "\n"
}