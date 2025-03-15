package testdata

import (
	"math/rand"
	"os"
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

	// Kanaal om gegenereerde regels te verzamelen
	lines := make(chan string, 100)
	var wg sync.WaitGroup

	// Start een goroutine voor schrijven naar bestand
	go func() {
		for line := range lines {
			f.WriteString(line)
		}
	}()

	// Parallel genereren van regels
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lines <- randomString(N_CHARS)
		}()
	}

	// Wacht tot alle regels gegenereerd zijn en sluit het kanaal
	wg.Wait()
	close(lines)
}

func randomNumber() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(10)
}

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		if i < N_NUMS {
			b[i] = byte(randomNumber() + '0')
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