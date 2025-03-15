package testdata

import (
	"math/rand"
	"os"
	"time"
)

const CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const SPACEIDX = 10

func GenerateTestFile(n int) {
	f, err := os.Create("test_data.txt")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	for i := 0; i < n; i++ {
		f.WriteString(randomString(30))
	}
}

func randomNumber() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // Create a new random generator
	return r.Intn(10)
}

func randomString(length int) string {
	nums := 4
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // Create a new random generator
	b := make([]byte, length)
	for i := range b {
		if i < nums {
			b[i] = byte(randomNumber() + '0')
			continue
		}
		if i == nums {
			b[i] = ' '
			continue
		}
		if i % SPACEIDX == 0 {
			b[i] = ' '
			continue
		}
		b[i] = CHARSET[r.Intn(len(CHARSET))]
	}
	return string(b) + "\n"
}
