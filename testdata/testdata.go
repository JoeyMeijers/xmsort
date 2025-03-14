package testdata

import (
	"math/rand"
	"os"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

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
	rand.Seed(time.Now().UnixNano()) // Seed voor willekeurige getallen
	return rand.Intn(10)
}

func randomString(length int) string {
	nums := 4
	rand.Seed(time.Now().UnixNano()) // Seed voor willekeurige getallen
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
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b) + "\n"
}
