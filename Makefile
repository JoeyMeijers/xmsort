TEST_RECORDS=500_000_000

generate:
	go run ./cmd/genfile/ --records=$(TEST_RECORDS)

build:
	GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/xmsort-windows-amd64.exe ./cmd/xmsort
	GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o bin/xmsort-darwin-arm64 ./cmd/xmsort

run:
	go run ./cmd/xmsort "I=test_data.txt, O=out.txt, RL=122, RT=V, TS=Y, RD=Y, EN=ZERO, MEM=512M, S1=(e=0,l=2,g=ebcdic,v=a), S2=(e=2,l=2,g=ebcdic,v=d), S3=(e=5,l=10,g=ascii,v=a), S4=(e=15,l=65,g=ascii,v=d)"

run-m:
	go run ./cmd/xmsort "I=test_data_m.txt, O=out.txt, RL=122, RT=V, TS=Y, RD=Y, EN=ZERO, MEM=512M, S1=(e=0,l=2,g=ebcdic,v=a), S2=(e=2,l=2,g=ebcdic,v=d), S3=(e=5,l=10,g=ascii,v=a), S4=(e=15,l=65,g=ascii,v=d)"

test:
	go test ./...
