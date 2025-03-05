# Read me

## Use

-input -output -sortkey
 go run . -input=test_data_m.txt -output=sorted_m.txt -sortkey=0,4,true,false -sortkey=5,10,false,true -sortkey=15,10,false,false

```go
SortKey {
    Start   int
    Length  int
    Numeric bool
    Asc     bool
}
```

## Build

### Download modules

go mod tidy  # Zorgt ervoor dat alle modules correct worden opgehaald
go mod download  # Download alle modules naar de Go cache

### Export betanden

go mod vendor  # Plaats alle afhankelijkheden in de 'vendor' map
tar -czf go_modules.tar.gz vendor go.mod go.sum  # Pak alles in

### unpack

tar -xzf go_modules.tar.gz  # Uitpakken

### build

go build -mod=vendor
