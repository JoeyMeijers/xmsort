# README

## Config Struct

```go
type Config struct {
    InputFile     string
    OutputFile    string
    SortKeys      SortKeySlice    // Sort keys based on fixed start position and length (fixed-width)
    FieldSortKeys FieldKeySlice   // Sort keys based on column index (delimiter-based)
    Delimiter     string          // Delimiter used to split fields for FieldSortKeys
    TestFile      int             // Number of lines for test file generation
}
```

## Usage

### Generate test file

```bash
go run . -testfile=10000
```

### Input and output files

```bash
--input  string  (required)  Path to input file  
--output string  (required)  Path to output file
```

### Sort keys

#### SortKey (fixed-width)

Sorts based on character positions in each line.

Format:

```
start,length,numeric,asc
```

- `start` (int): start position of the field in the line (0-based)  
- `length` (int): length of the field  
- `numeric` (bool): numeric sort (true/false)  
- `asc` (bool): ascending sort (true = ascending, false = descending)  

Example single key:

```bash
--sortkey 0,4,true,true
```

Multiple keys:

```bash
--sortkey 0,2,true,true --sortkey 2,2,true,false --sortkey 5,10,false,true --sortkey 15,10,false,false
```

Struct:

```go
type SortKey struct {
\tStart   int
\tLength  int
\tNumeric bool
\tAsc     bool
}
```

#### FieldSortKey (delimiter-based)

Sorts based on field index (column), split by the specified delimiter.

Format:

```
field,numeric,asc
```

- `field` (int): index of the field (0-based)  
- `numeric` (bool): numeric sort (true/false)  
- `asc` (bool): ascending sort (true/false)  

Example:

```bash
--keyfield 3,true,false
```

Struct:

```go
type FieldKey struct {
    Field   int
    Numeric bool
    Asc     bool
}
```

### Delimiter

Delimiter for splitting fields when using `--keyfield`.

Example:

```bash
--delimiter ","
```

---

## Build Instructions

### Download modules

```bash
go mod tidy        # Ensure all modules are fetched and dependencies fixed
go mod download    # Download modules locally
```

### Vendor dependencies

```bash
go mod vendor      # Place dependencies in vendor directory
tar -czf go_modules.tar.gz vendor go.mod go.sum  # Archive vendor and mod files
```

### Unpack

```bash
tar -xzf go_modules.tar.gz
```

### Build

```bash
go build -mod=vendor
```
