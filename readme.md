# Downlaod modules
go mod tidy  # Zorgt ervoor dat alle modules correct worden opgehaald
go mod download  # Download alle modules naar de Go cache

# Export betanden
go mod vendor  # Plaats alle afhankelijkheden in de 'vendor' map
tar -czf go_modules.tar.gz vendor go.mod go.sum  # Pak alles in

# unpack
tar -xzf go_modules.tar.gz  # Uitpakken

# build 
go build -mod=vendor
