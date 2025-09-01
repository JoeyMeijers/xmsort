TEST_RECORDS=25_000_000

generate:
	go run ./cmd/genfile/ --records=$(TEST_RECORDS)

run:
	go run ./cmd/xmsort \
		--input=test_data.txt \
		--output=sorted_test_data.txt \
		--sortkey 0,2,true,true \
		--sortkey 2,2,true,false \
		--sortkey 5,10,false,true \
		--sortkey 15,10,false,false \
		--recordlength=122 \
		--recordtype=V \
		--truncatespaces=true \
		--removeduplicates=true \
		--emptynumbers=ZERO \
		--memory=512M \
		--delimiter=""

run-xs-params:
	go run ./cmd/xmsort \
		"I=test_data.txt" \
		"O=out.txt" \
		"RL=122" \
		"RT=V" \
		"TS=Y" \
		"RD=Y" \
		"EN=ZERO" \
		"MEM=512M" \
		"s1=(e=0,l=2,g=ebcdic,v=a)" \
		"s2=(e=2,l=2,g=ebcdic,v=d)" \
		"s3=(e=5,l=10,g=ascii,v=a)" \
		"s4=(e=15,l=65,g=ascii,v=d)"

test:
	go test ./...
