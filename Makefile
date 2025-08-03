TEST_RECORDS=25_000_000

generate:
	go run ./cmd/genfile/ --records=$(TEST_RECORDS)

run: 
	go run ./cmd/xmsort --input=test_data.txt --output=sorted_test_data.txt --sortkey 0,2,true,true --sortkey 2,2,true,false --sortkey 5,10,false,true --sortkey 15,10,false,false

