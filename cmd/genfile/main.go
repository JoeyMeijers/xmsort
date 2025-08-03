package main

import (
	"github.com/joeymeijers/xmsort/internal/testdata"
	"github.com/joeymeijers/xmsort/internal/utils"
)

func main() {
	utils.SetupLogging()
	config := testdata.ParseFlags()

	utils.LogInfo("Generating test file with %d lines", config.Records)
	testdata.GenerateTestFile(config.Records, config.OutputFile)
}
