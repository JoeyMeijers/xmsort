package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

var logger *log.Logger

func setupLogging() {
	// Open het logbestand
	logFile, err := os.OpenFile("log.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Kan app.log niet openen: %v", err)
	}

	// Maak een centrale logger
	// Stuur logs zowel naar de console als naar het bestand
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger = log.New(multiWriter, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func logInfo(message string, args ...interface{}) {
	logger.Println("INFO: " + fmt.Sprintf(message, args...))
}

func logWarning(message string, args ...interface{}) {
	logger.Println("WARNING: " + fmt.Sprintf(message, args...))
}

func logError(message string, args ...interface{}) {
	logger.Println("ERROR: " + fmt.Sprintf(message, args...))
}

func logDebug(message string, args ...interface{}) {
	logger.Println("DEBUG: " + fmt.Sprintf(message, args...))
}
