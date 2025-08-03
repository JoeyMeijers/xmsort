package utils

import (
	"fmt"
	"io"
	"log"
	"os"
)

var logger *log.Logger

func SetupLogging() {
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

func LogInfo(message string, args ...any) {
	logger.Println("INFO: " + fmt.Sprintf(message, args...))
}

func LogWarning(message string, args ...any) {
	logger.Println("WARNING: " + fmt.Sprintf(message, args...))
}

func LogError(message string, args ...any) {
	logger.Println("ERROR: " + fmt.Sprintf(message, args...))
}

func LogDebug(message string, args ...any) {
	logger.Println("DEBUG: " + fmt.Sprintf(message, args...))
}
