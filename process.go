package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/marcelluseasley/golang_data_challenge/process/models"
)

func main() {

	// [REDACTED] Challenge
	// Author: Marcellus Easley

	// Open and ping a database connection to the docker db image.
	// Use this db connection if you like, it requires that the docker image
	// is running (via docker compose up).

	// Add process code here.
	var dds models.DeviceDataStore
	dds.InitializeDB()

	args := os.Args
	if len(args) != 2 {
		log.Fatal("error: invalid number of command line arguments")
	}

	incomingData := models.DeviceData{}
	err := json.Unmarshal([]byte(args[1]), &incomingData)
	if err != nil {
		log.Fatalf("error: unable to unmarshal command-line argument into data struct: %v", err)
	}

	existingData, newEntry := dds.GetData(incomingData)

	if newEntry {
		// set to nil to prevent output to command line
		existingData.Generated = nil
		jDat, err := json.Marshal(existingData)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(jDat))
		return
	}

	translatedData := models.Translate(&existingData, &incomingData)
	dds.UpdateState(translatedData)

	translatedData.Generated = nil
	jDat, err := json.Marshal(translatedData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jDat))
}
