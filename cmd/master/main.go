package main

import (
	"gomr/internal/master"
	"log"
	"os"
	"strconv"
)

func main() {
	config := master.MasterConfig{}
	config.Input = os.Getenv("INPUT_PATH")
	mappersCount, err := strconv.ParseInt(os.Getenv("MAPPERS_COUNT"), 10, 64)
	if err != nil {
		log.Fatalf("Error parsing the mapper count")
	}
	config.MappersCount = int(mappersCount)

	workersCount, err := strconv.ParseInt(os.Getenv("WORKERS_COUNT"), 10, 64)
	if err != nil {
		log.Fatalf("Error parsing the workers count")

	}
	config.WorkersCount = int(workersCount)

	reducersCounts, err := strconv.ParseInt(os.Getenv("REDUCERS_COUNT"), 10, 64)
	if err != nil {
		log.Fatalf("Error parsing the reducers count")

	}
	config.ReducersCount = int(reducersCounts)
	master.StartMaster(config)
}
