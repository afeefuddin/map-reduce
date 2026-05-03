package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type GomrData struct {
	inputPath           string
	workerFilePath      string
	workerCount         int
	mapperWorkersCount  int
	reducerWorkersCount int
}

func getSafe(args []string, idx int) (string, bool) {
	if len(args) > idx {
		return args[idx], true
	}

	return "", false
}

func buildData(data *GomrData) {
	// Set default values
	data.workerCount = 5
	data.mapperWorkersCount = 5
	data.reducerWorkersCount = 2

	for i := 2; i < len(os.Args); i++ {
		currentCmd := os.Args[i]

		val, found := getSafe(os.Args, i+1)
		if found {
			switch currentCmd {
			case "--input":
				data.inputPath = val
			case "--workers":
				workersCount, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					fmt.Println("Error parsing the value of workers")
				} else {
					data.workerCount = int(workersCount)
				}
			case "--mappers":
				mappersCount, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					fmt.Println("Error parsing the value of mappers")
				} else {
					data.mapperWorkersCount = int(mappersCount)
				}
			case "--reducers":
				reducersCount, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					fmt.Println("Error parsing the value of reducers")
				} else {
					data.reducerWorkersCount = int(reducersCount)
				}
			}
			i++
		}
	}

	if !strings.HasSuffix(os.Args[len(os.Args)-1], ".go") || strings.HasPrefix(os.Args[len(os.Args)-2], "--") {
		fmt.Println("Please specify the worker file")
		os.Exit(2)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("gomr is a cli to run tasks in a distributed manner using k8s following the map reduce pattern")
		fmt.Println("Run gomr start --input ./input/input.txt --workers 5 --mappers 5 --reducers 2 worker.go")
		os.Exit(2)
	}

	cmd := os.Args[1]

	data := &GomrData{}

	if cmd == "start" {
		buildData(data)
	} else {
		fmt.Println("Run gomr start --input ./input/input.txt --workers 5 --mappers 5 --reducers 2 worker.go")
	}
}
