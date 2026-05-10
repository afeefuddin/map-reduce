package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultMasterImage = "mr-master:latest"
	defaultWorkerImage = "mr-worker:latest"
)

type GomrData struct {
	inputPath           string
	inputObject         string
	workerFilePath      string
	workerBuildPath     string
	workerCount         int
	mapperWorkersCount  int
	reducerWorkersCount int
	masterImage         string
	workerImage         string
	rolloutID           string
}

func buildData(args []string) (*GomrData, error) {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	data := &GomrData{
		workerCount:         5,
		mapperWorkersCount:  5,
		reducerWorkersCount: 2,
		masterImage:         defaultMasterImage,
		workerImage:         defaultWorkerImage,
		rolloutID:           time.Now().UTC().Format(time.RFC3339Nano),
	}

	fs.StringVar(&data.inputPath, "input", "", "local input file to upload")
	fs.IntVar(&data.workerCount, "workers", data.workerCount, "worker pod count")
	fs.IntVar(&data.mapperWorkersCount, "mappers", data.mapperWorkersCount, "map task count")
	fs.IntVar(&data.reducerWorkersCount, "reducers", data.reducerWorkersCount, "reduce task count")
	fs.StringVar(&data.masterImage, "master-image", data.masterImage, "master image tag")
	fs.StringVar(&data.workerImage, "worker-image", data.workerImage, "worker image tag")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if data.inputPath == "" {
		return nil, errors.New("missing --input")
	}
	if fs.NArg() != 1 {
		return nil, errors.New("please specify exactly one worker .go file")
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get working directory: %w", err)
	}

	data.inputPath = absPath(pwd, data.inputPath)
	data.workerFilePath = absPath(pwd, fs.Arg(0))

	if !strings.HasSuffix(data.workerFilePath, ".go") {
		return nil, errors.New("worker file must end with .go")
	}
	if err := requireFile(data.inputPath); err != nil {
		return nil, fmt.Errorf("input: %w", err)
	}
	if err := requireFile(data.workerFilePath); err != nil {
		return nil, fmt.Errorf("worker: %w", err)
	}

	workerBuildPath, err := filepath.Rel(pwd, data.workerFilePath)
	if err != nil || strings.HasPrefix(workerBuildPath, ".."+string(filepath.Separator)) || workerBuildPath == ".." {
		return nil, errors.New("worker file must live inside the current Docker build context")
	}
	if data.workerCount <= 0 || data.mapperWorkersCount <= 0 || data.reducerWorkersCount <= 0 {
		return nil, errors.New("--workers, --mappers, and --reducers must all be greater than zero")
	}

	data.workerBuildPath = filepath.ToSlash(workerBuildPath)
	data.inputObject = "inputs/" + filepath.Base(data.inputPath)

	return data, nil
}
