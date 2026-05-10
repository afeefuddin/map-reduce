package cli

import (
	"context"
	"fmt"
	"log"
	"os"
)

func Main() {
	log.SetFlags(0)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "start":
		data, err := buildData(os.Args[2:])
		if err != nil {
			log.Fatalf("gomr start: %s", err)
		}

		if err := start(context.Background(), data); err != nil {
			log.Fatalf("gomr start: %s", err)
		}
	default:
		printUsage()
		os.Exit(2)
	}
}

func start(ctx context.Context, data *GomrData) error {
	if err := requireCommand("kubectl"); err != nil {
		return err
	}
	if err := requireCommand("docker"); err != nil {
		return err
	}

	if err := applyMinIO(ctx); err != nil {
		return err
	}
	if err := uploadInput(ctx, data); err != nil {
		return err
	}
	if err := buildImages(ctx, data); err != nil {
		return err
	}
	if err := loadImagesIntoLocalCluster(ctx, data); err != nil {
		return err
	}
	if err := deployMasterAndWorkers(ctx, data); err != nil {
		return err
	}

	log.Println("MapReduce cluster started")
	return nil
}

func printUsage() {
	fmt.Println("gomr is a CLI for running Go MapReduce workers on Kubernetes")
	fmt.Println("Usage: gomr start --input ./input.txt --workers 5 --mappers 5 --reducers 2 ./worker.go")
}
