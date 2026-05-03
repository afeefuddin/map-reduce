package worker

import (
	"context"
	"fmt"
	masterpb "gomr/gen/master"
	workerpb "gomr/gen/worker"
	"gomr/internal/storage"
	"io"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var MapReduceWorkerConfig *WorkerConfig
var MapperData [][][]string
var ReducerData []string

func StartWorker(config *WorkerConfig) {
	MapReduceWorkerConfig = config

	lis, err := net.Listen("tcp", config.WorkerAddr)

	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerServer(grpcServer, &Server{})

	// Register the current worker with the server
	go RegisterWithMaster(config)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %s", err)
	}
}

func PerformTask(req *workerpb.TaskRequest) {
	if req.Type == "map" {
		location := req.Location

		obj, err := storage.GetObject(location)

		if err != nil {
			log.Fatalf("Error getting the input object %s", err)
		}

		data, err := io.ReadAll(obj)

		if err != nil {
			log.Fatalf("Error getting the data object %s", err)
		}

		MapperData = make([][][]string, MapReduceWorkerConfig.ReducerCount)
		MapReduceWorkerConfig.Mapper(&taskContext{phase: "map"}, string(data))

		// Sync the in memory mapper data to minio
		SyncReducerDataToMinio()

	} else {
		id, err := strconv.ParseInt(req.Id, 10, 64)

		if err != nil {
			log.Fatalf("Error parinsg id %s %s", err, req.Id)
		}
		ReducerData = nil

		client, err := storage.GetMinioClient()
		ctx := context.Background()

		if err != nil {
			log.Fatalf("Error connecting to minio")
		}

		// var paths []minio.ObjectInfo
		var reducerData []string

		for d := range client.ListObjectsIter(ctx, "map-reduce-bucket", minio.ListObjectsOptions{Prefix: "workers", Recursive: true}) {
			if strings.HasSuffix(d.Key, fmt.Sprintf("reducer-%d.txt", id)) {
				data, err := storage.GetObject(d.Key)
				if err != nil {
					log.Fatalf("Error reading the reducer dat")
				}
				redData, err := io.ReadAll(data)

				if err != nil {
					log.Fatalf("Error reading the reducer dat")
				}

				reducerData = append(reducerData, string(redData))
			}
		}

		var wordsWithCount []string
		// wordsWithCount := slices.
		for _, d := range reducerData {
			words := strings.Split(d, "\n")
			wordsWithCount = slices.Concat(wordsWithCount, words)
		}

		m := make(map[string][]int)

		for _, line := range wordsWithCount {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}

			count, err := strconv.Atoi(fields[1])
			if err != nil {
				log.Printf("Skipping invalid reducer data %q: %s", line, err)
				continue
			}

			m[fields[0]] = append(m[fields[0]], count)
		}

		for key, value := range m {
			MapReduceWorkerConfig.Reducer(&taskContext{phase: "reduce"}, key, value)
		}

		SyncOutputToMinio(req.Id)
	}

	conn, err := grpc.NewClient(MapReduceWorkerConfig.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("Failed to connect to the master: %s", err)
		return
	}

	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := masterpb.NewMasterClient(conn)

	_, err = c.ReportCompletion(ctx, &masterpb.WorkerId{Id: MapReduceWorkerConfig.WorkerId, TaskId: req.Id, TaskType: req.Type})

	if err != nil {
		log.Printf("Error reporting task completion: %s", err)
	}
}

func RegisterWithMaster(config *WorkerConfig) {
	conn, err := grpc.NewClient(config.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Failed to connect to the master: %s", err)
	}

	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := masterpb.NewMasterClient(conn)

	res, err := c.RegiserWorker(ctx, &masterpb.WorkerData{Id: config.WorkerId, Address: config.WorkerAddr})

	if err != nil {
		log.Fatalf("Error registring the worker: %s", err)
	} else {
		log.Printf("Registerd with the master %v", res.Registered)
	}
}

func SyncReducerDataToMinio() {
	data := make([]string, len(MapperData))
	for i, elem := range MapperData {
		var reducerDataBuilder strings.Builder
		for _, e := range elem {
			reducerDataBuilder.WriteString(strings.Join(e, " "))
			reducerDataBuilder.WriteString("\n")
		}
		data[i] = reducerDataBuilder.String()
	}

	for i, d := range data {
		storage.UploadData(fmt.Sprintf("workers/worker-%s/reducer-%d.txt", MapReduceWorkerConfig.WorkerId, i), d)
	}
}

func SyncOutputToMinio(id string) {
	combinedData := strings.Join(ReducerData, "\n")
	storage.UploadData(fmt.Sprintf("outputs/output-%s.txt", id), combinedData)
}
