package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	masterpb "map-reduce/gen/master"
	workerpb "map-reduce/gen/worker"
	"map-reduce/internal/utils"
	mapreduce "map-reduce/pkg/map-reduce"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var MapReduceWorkerConfig *mapreduce.MapReduceConfig
var MapperData [][][]string

func StartWorker(config *mapreduce.MapReduceConfig) {
	MapReduceWorkerConfig = config

	lis, err := net.Listen("tcp", config.WorkerAdd)

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

		obj, err := utils.GetObject(location)

		if err != nil {
			log.Fatalf("Error getting the input object %s", err)
		}

		data, err := io.ReadAll(obj)

		if err != nil {
			log.Fatalf("Error getting the data object %s", err)
		}

		MapperData = make([][][]string, MapReduceWorkerConfig.ReducerCount)
		MapReduceWorkerConfig.Mapper(string(data))

		// Sync the in memory mapper data to minio
		SyncReducerDataToMinio()

	} else {
		MapReduceWorkerConfig.Reducer()
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

func RegisterWithMaster(config *mapreduce.MapReduceConfig) {
	conn, err := grpc.NewClient(config.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Failed to connect to the master: %s", err)
	}

	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := masterpb.NewMasterClient(conn)

	res, err := c.RegiserWorker(ctx, &masterpb.WorkerData{Id: config.WorkerId, Address: config.WorkerAdd})

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
		utils.UploadData(fmt.Sprintf("workers/worker-%s/reducer-%d.txt", MapReduceWorkerConfig.WorkerId, i), d)
	}
}
