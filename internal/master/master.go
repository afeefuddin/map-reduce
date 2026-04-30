package master

import (
	"context"
	"log"
	masterpb "map-reduce/gen/master"
	"net"
	"time"

	"google.golang.org/grpc"
)

func StartMaster(config MasterConfig) {
	MasterStateData = &MasterState{workers: make(map[string]WorkerData)}
	ctx, _ := context.WithCancel(context.Background())

	lis, err := net.Listen("tcp", ":9001")

	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}

	grpcServer := grpc.NewServer()

	masterpb.RegisterMasterServer(grpcServer, &Server{})

	go func(ctx context.Context) {
		// Wait for all workers to register before the map/reduce scheduler starts.
		for {
			MasterStateData.mu.RLock()
			workerCount := len(MasterStateData.workers)
			MasterStateData.mu.RUnlock()
			log.Printf("Worker count %d", workerCount)

			if config.WorkersCount == workerCount {
				log.Print("All workers connected")
				break
			}

			time.Sleep(time.Second)
		}

		OrchestrateMapReduce(ctx, config)
	}(ctx)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to start the server %s", err)
	}

}
