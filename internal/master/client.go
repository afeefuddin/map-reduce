package master

import (
	workerpb "gomr/gen/worker"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func GetClient(target string) (workerpb.WorkerClient, error) {

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}

	return workerpb.NewWorkerClient(conn), nil
}
