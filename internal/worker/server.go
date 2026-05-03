package worker

import (
	"context"
	"log"

	workerpb "gomr/gen/worker"
)

type Server struct {
	workerpb.UnimplementedWorkerServer
}

func (s *Server) Health(context.Context, *workerpb.HealthRequest) (*workerpb.HealthStatus, error) {
	return &workerpb.HealthStatus{
		Status: "ok",
	}, nil
}

func (s *Server) AssignTask(ctx context.Context, req *workerpb.TaskRequest) (*workerpb.TaskResponse, error) {
	go PerformTask(req)
	log.Printf("Got assigned a taks to perform %s, %s, %s", req.Id, req.Type, req.Location)
	// go run
	return &workerpb.TaskResponse{}, nil
}
