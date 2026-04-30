package master

import (
	"context"
	"log"
	"slices"

	masterpb "map-reduce/gen/master"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	masterpb.UnimplementedMasterServer
}

func (s *Server) ReportCompletion(ctx context.Context, req *masterpb.WorkerId) (*masterpb.ReportCompletionResponse, error) {
	MasterStateData.mu.Lock()
	defer MasterStateData.mu.Unlock()

	idx := slices.IndexFunc(MasterStateData.WorkersSchedule, func(c WorkerSchedulerData) bool { return c.Id == req.Id })
	if idx < 0 {
		return nil, status.Errorf(codes.NotFound, "worker not found: %s", req.Id)
	}
	MasterStateData.WorkersSchedule[idx].MarkFree()

	if req.TaskType == "map" {
		taskIdx := slices.IndexFunc(MasterStateData.mapTasks, func(c MapTask) bool { return c.Id == req.TaskId })
		if taskIdx < 0 {
			return nil, status.Errorf(codes.NotFound, "map task not found: %s", req.TaskId)
		}
		MasterStateData.mapTasks[taskIdx].MarkCompleted()
	} else {

	}

	return &masterpb.ReportCompletionResponse{}, nil
}

func (s *Server) RegiserWorker(ctx context.Context, req *masterpb.WorkerData) (*masterpb.RegisterWorkerResponse, error) {
	MasterStateData.mu.Lock()
	defer MasterStateData.mu.Unlock()

	MasterStateData.workers[req.Id] = WorkerData{
		Address: req.Address,
	}

	MasterStateData.WorkersSchedule = append(MasterStateData.WorkersSchedule, WorkerSchedulerData{Id: req.Id, Addr: req.Address})

	log.Printf("Registered new worker: id=%s address=%s", req.Id, req.Address)
	log.Printf("Total workers: %d", len(MasterStateData.workers))

	return &masterpb.RegisterWorkerResponse{Registered: true}, nil
}
