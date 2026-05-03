package gomr

import (
	"gomr/internal/worker"
	"gomr/pkg/gomr/api"
	"os"

	"github.com/google/uuid"
)

func Run(mrConfig *api.MapReduceConfig) {
	ip := os.Getenv("POD_IP")
	addr := ip + ":50051"
	reducerCount := mrConfig.ReducerCount
	if reducerCount == 0 {
		reducerCount = 2
	}

	worker.StartWorker(&worker.WorkerConfig{
		Mapper:       mrConfig.Mapper,
		Reducer:      mrConfig.Reducer,
		ReducerCount: reducerCount,
		WorkerAddr:   addr,
		WorkerId:     uuid.New().String(),
		MasterAddr:   "master:9001",
	})
}
