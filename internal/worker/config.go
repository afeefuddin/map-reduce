package worker

import "gomr/pkg/gomr/api"

type WorkerConfig struct {
	Mapper       api.Mapper
	Reducer      api.Reducer
	ReducerCount int
	WorkerAddr   string
	WorkerId     string
	MasterAddr   string
}
