package main

import (
	"map-reduce/internal/worker"
	mapreduce "map-reduce/pkg/map-reduce"
	"os"
	"strings"

	"github.com/google/uuid"
)

func mapper(chunk string) {
	lines := strings.SplitSeq(chunk, "\n")

	for l := range lines {
		words := strings.SplitSeq(l, " ")
		for word := range words {
			worker.Emit(word, "1")
		}
	}
}

func reducer() {

}

func main() {
	ip := os.Getenv("POD_IP")
	addr := ip + ":50051"
	worker.StartWorker(&mapreduce.MapReduceConfig{
		Mapper:       mapper,
		Reducer:      reducer,
		MasterAddr:   "master:9001",
		WorkerId:     uuid.New().String(),
		WorkerAdd:    addr,
		ReducerCount: 2,
	})
}
