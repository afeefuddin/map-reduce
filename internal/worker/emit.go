package worker

import (
	"fmt"
	"hash/fnv"
)

type taskContext struct {
	phase string
}

func (c *taskContext) Emit(key, value string) error {
	if c.phase == "map" {
		Emit(key, value)
		return nil
	}

	EmitR(key, value)
	return nil
}

func Emit(key, value string) {
	idx := decideReducer(key)
	kv := []string{key, value}
	MapperData[idx] = append(MapperData[idx], kv)
}

func EmitR(key, value string) {
	ReducerData = append(ReducerData, fmt.Sprintf("%s: %s", key, value))
}

func decideReducer(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(MapReduceWorkerConfig.ReducerCount))
}
