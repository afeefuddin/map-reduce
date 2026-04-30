package worker

import "hash/fnv"

func Emit(key, value string) {
	idx := decideReducer(key)
	kv := []string{key, value}
	MapperData[idx] = append(MapperData[idx], kv)
}

func decideReducer(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(MapReduceWorkerConfig.ReducerCount))
}
