package mapreduce

type Mapper func(chunk string)
type Reducer func(key string, value []int)

type MapReduceConfig struct {
	Mapper       Mapper
	Reducer      Reducer
	MasterAddr   string
	WorkerId     string
	WorkerAdd    string
	ReducerCount int
}
