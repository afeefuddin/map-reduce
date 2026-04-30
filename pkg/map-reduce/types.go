package mapreduce

type Mapper func(chunk string)
type Reducer func()

type MapReduceConfig struct {
	Mapper       Mapper
	Reducer      Reducer
	MasterAddr   string
	WorkerId     string
	WorkerAdd    string
	ReducerCount int
}
