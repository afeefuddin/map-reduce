package api

type Context interface {
	Emit(key, value string) error
}

type Mapper func(ctx Context, chunk string)
type Reducer func(ctx Context, key string, value []int)

type MapReduceConfig struct {
	Mapper       Mapper
	Reducer      Reducer
	ReducerCount int
}
