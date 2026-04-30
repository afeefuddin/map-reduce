package mapreduce

func Perform(config *MapReduceConfig) {
	if config.Mapper == nil || config.Reducer == nil {
		panic("Mapper or Reducer not defined")
	}
}
