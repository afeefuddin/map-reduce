package main

import (
	"gomr/pkg/gomr"
	"strconv"
	"strings"
)

func mapper(ctx gomr.Context, chunk string) {
	lines := strings.SplitSeq(chunk, "\n")

	for l := range lines {
		words := strings.SplitSeq(l, " ")
		for word := range words {
			ctx.Emit(word, "1")
		}
	}
}

func reducer(ctx gomr.Context, key string, val []int) {
	v := 0

	for _, count := range val {
		v += count
	}

	ctx.Emit(key, strconv.Itoa(v))
}

func main() {
	gomr.Run(&gomr.MapReduceConfig{
		Mapper:  mapper,
		Reducer: reducer,
	})
}
