package main

import (
	"map-reduce/internal/master"
)

func main() {
	master.StartMaster(master.MasterConfig{MappersCount: 5, ReducersCount: 2, WorkersCount: 5, Input: "input/wordcount.txt"})
}
