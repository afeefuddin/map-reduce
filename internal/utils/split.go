package utils

import (
	"bufio"
	"fmt"
	"gomr/internal/storage"
	"strings"
)

func ShardDataAndUpload(inputFile string, count int) ([]string, error) {
	obj, err := storage.GetObject(inputFile)

	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(obj)

	shards := make([][]string, count)
	i := 0

	for scanner.Scan() {
		line := scanner.Text()
		shardId := i % count
		shards[shardId] = append(shards[shardId], line)
		i++
	}

	var shardedData []string

	for i, shard := range shards {
		data := strings.Join(shard, "\n")
		splittedInputFile := fmt.Sprintf("splitted-input/input-shard-%d.txt", i)
		if err := storage.UploadData(splittedInputFile, data); err != nil {
			return nil, err
		}

		shardedData = append(shardedData, splittedInputFile)
	}

	return shardedData, nil
}
