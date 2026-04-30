package master

import (
	"context"
	"fmt"
	"log"
	workerpb "map-reduce/gen/worker"
	"map-reduce/internal/utils"
	"time"
)

func OrchestrateMapReduce(ctx context.Context, config MasterConfig) {
	shardedData, err := utils.ShardDataAndUpload(config.Input, config.MappersCount)

	if err != nil {
		log.Fatalf("Error sharding the data %s", err)
		return
		// panic("Error sharding the data")
	}

	// append all the map tasks
	for idx, data := range shardedData {
		task := MapTask{
			Task: Task{
				Id:        fmt.Sprintf("map-%d", idx),
				input:     data,
				completed: false,
			},
		}

		MasterStateData.mapTasks = append(MasterStateData.mapTasks, task)
	}

	// keep looping till you finish the map task

	for {
		log.Printf("Looooooppping to finish %d tasks", len(MasterStateData.mapTasks))
		taskCompletedCount := 0
		for _, task := range MasterStateData.mapTasks {
			if task.completed {
				taskCompletedCount++
				continue
			}

			if task.IsOngoingTask() {
				continue
			}

			for {
				worker := GetNextWorker()

				if worker != nil {
					// assign this worker
					MasterStateData.mu.Lock()
					// rpc

					log.Printf("Found a pod, %s", worker.Addr)
					client, err := GetClient(worker.Addr)

					if err != nil {
						MasterStateData.mu.Unlock()
						continue
					}

					_, err = client.AssignTask(ctx, &workerpb.TaskRequest{
						Id:       task.Id,
						Type:     "map",
						Location: task.input,
					})

					log.Printf("Map task assigned to %s", worker.Id)

					if err != nil {
						MasterStateData.mu.Unlock()
						continue
					}

					now := time.Now()
					worker.StartWork(now)
					task.startedAt = &now
					MasterStateData.mu.Unlock()
					break
				} else {
					log.Printf("No Worker sad life :(")
				}

				time.Sleep(time.Second)
			}
		}

		if taskCompletedCount == len(MasterStateData.mapTasks) {
			break
		}
	}

	log.Println("All map tasks completed!")

}
