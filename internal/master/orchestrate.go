package master

import (
	"context"
	"fmt"
	workerpb "gomr/gen/worker"
	"gomr/internal/utils"
	"log"
	"strconv"
	"time"
)

type taskExecutor func(taskIndex int, worker *WorkerSchedulerData)

func taskLoop(n int, isCompleted, isOnGoing func(i int) bool, executor taskExecutor) {
	for {
		taskCompletedCount := 0
		for i := range n {
			if isCompleted(i) {
				taskCompletedCount++
				continue
			}

			if isOnGoing(i) {
				continue
			}

			for {
				worker := GetNextWorker()

				if worker != nil {
					executor(i, worker)
					break
				} else {
					time.Sleep(time.Second)
				}
			}
		}

		if taskCompletedCount == n {
			break
		}
	}
}

func OrchestrateMapReduce(ctx context.Context, config MasterConfig) {
	shardedData, err := utils.ShardDataAndUpload(config.Input, config.MappersCount)

	if err != nil {
		log.Fatalf("Error sharding the data %s", err)
		return
	}

	// append all the map tasks
	for idx, data := range shardedData {
		task := MapTask{
			Task: Task{
				Id:        fmt.Sprintf("map-%d", idx),
				completed: false,
			},
			input: data,
		}

		MasterStateData.mapTasks = append(MasterStateData.mapTasks, task)
	}

	// keep looping till you finish the map task
	taskLoop(
		len(MasterStateData.mapTasks),
		func(i int) bool {
			return MasterStateData.mapTasks[i].IsCompleted()
		},
		func(i int) bool {
			return MasterStateData.mapTasks[i].IsOnGoingTask()
		},
		startMapTask,
	)

	log.Println("All map tasks completed!")

	// Now push the reducer taks
	for i := range config.ReducersCount {
		MasterStateData.reduceTasks = append(MasterStateData.reduceTasks, Task{
			Id:        strconv.Itoa(i),
			startedAt: nil,
			completed: false,
		})
	}

	taskLoop(
		len(MasterStateData.reduceTasks),
		func(i int) bool {
			return MasterStateData.reduceTasks[i].IsCompleted()
		},
		func(i int) bool {
			return MasterStateData.reduceTasks[i].IsOnGoingTask()
		},
		startReduceTask,
	)

	log.Println("Map reduce completed")
}

func startMapTask(taskIndex int, worker *WorkerSchedulerData) {
	MasterStateData.mu.Lock()
	task := MasterStateData.mapTasks[taskIndex]
	// rpc

	log.Printf("Found a pod, %s", worker.Addr)
	client, err := GetClient(worker.Addr)

	if err != nil {
		MasterStateData.mu.Unlock()
		return
	}

	ctx := context.Background()
	_, err = client.AssignTask(ctx, &workerpb.TaskRequest{
		Id:       task.Id,
		Type:     "map",
		Location: task.input,
	})

	log.Printf("Map task assigned to %s", worker.Id)

	if err != nil {
		MasterStateData.mu.Unlock()
		return
	}

	now := time.Now()
	worker.StartWork(now)
	task.startedAt = &now
	MasterStateData.mu.Unlock()
}

func startReduceTask(taskIndex int, worker *WorkerSchedulerData) {
	MasterStateData.mu.Lock()
	// rpc
	task := MasterStateData.mapTasks[taskIndex]

	log.Printf("Found a pod, %s", worker.Addr)
	client, err := GetClient(worker.Addr)

	if err != nil {
		MasterStateData.mu.Unlock()
		return
	}

	ctx := context.Background()
	_, err = client.AssignTask(ctx, &workerpb.TaskRequest{
		Id:       task.Id,
		Type:     "reduce",
		Location: "",
	})

	log.Printf("Map task assigned to %s", worker.Id)

	if err != nil {
		MasterStateData.mu.Unlock()
		return
	}

	now := time.Now()
	worker.StartWork(now)
	task.startedAt = &now
	MasterStateData.mu.Unlock()
}
