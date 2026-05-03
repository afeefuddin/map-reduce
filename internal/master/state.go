package master

import (
	"slices"
	"sync"
	"time"
)

type WorkerData struct {
	Address string
}

type WorkerSchedulerData struct {
	Id           string
	Addr         string
	lastWorkedAt *time.Time
	startedAt    *time.Time
}

type WorkerSchedulerNode struct {
	WorkerSchedulerData
	Next *WorkerSchedulerNode
}

type MasterState struct {
	workers         map[string]WorkerData
	mu              sync.RWMutex
	mapTasks        []MapTask
	reduceTasks     []Task
	WorkersSchedule []WorkerSchedulerData
}

type Task struct {
	Id        string
	startedAt *time.Time
	completed bool
}

type MapTask struct {
	Task
	input  string
	output string
}

type MasterConfig struct {
	MappersCount  int
	ReducersCount int
	WorkersCount  int
	Input         string
}

var MasterStateData *MasterState
var ThresholdTime int = 60

func (w *WorkerSchedulerData) isBusy() bool {
	if w.startedAt == nil {
		return false
	}

	return w.startedAt.After(time.Now().Add(-10 * time.Second))
}

func (w *WorkerSchedulerData) MarkFree() {
	w.startedAt = nil
}

func (t *Task) IsOnGoingTask() bool {
	if t.startedAt == nil {
		return false
	}

	return t.startedAt.After(time.Now().Add(-time.Duration(ThresholdTime) * time.Second))
}

func (t *Task) IsCompleted() bool {
	return t.completed
}

func (t *Task) MarkCompleted() {
	t.completed = true
}

func GetNextWorker() *WorkerSchedulerData {
	MasterStateData.mu.Lock()
	defer MasterStateData.mu.Unlock()

	totalWorkers := len(MasterStateData.WorkersSchedule)
	if totalWorkers == 0 {
		return nil
	}

	// WorkersSchedule is maintained as an LRU queue (least-recently-used at front).
	// Rotate busy workers to the back until a free/expired one reaches the front.
	for _ = range totalWorkers {
		worker := &MasterStateData.WorkersSchedule[0]
		if !worker.isBusy() {
			return worker
		}

		MasterStateData.WorkersSchedule = append(
			MasterStateData.WorkersSchedule[1:],
			MasterStateData.WorkersSchedule[0],
		)
	}

	return nil
}

func (w *WorkerSchedulerData) StartWork(now time.Time) {
	w.lastWorkedAt = &now
	w.startedAt = &now

	idx := slices.IndexFunc(MasterStateData.WorkersSchedule, func(element WorkerSchedulerData) bool { return element.Id == w.Id })
	if idx < 0 {
		return
	}

	totalWorkers := len(MasterStateData.WorkersSchedule)
	if totalWorkers <= 1 || idx == totalWorkers-1 {
		return
	}

	temp := MasterStateData.WorkersSchedule[totalWorkers-1]
	MasterStateData.WorkersSchedule[totalWorkers-1] = MasterStateData.WorkersSchedule[idx]
	MasterStateData.WorkersSchedule[idx] = temp
}
