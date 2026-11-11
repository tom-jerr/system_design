package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type ExampleArgs struct{ X int }
type ExampleReply struct{ Y int }

type RegisterWorkerArgs struct{}
type RegisterWorkerReply struct{ WorkerID int }

type RequestTaskArgs struct{ WorkerID int }
type RequestTaskReply struct {
	Type     string // "map", "reduce", "wait", "exit"
	TaskID   int
	Attempt  uint64
	Filename []string
	NReduce  int
	NMap     int
}
type ReportTaskArgs struct {
	Type     string
	TaskID   int
	WorkerID int
	Attempt  uint64
	Filename []string
}
type ReportTaskReply struct{ Accepted bool }

const taskTimeout = 10 * time.Second

// IDs are unique within this coordinator's lifetime, including worker restarts.
func (c *Coordinator) RegisterWorker(_ *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextWorkerID++
	reply.WorkerID = c.nextWorkerID
	return nil
}

func (c *Coordinator) advancePhase() {
	if c.phase == "map" && c.allMapTasksFinished() {
		c.phase = "reduce"
	}
	if c.phase == "reduce" && c.allReduceTasksFinished() {
		c.phase = "done"
	}
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advancePhase()
	if c.phase == "done" {
		reply.Type = "exit"
		return nil
	}
	tasks := c.mapTasks
	if c.phase == "reduce" {
		tasks = c.reduceTasks
	}
	for i := range tasks {
		task := &tasks[i]
		if task.state == Idle || task.state == InProgress && time.Since(task.startTime) >= taskTimeout {
			task.state = InProgress
			task.worker = args.WorkerID
			task.attempt++
			task.startTime = time.Now()
			*reply = RequestTaskReply{Type: c.phase, TaskID: task.id, Attempt: task.attempt,
				Filename: append([]string(nil), task.filename...), NReduce: len(c.reduceTasks), NMap: len(c.mapTasks)}
			return nil
		}
	}
	// Polling ensures expired tasks are reconsidered even if nobody reports completion.
	reply.Type = "wait"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Type != c.phase {
		return nil
	}
	var tasks []Task
	switch args.Type {
	case "map":
		tasks = c.mapTasks
	case "reduce":
		tasks = c.reduceTasks
	default:
		return nil
	}
	if args.TaskID < 0 || args.TaskID >= len(tasks) {
		return nil
	}
	task := &tasks[args.TaskID]
	if task.state != InProgress || task.worker != args.WorkerID || task.attempt != args.Attempt {
		return nil
	}
	if args.Type == "map" {
		if len(args.Filename) != len(c.reduceTasks) {
			return fmt.Errorf("map task %d: wrong output count", args.TaskID)
		}
		for i, filename := range args.Filename {
			c.reduceTasks[i].filename = append(c.reduceTasks[i].filename, filename)
		}
	} else {
		if len(args.Filename) != 1 {
			return fmt.Errorf("reduce task %d: wrong output count", args.TaskID)
		}
		// Publish only the current attempt, while holding the same lock used for reassignment.
		if err := os.Rename(args.Filename[0], filepath.Join(filepath.Dir(args.Filename[0]), fmt.Sprintf("mr-out-%d", args.TaskID))); err != nil {
			return err
		}
	}
	task.state = Completed
	reply.Accepted = true
	c.advancePhase()
	return nil
}
