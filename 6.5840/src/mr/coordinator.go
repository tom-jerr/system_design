package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type Task struct {
	id        int
	state     TaskState
	worker    int
	attempt   uint64   // attempt number, incremented each time the task is reassigned
	filename  []string // for reduce tasks, this is the list of intermediate files from map tasks
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu           sync.Mutex
	nextWorkerID int
	// so we can not overlap map and reduce phases
	phase       string // "map" or "reduce" or "done"
	mapTasks    []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) allMapTasksFinished() bool {
	for _, task := range c.mapTasks {
		if task.state != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksFinished() bool {
	for _, task := range c.reduceTasks {
		if task.state != Completed {
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	server := rpc.NewServer()
	if err := server.Register(c); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, mux)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.phase = "map"
	c.mapTasks = make([]Task, len(files))
	for i, file := range files {
		c.mapTasks[i] = Task{id: i, filename: []string{file}}
	}
	c.reduceTasks = make([]Task, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{id: i}
	}
	c.server(sockname)
	return &c
}
