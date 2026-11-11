package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	coordSockName = sockname

	registered := RegisterWorkerReply{}
	if !call("Coordinator.RegisterWorker", &RegisterWorkerArgs{}, &registered) {
		return
	}
	workerID := registered.WorkerID
	for {
		reply := RequestTaskReply{}
		if !call("Coordinator.RequestTask", &RequestTaskArgs{WorkerID: workerID}, &reply) {
			return
		}
		var files []string
		var err error
		switch reply.Type {
		case "map":
			files, err = runMapTask(reply, mapf)
		case "reduce":
			files, err = runReduceTask(reply, reducef)
		case "wait":
			time.Sleep(200 * time.Millisecond)
			continue
		case "exit":
			return
		default:
			log.Printf("unknown task type %q", reply.Type)
			return
		}
		if err != nil {
			log.Printf("%s task %d: %v", reply.Type, reply.TaskID, err)
			return
		}
		report := ReportTaskArgs{Type: reply.Type, TaskID: reply.TaskID, WorkerID: workerID,
			Attempt: reply.Attempt, Filename: files}
		result := ReportTaskReply{}
		// A lost reply leaves the commit outcome unknown; preserve the files in that case.
		if !call("Coordinator.ReportTask", &report, &result) {
			return
		}
		if !result.Accepted {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}
}

// RPC filenames must be absolute: the coordinator may have a different working directory.
func createTaskFile(pattern string) (*os.File, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, pattern)
}

// Each attempt owns private files. Unfinished attempts never become reduce inputs.
func runMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) (files []string, err error) {
	defer func() {
		if err != nil {
			for _, file := range files {
				os.Remove(file)
			}
		}
	}()
	contents, err := os.ReadFile(task.Filename[0])
	if err != nil {
		return nil, err
	}
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.Filename[0], string(contents)) {
		if task.NReduce > 0 {
			r := ihash(kv.Key) % task.NReduce
			buckets[r] = append(buckets[r], kv)
		}
	}
	for r, bucket := range buckets {
		f, e := createTaskFile(fmt.Sprintf("mr-map-%d-%d-%d-*", task.TaskID, task.Attempt, r))
		if e != nil {
			return files, e
		}
		files = append(files, f.Name())
		enc := json.NewEncoder(f)
		for _, kv := range bucket {
			if e = enc.Encode(&kv); e != nil {
				f.Close()
				return files, e
			}
		}
		if e = f.Close(); e != nil {
			return files, e
		}
	}
	return files, nil
}

func runReduceTask(task RequestTaskReply, reducef func(string, []string) string) (files []string, err error) {
	var kva []KeyValue
	for _, filename := range task.Filename {
		f, e := os.Open(filename)
		if e != nil {
			return nil, e
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			e = dec.Decode(&kv)
			if e == io.EOF {
				break
			}
			if e != nil {
				f.Close()
				return nil, e
			}
			kva = append(kva, kv)
		}
		if e = f.Close(); e != nil {
			return nil, e
		}
	}
	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })
	f, err := createTaskFile(fmt.Sprintf("mr-reduce-%d-%d-*", task.TaskID, task.Attempt))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			os.Remove(f.Name())
		}
	}()
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = kva[k].Value
		}
		if _, err = fmt.Fprintf(f, "%v %v\n", kva[i].Key, reducef(kva[i].Key, values)); err != nil {
			f.Close()
			return nil, err
		}
		i = j
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	return []string{f.Name()}, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
