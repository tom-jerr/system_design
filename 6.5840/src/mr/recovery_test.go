package mr

import (
	"os"
	"testing"
	"time"
)

func TestRecoveryAttempts(t *testing.T) {
	c := &Coordinator{phase: "map", mapTasks: []Task{{id: 0, filename: []string{"input"}}}, reduceTasks: []Task{{id: 0}}}
	register := func() int {
		var reply RegisterWorkerReply
		c.RegisterWorker(&RegisterWorkerArgs{}, &reply)
		return reply.WorkerID
	}
	w1, w2 := register(), register()
	if w1 == w2 {
		t.Fatal("worker IDs reused")
	}
	request := func(worker int) RequestTaskReply {
		var reply RequestTaskReply
		c.RequestTask(&RequestTaskArgs{WorkerID: worker}, &reply)
		return reply
	}
	report := func(task RequestTaskReply, worker int, files ...string) bool {
		t.Helper()
		var reply ReportTaskReply
		if err := c.ReportTask(&ReportTaskArgs{Type: task.Type, TaskID: task.TaskID, Attempt: task.Attempt, WorkerID: worker, Filename: files}, &reply); err != nil {
			t.Fatal(err)
		}
		return reply.Accepted
	}
	first := request(w1)
	if first.Type != "map" || request(w2).Type != "wait" {
		t.Fatal("expected map assignment and waiting worker")
	}
	// Simulate a crashed worker's expired lease without waiting ten seconds.
	c.mapTasks[0].startTime = time.Now().Add(-taskTimeout)
	second := request(w2)
	if second.Type != "map" || second.Attempt <= first.Attempt {
		t.Fatal("expired map was not reassigned")
	}
	if report(first, w1, "stale") || len(c.reduceTasks[0].filename) != 0 {
		t.Fatal("stale map committed")
	}
	if !report(second, w2, "current") {
		t.Fatal("current map rejected")
	}
	if report(second, w2, "current") || len(c.reduceTasks[0].filename) != 1 {
		t.Fatal("duplicate map inputs")
	}
	reduce := request(w2)
	if reduce.Type != "reduce" || len(reduce.Filename) != 1 || reduce.Filename[0] != "current" {
		t.Fatal("incorrect reduce inputs")
	}
	c.reduceTasks[0].startTime = time.Now().Add(-taskTimeout)
	// Even the same worker must use a new attempt after reassignment.
	retry := request(w2)
	if retry.Type != "reduce" || retry.Attempt <= reduce.Attempt {
		t.Fatal("expired reduce was not reassigned")
	}
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(oldDir) })
	if err := os.WriteFile("stale", []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("current", []byte("current"), 0600); err != nil {
		t.Fatal(err)
	}
	if report(reduce, w2, "stale") {
		t.Fatal("old attempt accepted")
	}
	if !report(retry, w2, "current") {
		t.Fatal("current reduce rejected")
	}
	if report(reduce, w2, "stale") {
		t.Fatal("late reduce accepted")
	}
	data, err := os.ReadFile("mr-out-0")
	if err != nil || string(data) != "current" {
		t.Fatalf("bad output: %q, %v", data, err)
	}
	if !c.Done() || request(w1).Type != "exit" {
		t.Fatal("job did not finish")
	}
}
