package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu    sync.Mutex
	tasks []Task
}

var cr = Coordinator{}

type workerStatus int

const (
	UNASSIGNED workerStatus = iota
	RUNNING
	HUNG
	DONE
)

type Task struct {
	Filename string
	WorkerID int
	Status   workerStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskForWork(args *AskForWorkArgs, reply *AskForWorkReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, _ := range cr.tasks {
		task := &cr.tasks[index]
		if task.Status == UNASSIGNED {
			reply.Filename = task.Filename
			task.Status = RUNNING
			task.WorkerID = args.WorkerId
			break
		}
	}
	fmt.Println(reply)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	for _, file := range files {
		cr.tasks = append(cr.tasks, Task{Filename: file, WorkerID: -1, Status: UNASSIGNED})
	}

	cr.server()
	return &cr
}
