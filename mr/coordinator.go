package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type workerStatus int

const (
	QUEUED workerStatus = iota
	RUNNING
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	InputFilesLocation []string
	MappingInputStatus map[string]int
	IsMappingComplete bool
	NumOfReduceTasks int
	ReduceTasksStatus []int
	IsReducingComplete bool
	MLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DemandTask(args *TaskArgs, reply *TaskReply) error {
	reply.TaskType = "snooze"
	log.Printf("Worker %d demanded task\n", args.WorkerId)

	c.MLock.Lock()
	defer c.MLock.Unlock()

	if !c.IsMappingComplete {
		for file, status := range c.MappingInputStatus {
			if status == QUEUED {
				reply.Filename = file
				reply.TaskType = "map"
				c.MappingInputStatus[file] = RUNNING
				break
			}
		}
	}
	else if !c.IsReducingComplete {
		for i, status := range c.ReduceTasksStatus {
			if status == QUEUED {
				fmt.Sprintf(reply.Filename, "mr-int-%d", i)
				reply.TaskType = "reduce"
				c.ReduceTasksStatus[i] = RUNNING
				break
			}
		}
	}

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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.MLock.Lock()
	defer c.MLock.Unlock()

	ret = c.IsReducingComplete

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// Initialising the coordinator
	c.IsReducingComplete = false
	c.NumOfReduceTasks = nReduce
	c.ReduceTasksStatus = make([]int, nReduce)
	for i := 0; i < nReduce; ++i {
		c.ReduceTasksStatus[i] = QUEUED
	}

	c.IsMappingComplete = false
	c.MappingInputStatus = make(map[string]int)
	for _, file := range files {
		c.InputFilesLocation = append(c.InputFilesLocation, file)
		c.MappingInputStatus[file] = QUEUED
	}

	c.server()
	return &c
}
