package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"


type Coordinator struct {
	// Your definitions here.
	InputFilesLocation []string
	MappingInputStatus map[string]workerStatus
	IsMappingComplete bool
	NumOfReduceTasks int
	ReduceTasksStatus []workerStatus
	IsReducingComplete bool
	MLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.


func (c *Coordinator) DemandTask(args *TaskArgs, reply *TaskReply) error {
	// log.Printf("Worker %d demanded task\n", args.WorkerId)
	reply.Tasktype = SNOOZE

	c.MLock.Lock()
	defer c.MLock.Unlock()

	if !c.IsMappingComplete {
		for file, status := range c.MappingInputStatus {
			if status == QUEUED {
				reply.Filename = file
				reply.Tasktype = MAP
				reply.ReduceWorkers = c.NumOfReduceTasks
				c.MappingInputStatus[file] = RUNNING
				break
			}
		}
	} else if !c.IsReducingComplete {
		for i, status := range c.ReduceTasksStatus {
			if status == QUEUED {
				formatString := intermediateFilePrefix + "%d"
				reply.Filename = fmt.Sprintf(formatString , i)
				reply.Tasktype = REDUCE
				reply.ReduceWorkers = c.NumOfReduceTasks
				c.ReduceTasksStatus[i] = RUNNING
				break
			}
		}
	} 

	if reply.Tasktype != SNOOZE {
		go c.startTimer(*args, *reply)
	}

	return nil
}

func (c *Coordinator) startTimer(args TaskArgs, reply TaskReply) {
	ticker := time.NewTicker(10*time.Second)
	defer ticker.Stop()

	var pos int
	var filename string
	if reply.Tasktype == REDUCE {
		fmt.Sscanf(reply.Filename, intermediateFilePrefix + "%d", &pos)
	} else if reply.Tasktype == MAP {
		filename = reply.Filename
	}

	for {
		select {
		case <-ticker.C:
			if reply.Tasktype == REDUCE  {
				c.MLock.Lock()
				c.ReduceTasksStatus[pos] = QUEUED
				c.MLock.Unlock()
			} else if reply.Tasktype == MAP {
				c.MLock.Lock()
				c.MappingInputStatus[filename] = QUEUED
				c.MLock.Unlock()
			}
			// log.Printf("Worker %d failed to deliver in 10s\n",args.WorkerId)
			return

		default:
			if reply.Tasktype == REDUCE {
				c.MLock.Lock()
				if c.ReduceTasksStatus[pos] == COMPLETED {
					c.MLock.Unlock()
					return 
				} else {
					c.MLock.Unlock()
				}
			} else if reply.Tasktype == MAP {
				c.MLock.Lock()
				if c.MappingInputStatus[filename] == COMPLETED {
					c.MLock.Unlock()
					return 
				} else {
					c.MLock.Unlock()
				}
			}
		}
	}
}

func (c *Coordinator) SubmitTask(args *SubmissionArgs, reply *SubmissionReply) error {
	c.MLock.Lock()
	defer c.MLock.Unlock()

	if args.Tasktype == MAP {
		file := args.Filename
		
		c.MappingInputStatus[file] = COMPLETED
	} else if args.Tasktype == REDUCE {
		var pos int
		fmt.Sscanf(args.Filename, intermediateFilePrefix + "%d", &pos)

		c.ReduceTasksStatus[pos] = COMPLETED
	}

	// log.Printf("Worker %d submitted results\n", args.WorkerId)
	reply.Status = "Server Received the report!!"
	
	go c.checkStatus(args.Tasktype)

	return nil
}

func (c *Coordinator) checkStatus(taskType TaskType) {
	flag := true

	c.MLock.Lock()
	defer c.MLock.Unlock()

	if taskType == MAP {
		for _, value := range c.MappingInputStatus {
			flag = (flag && (value == COMPLETED))
		}
		c.IsMappingComplete = flag
	} else if taskType == REDUCE {
		for _, value := range c.ReduceTasksStatus {
			flag = (flag && (value == COMPLETED))
		}
		c.IsReducingComplete = flag
	}
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
	c := Coordinator{
		InputFilesLocation: files,
		MappingInputStatus: make(map[string]workerStatus),
		IsMappingComplete: false,
		NumOfReduceTasks: nReduce,
		ReduceTasksStatus: make([]workerStatus, nReduce),
		IsReducingComplete: false,
	}

	// Your code here.

	// Initialising the coordinator
	for index, _ := range c.ReduceTasksStatus {
		c.ReduceTasksStatus[index] = QUEUED
	}

	for _, file := range files {
		c.MappingInputStatus[file] = QUEUED
	}

	c.server()
	return &c
}
