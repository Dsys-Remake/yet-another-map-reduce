package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	Tasktype TaskType
	Filename string
	ReduceWorkers int
}

type SubmissionArgs struct {
	WorkerId int
	Tasktype TaskType
	Filename string
}

type SubmissionReply struct {
	Status string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
