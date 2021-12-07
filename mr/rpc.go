package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	SNOOZE
)

// Add your RPC definitions here.
type TaskArgs struct {
	WorkerId int
}

type TaskReply struct {
	TaskType TaskType
	Filename string
}

type SubmissionArgs struct {
	WorkerId int
	TaskType TaskType
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
