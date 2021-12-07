package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"time"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		reply := CallForTask()

		if reply.TaskType == MAP {

		} else if reply.TaskType == REDUCE {

		} else if reply.TaskType == SNOOZE {
			time.Sleep(time.Second)
		} else{
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallForTask() TaskReply {
	args := TaskArgs{os.Getpid()}
	reply := TaskReply{}

	err := call("Coordinator.DemandTask", &args, &reply)

	if !err {
		log.Printf("Worker process: %v will exit\n", args.WorkerId)
	}
	return reply
}

func CallForSubmit(taskReply TaskReply) {
	args := SubmissionArgs{
		WorkerId: os.Getpid(),
		Filename: taskReply.Filename,
		TaskType: taskReply.TaskType,
	}

	reply := SubmissionReply{}

	err := call("Coordinator.SubmitTask", &args, &reply)

	if !err {
		log.Println("Error sending intermediate results to Coordinator")
	}
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
