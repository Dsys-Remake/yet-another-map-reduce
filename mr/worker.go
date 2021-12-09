package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"io/ioutil"
	"sort"
	"strconv"
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

		if reply.Tasktype == MAP {

			data, err := ioutil.ReadFile(reply.Filename)
			if err != nil {
				return
			}
		
			kv := mapf(reply.Filename, string(data))
			res := CallForSubmit(reply)
			if  res.Status == CONTINUE {
				storeKeyValuesToTempFile(kv, reply.ReduceWorkers)				
			}
		} else if reply.Tasktype == REDUCE {

			kv := getSortedKeyValuesFromTempFile(reply.Filename)
			kv = collectUniqueAndRunReduce(reducef, kv)
			res := CallForSubmit(reply)
			if res.Status == CONTINUE {
				StoreReduceOutput(kv, outputFileName(reply.Filename))
				os.Remove(reply.Filename)
			}
		} else if reply.Tasktype == SNOOZE {
			time.Sleep(200*time.Millisecond)
		} else{
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func outputFileName(tempFileName string) string {
	var pos int
	fmt.Sscanf(tempFileName, intermediateFilePrefix + "%d", &pos)
	// log.Printf("%d %s",pos, tempFileName)
	return outputFilePrefix + strconv.Itoa(pos)
}
func collectUniqueAndRunReduce(reducef func(string, []string) string, intermediate []KeyValue) []KeyValue {
	res := []KeyValue{}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		res = append(res, KeyValue{Key: intermediate[i].Key, Value: output})

		i = j
	}

	return res
}

func StoreReduceOutput(kv []KeyValue,  outFile string) error {
	file, err := os.OpenFile(outFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, item := range kv {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", item.Key, item.Value)
	}

	return nil
}

func getSortedKeyValuesFromTempFile(fileName string) []KeyValue {
	f, err := os.Open(fileName)
	if err != nil {
		return nil
	}
	defer f.Close()
	list := []KeyValue{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var key, value string
		fmt.Sscanf(scanner.Text(), "%s %s", &key, &value)
		list = append(list, KeyValue{Key: key, Value: value})
	}

	sort.Slice(list, func (i, j int) bool{
		return list[i].Key < list[j].Key
	})

	return list
}

func storeKeyValuesToTempFile(kv []KeyValue, nReduce int) {
	
	separatedData := make([][]KeyValue, nReduce)
	for _, item := range kv {
		index := ihash(item.Key) % nReduce
		separatedData[index] = append(separatedData[index], item)
	}

	for i, data := range separatedData {
		if len(data) == 0 {
			continue
		}

		tmpFileName := intermediateFilePrefix + strconv.Itoa(i)
	
		file, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			continue
		}
		defer file.Close()
		for _, item := range data {
			fmt.Fprintf(file, "%s %s\n", item.Key, item.Value)
		}

	}
}


func CallForTask() TaskReply {
	args := TaskArgs{os.Getpid()}
	reply := TaskReply{}

	err := call("Coordinator.DemandTask", &args, &reply)

	if !err {
		// log.Printf("Worker process: %v will exit\n", args.WorkerId)
	}
	return reply
}

func CallForSubmit(taskReply TaskReply) SubmissionReply {
	args := SubmissionArgs{
		WorkerId: os.Getpid(),
		Filename: taskReply.Filename,
		Tasktype: taskReply.Tasktype,
	}

	reply := SubmissionReply{}

	err := call("Coordinator.SubmitTask", &args, &reply)

	if !err {
		// log.Println("Error sending intermediate results to Coordinator")
		return SubmissionReply{Status: ABORT}
	}

	return reply
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
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
