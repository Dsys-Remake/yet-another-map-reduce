# yet-another-map-reduce
Reinventing the wheel with a distributed map reduce implementation in golang.

The implementation would be based around what is given in this [MIT 6.824](https://pdos.csal.mit.edu/6.824/labs/lab-mr.html) and the mapreduce paper.

## Functionality

- One Coordinator to distribute tasks, multiple workers to run tasks.
- Single machine architecture with RPC communication
- Worker asks task from coordinator and run the computation and produce more files
- The coordinator should check if the workers are doing their job or not.
- If the worker hasn't completed its task in a fixed amount of time then defer task to another worker
- The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the argument that main/mrcoordinator.go passes to MakeCoordinator().
- The worker implementation should put the output of the X'th reduce task in the file  mr-out-X.
- A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- main/mrcoordinator.go expects mr/coordinator.go to implement a Done() method that returns true when the MapReduce job is completely finished; at that point, mrcoordinator.go will exit.

## Improvements
- Mock a distributed architecture by using docker for various processes, using TCP/IP instead of UNIX sockets.

### Coordinator States
- InputFilesLocation []string
- MappingInputStatus map[string]int (Queued, Running, Completed)
- IsMappingComplete - bool
- NumOfReduceTasks - int
- ReduceTasksStatus - map[string]int (Queued, Running, Completed)
- IsReducingComplete - bool
- Lock - for synchronisation

### RPC Calls
- Worker --> Coordinator (DemandTask){args: "", reply: {"map"/"reduce", filename}}
- Worker --> Coordinator (TaskResult){args: map/reduce-taskName, reply: "Received"}

- Note: Coordinator will have bgTask goroutine with a ticker for 10 seconds. If no reply comes from the channel, reset the task to "Queued" and then a Worker will pickup the task. 
- Note: Worker will keep asking the Coordinator for task every 2 seconds (using Sleep), if Coordinator can't be connected then Worker program will exit.

## Extras
- Setting up a golang project modules and packages. 
