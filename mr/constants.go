package mr

type workerStatus int

const (
	QUEUED workerStatus = iota
	RUNNING
	COMPLETED
)


type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	SNOOZE
)

type SubmissionReplyStatus int

const (
	CONTINUE SubmissionReplyStatus = iota
	ABORT
)


const intermediateFilePrefix string = "mr-int-"
const outputFilePrefix string  = "mr-out-"