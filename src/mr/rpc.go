package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

// Add your RPC definitions here.
type TaskArgs struct {
}

const (
	Map int = iota
	Reduce
)

type Task struct {
	Index     int
	Type      int    //task type
	FileName  string //target address
	MapNum    int
	ReduceNum int
	StartAt   time.Time
	InQueue bool
	IsRuning bool
	Isdone    bool
}

// 请求任务参数
type ReqTaskArgs struct {
	// 当前worker存活,可以执行任务
	// WorkerStatus bool
}

// 请求任务返回值
type ReqTaskReply struct {
	// 返回一个任务
	Task Task
	// 是否完成所有任务
	// TaskDone bool
}

type ReportTaskArgs struct {
	Type   int
	Index  int
	Isdone bool
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
