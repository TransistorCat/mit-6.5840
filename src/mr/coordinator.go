package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	mapPhrase    int = 0
	reducePhrase int = 1
)

type Coordinator struct {
	// Your definitions here.
	nmap       int
	nreduce    int
	taskQueue  chan Task
	taskRecord []*Task
	taskPhrase int
	success    chan int
	files      []string
	taskNum    int
	mu         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) HandleTaskReq(args *ReqTaskArgs, reply *ReqTaskReply) error {

	task, ok := <-c.taskQueue
	task.StartAt = time.Now()
	task.IsRuning = true
	c.taskRecord[task.Index].StartAt = time.Now()
	c.taskRecord[task.Index].IsRuning = true
	c.taskRecord[task.Index].InQueue = false
	if !ok {
		return errors.New("TaskReq error")
	}
	reply.Task = task

	return nil
}

func (c *Coordinator) HandleTaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if !args.Isdone {
		c.addTask(args.Index)
		fmt.Printf("retry no.%v task\n", args.Index)
	} else if c.taskRecord[args.Index].Isdone {
		return nil
	} else {
		fmt.Printf("finish no.%v task\n", args.Index)
		// c.taskRecord[args.Index].Isdone = true
		c.mu.Lock()
		c.taskRecord[args.Index].Isdone = true
		c.taskRecord[args.Index].IsRuning = false
		c.taskNum--
		fmt.Printf("现在taskNum为%d\n", c.taskNum)
		c.mu.Unlock()
	}
	return nil

}

func (c *Coordinator) initMapTask() {
	for i := 0; i < c.nmap+c.nreduce; i++ {

		task := Task{
			FileName:  "",
			Index:     i,
			Type:      Reduce,
			MapNum:    c.nmap,
			ReduceNum: c.nreduce,
			Isdone:    false,
			IsRuning:  false,
			InQueue:   false,
		}
		if i < c.nmap {
			task.FileName = c.files[i]
			task.Type = Map
			c.taskRecord[i] = &task

			c.taskQueue <- task
			c.taskRecord[i].InQueue = true
		} else {
			c.taskRecord[i] = &task
		}

	}
}

func (c *Coordinator) initReduceTask() {
	fmt.Println("initReduceTask")
	for i := 0; i < c.nreduce; i++ {
		c.addTask(i + c.nmap)
	}

}
func (c *Coordinator) addTask(index int) {
	c.taskQueue <- *c.taskRecord[index]
	c.taskRecord[index].InQueue = true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.taskNum == c.nreduce {
		c.initReduceTask()
		c.taskPhrase = reducePhrase
	}
	if c.taskNum == 0 && c.taskPhrase == reducePhrase {
		ret = true
		close(c.taskQueue)
		return ret
	}
	c.mu.Unlock()

	for k, _ := range c.taskRecord {
		c.checkTask(k)
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// func (c *Coordinator) heartBeat(index int, success chan int) {
// 	select {
// 	case <-success:

//		case <-time.After(5 * time.Second):
//			fmt.Println("timed out")
//			c.addTask(index)
//		}
//	}
func (c *Coordinator) checkTask(taskIndex int) {
	timeDuration := time.Since(c.taskRecord[taskIndex].StartAt)
	if timeDuration > 10*time.Second && c.taskRecord[taskIndex].IsRuning && !c.taskRecord[taskIndex].InQueue {
		// 任务超时重新加入队列
		c.addTask(taskIndex)
		c.taskRecord[taskIndex].InQueue = true
		fmt.Printf("timeout no.%d\n", taskIndex)
	}
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nmap = len(files)
	c.nreduce = nReduce
	c.taskNum = c.nmap + c.nreduce
	c.taskQueue = make(chan Task, c.nmap+c.nreduce)
	c.taskRecord = make([]*Task, c.nmap+c.nreduce) //task数取决于10*fileNum
	c.success = make(chan int, 1)
	c.taskPhrase = mapPhrase
	c.files = files // Your code here.
	c.initMapTask()
	c.server()
	// c.checkTask()

	return &c
}
func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
