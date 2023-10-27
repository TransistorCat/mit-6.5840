package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// fmt.Printf("ready go")
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := getTask()
		runTask(mapf, reducef, reply)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func getTask() *Task {
	args := ReqTaskArgs{}
	reply := ReqTaskReply{}
	call("Coordinator.HandleTaskReq", &args, &reply)
	// if ok {
	// 	fmt.Printf("recive reply\n")
	// } else {
	// 	fmt.Printf("call failed\n")
	// }
	return &reply.Task
}

func runTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, task *Task) {
	switch task.Type {
	case Map:
		file, err := os.Open(task.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.FileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.FileName)
		}
		file.Close()
		kva := mapf(task.FileName, string(content))
		//通过key分成reduceNum份
		for i := 0; i < task.ReduceNum; i++ {
			oname := fmt.Sprintf("mr-%d-%d", task.Index, i)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
			for _, kv := range kva {
				if ihash(kv.Key)%task.ReduceNum == i {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", err)
					}
				}
			}
		}

	case Reduce:

		res := make(map[string][]string)
		for i := 0; i < task.MapNum; i++ {
			oname := fmt.Sprintf("mr-%d-%d", i, task.Index-task.MapNum)
			ofile, _ := os.Open(oname)
			dec := json.NewDecoder(ofile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				res[kv.Key] = append(res[kv.Key], kv.Value)
			}
		}

		var keys []string
		for k := range res {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		outputFileName := fmt.Sprintf("mr-out-%d", task.Index-task.MapNum)
		outputfile, _ := os.Create(outputFileName)
		for _, k := range keys {
			output := reducef(k, res[k])
			fmt.Fprintf(outputfile, "%v %v\n", k, output)
		}
		outputfile.Close()

	}
	ReportTask(task.Type, task.Index, true)
}

func ReportTask(Type int, Index int, Isdone bool) {
	args := ReportTaskArgs{
		Type:   Type,
		Index:  Index,
		Isdone: Isdone,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.HandleTaskReport", &args, &reply)
	if ok {
		fmt.Printf("report no.%d success\n", args.Index)
	} else {
		fmt.Printf("report failed\n")
	}
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
