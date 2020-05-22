package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
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

var phase = "map"

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// reduceFiles := make(map[string]*os.File)
	filename := "start"
	go checkReduce()
	for true {
		reply := GetMapJobReply{}
		call("Master.GetMapJob", &EmptyArgs{}, &reply)
		filename = reply.Filename
		if filename == "" {
			if phase == "reduce" {
				break
			} else {
				continue
			}
		}
		fmt.Printf("get job from master: %s\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		args := ReturnMapArgs{
			WordCounts: kva,
			Filename:   filename,
		}
		call("Master.ReturnMap", &args, &EmptyReply{})
	}

	for true {
		reduceReply := GetReduceJobReply{}
		call("Master.GetReduceJob", &EmptyArgs{}, &reduceReply)
		if reduceReply.Word == "" {
			if phase == "done" {
				break
			} else {
				continue
			}
		}
		oname := fmt.Sprintf("mr-out-%v", ihash(reduceReply.Word)%10)
		// fmt.Printf("received word: %s; filename: %s\n", reduceReply.Word, oname)
		// if _, ok := reduceFiles[oname]; !ok {
		// 	reduceFiles[oname], _ = os.Create(oname)
		// }
		output := reducef(reduceReply.Word, reduceReply.Counts)
		// fmt.Fprintf(reduceFiles[oname], "%v %v\n", reduceReply.Word, output)

		file, _ := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if _, err := file.WriteString(fmt.Sprintf("%v %v\n", reduceReply.Word, output)); err != nil {
			log.Println(err)
		}
		call("Master.ReturnReduce", &ReturnReduceArgs{
			Word: reduceReply.Word,
		}, &EmptyReply{})
	}

	// for _, file := range reduceFiles {
	// 	file.Close()
	// }
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func checkReduce() {
	for true {
		phaseReply := GetPhaseReply{}
		call("Master.GetPhase", &EmptyArgs{}, &phaseReply)
		phase = phaseReply.Phase
		if phaseReply.Phase == "done" {
			break
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("current phase: %s\n", phase)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
