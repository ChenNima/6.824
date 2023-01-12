package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
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

type worker struct {
	nReduce int
	// 0-map 1-reduce 2-finish
	phase int
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("worker started")
	mWorker := worker{
		phase: 0,
	}
	for mWorker.phase == 0 {
		DoMapTask(&mWorker, mapf)
	}
	fmt.Println("Map finished, start reducing")

	for mWorker.phase == 1 {
		DoReduceTask(&mWorker, reducef)
	}
	fmt.Println("Reduce finished, exiting")
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func DoMapTask(mWorker *worker, mapf func(string, string) []KeyValue) {
	args := TakeMapArgs{}
	reply := TakeMapReply{}
	call("Master.TakeMap", &args, &reply)
	(*mWorker).nReduce = reply.NReduce
	if reply.MapFinished {
		(*mWorker).phase = 1
		return
	}
	if reply.Filename == "" {
		// no file available, waiting
		return
	}
	filename := reply.Filename
	mapNo := reply.MapNo
	fmt.Printf("Start map %s, taskNo %d\n", filename, mapNo)
	fmt.Println("Start map: ", filename)
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
	kvBucket := make(map[int][]KeyValue)
	for _, kv := range kva {
		bucketNo := ihash(kv.Key) % mWorker.nReduce
		kvs, prs := kvBucket[bucketNo]
		if !prs {
			kvBucket[bucketNo] = []KeyValue{kv}
		} else {
			kvBucket[bucketNo] = append(kvs, kv)
		}
	}
	for bucketNo, kvs := range kvBucket {
		ofileName := fmt.Sprintf("mr-%d-%d", mapNo, bucketNo)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Writing to: ", ofileName)
		tempfile, _ := ioutil.TempFile("./", "mapper")
		enc := json.NewEncoder(tempfile)
		enc.Encode(&kvs)
		os.Rename(tempfile.Name(), ofileName)
		tempfile.Close()
	}
	commitArgs := CommitMapArgs{
		Filename: filename,
	}
	commitReply := EmptyReply{}
	call("Master.CommitMap", &commitArgs, &commitReply)
	fmt.Println("Process finished: ", filename)
}

func DoReduceTask(mWorker *worker, reducef func(string, []string) string) {
	args := TakeReduceArgs{}
	reply := TakeReduceReply{}
	call("Master.TakeReduce", &args, &reply)
	if reply.ReduceFinished {
		mWorker.phase = 2
		return
	}
	if reply.ReduceNo == -1 {
		return
	}
	reducer := reply.ReduceNo
	fmt.Println("Start reduce: ", reducer)
	files, err := filepath.Glob(fmt.Sprintf("./mr-*-%d", reducer))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(files)
	kvMap := make(map[string][]string)
	for _, filename := range files {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		kva := []KeyValue{}
		for {
			if err := dec.Decode(&kva); err != nil {
				break
			}
		}
		file.Close()

		for _, kv := range kva {
			values, pres := kvMap[kv.Key]
			if !pres {
				kvMap[kv.Key] = []string{kv.Value}
			} else {
				kvMap[kv.Key] = append(values, kv.Value)
			}
		}
	}

	ofileName := fmt.Sprintf("mr-out-%d", reducer)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Writing to: ", ofileName)
	tempfile, err := ioutil.TempFile("./", "reducer")
	if err != nil {
		fmt.Println(err)
	}

	for key, values := range kvMap {
		output := reducef(key, values)
		fmt.Fprintf(tempfile, "%v %v\n", key, output)
	}

	err = tempfile.Close()
	if err != nil {
		fmt.Println(err)
	}
	err = os.Rename(tempfile.Name(), ofileName)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Writing to: ", ofileName, " success, start commit")
	commitArgs := CommitReducerArgs{
		ReduceNo: reducer,
	}
	commitReply := EmptyReply{}
	call("Master.CommitReducer", &commitArgs, &commitReply)
	fmt.Println("Reducer process finished: ", reducer)
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
