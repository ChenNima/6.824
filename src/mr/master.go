package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type fileState struct {
	// number of file task
	fileNo int
	// 0-unprocessed 1-processing 2-processed
	status int
}

type Master struct {
	// Your definitions here.
	nReduce      int
	files        []string
	fileStateMap map[string]*fileState
	mapRemain    int

	reducers        []int
	reducerStateMap map[int]int
	reduceRemain    int
	mu              sync.Mutex
}

func (m *Master) TakeMap(args *TakeMapArgs, reply *TakeMapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	reply.NReduce = m.nReduce
	if m.mapRemain == 0 {
		// all map finished, change to reduce
		reply.MapFinished = true
		return nil
	}
	if len(m.files) == 0 {
		// no file available, worker needs to wait
		reply.Filename = ""
		return nil
	}
	file := m.files[0]
	m.files = m.files[1:]
	reply.Filename = file
	fileState := m.fileStateMap[file]
	reply.MapNo = fileState.fileNo
	fileState.status = 1
	fmt.Println("file taken for mapping: ", file)
	go func() {
		// wait 10 sec
		time.Sleep(10 * time.Second)
		fmt.Println("Check completion: ", file)
		m.mu.Lock()
		defer m.mu.Unlock()
		fileState := m.fileStateMap[file]
		fmt.Println("Check completion: ", file, " - ", fileState.status)
		if fileState.status == 1 {
			// still processing, re-enqueue job
			fmt.Println("file timeout!: ", file)
			fileState.status = 0
			m.files = append(m.files, file)
		}
	}()
	return nil
}

func (m *Master) CommitMap(args *CommitMapArgs, reply *EmptyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	fileState := m.fileStateMap[args.Filename]
	if fileState.status == 2 {
		// already committed
		return nil
	}
	fileState.status = 2
	m.mapRemain--
	fmt.Println("file map committed: ", args.Filename)
	fmt.Println("remaining map: ", m.mapRemain)
	return nil
}

func (m *Master) TakeReduce(args *TakeReduceArgs, reply *TakeReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reduceRemain == 0 {
		reply.ReduceFinished = true
		return nil
	}
	if len(m.reducers) == 0 {
		reply.ReduceFinished = false
		reply.ReduceNo = -1
		return nil
	}
	reducer := m.reducers[0]
	m.reducers = m.reducers[1:]
	m.reducerStateMap[reducer] = 1
	reply.ReduceNo = reducer
	fmt.Println("reducer taken for mapping: ", reducer)
	go func() {
		// wait 10 sec
		time.Sleep(10 * time.Second)
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.reducerStateMap[reducer] == 1 {
			// still processing, re-enqueue reducer
			fmt.Println("reducer timeout!: ", reducer)
			m.reducerStateMap[reducer] = 0
			m.reducers = append(m.reducers, reducer)
		}
	}()
	return nil
}

func (m *Master) CommitReducer(args *CommitReducerArgs, reply *EmptyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reducerStateMap[args.ReduceNo] == 2 {
		// already committed
		return nil
	}
	m.reducerStateMap[args.ReduceNo] = 2
	m.reduceRemain--
	fmt.Println("reducer committed: ", args.ReduceNo)
	fmt.Println("remaining reducers: ", m.reduceRemain)
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	if m.reduceRemain == 0 {
		time.Sleep(1 * time.Second)
		return true
	}
	return false
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	fileMap := make(map[string]*fileState)
	for i, file := range files {
		state := fileState{
			fileNo: i,
			status: 0,
		}
		fileMap[file] = &state
	}
	reducers := []int{}
	reducerStateMap := make(map[int]int)
	for i := 0; i < nReduce; i++ {
		reducers = append(reducers, i)
		reducerStateMap[i] = 0
	}
	m := Master{
		nReduce:      nReduce,
		files:        files,
		fileStateMap: fileMap,
		mapRemain:    len(files),

		reducers:        reducers,
		reducerStateMap: reducerStateMap,
		reduceRemain:    nReduce,
	}
	fmt.Println(files)
	fmt.Println(nReduce)
	fmt.Println("Master started")
	// Your code here.

	m.server()
	return &m
}
