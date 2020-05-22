package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

type Master struct {
	phase           string
	files           []string
	filesInProcess  map[string]bool
	filesCount      int
	filesMu         sync.Mutex
	intermediate    [][]KeyValue
	reduceReply     []GetReduceJobReply
	reduceInProcess map[string]bool
	intermediateMu  sync.Mutex
	reduceReplyMu   sync.Mutex
	done            bool
	reduceCount     int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetMapJob(args *EmptyArgs, reply *GetMapJobReply) error {
	// println("master GetMapJob")
	func() {
		defer m.filesMu.Unlock()
		m.filesMu.Lock()
		if len(m.files) > 0 {
			var filename string
			filename, m.files = m.files[0], m.files[1:]
			reply.Filename = filename
			m.filesInProcess[filename] = true
			go func() {
				defer m.filesMu.Unlock()
				time.Sleep(10 * time.Second)
				m.filesMu.Lock()
				if processing := m.filesInProcess[filename]; processing {
					m.filesInProcess[filename] = false
					m.files = append(m.files, filename)
				}
			}()
		}
	}()
	return nil
}

func (m *Master) ReturnMap(args *ReturnMapArgs, reply *EmptyReply) error {
	println("master ReturnMap")
	func() {
		defer m.intermediateMu.Unlock()
		defer m.filesMu.Unlock()
		m.intermediateMu.Lock()
		m.filesMu.Lock()
		if m.filesInProcess[args.Filename] {
			m.filesInProcess[args.Filename] = false
			m.intermediate = append(m.intermediate, args.WordCounts)
		}
	}()
	return nil
}

func (m *Master) GetPhase(args *EmptyArgs, reply *GetPhaseReply) error {
	// println("master GetPhase")
	reply.Phase = m.phase
	return nil
}

func (m *Master) GetReduceJob(args *EmptyArgs, reply *GetReduceJobReply) error {
	// println("master GetReduceJob")
	func() {
		defer m.reduceReplyMu.Unlock()
		m.reduceReplyMu.Lock()
		if len(m.reduceReply) > 0 {
			var ret GetReduceJobReply
			ret, m.reduceReply = m.reduceReply[0], m.reduceReply[1:]
			reply.Word = ret.Word
			reply.Counts = ret.Counts
			m.reduceInProcess[ret.Word] = true
			go func() {
				defer m.reduceReplyMu.Unlock()
				time.Sleep(10 * time.Second)
				m.reduceReplyMu.Lock()
				if processing := m.reduceInProcess[ret.Word]; processing {
					m.reduceInProcess[ret.Word] = false
					m.reduceReply = append(m.reduceReply, ret)
				}
			}()
		}
	}()
	return nil
}

func (m *Master) ReturnReduce(args *ReturnReduceArgs, reply *EmptyReply) error {
	defer m.reduceReplyMu.Unlock()
	m.reduceReplyMu.Lock()
	if m.reduceInProcess[args.Word] {
		m.reduceInProcess[args.Word] = false
		m.reduceCount = m.reduceCount - 1
	}
	return nil
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.done

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		phase:           "map",
		files:           files,
		filesInProcess:  make(map[string]bool),
		filesCount:      len(files),
		filesMu:         sync.Mutex{},
		intermediate:    [][]KeyValue{},
		intermediateMu:  sync.Mutex{},
		done:            false,
		reduceReplyMu:   sync.Mutex{},
		reduceInProcess: make(map[string]bool),
	}

	// Your code here.
	go mrMaster(&m)
	m.server()
	return &m
}

func mrMaster(m *Master) {
	for len(m.intermediate) != m.filesCount {
		fmt.Printf("waiting for map done - filesCount: %v intermediate: %v \n", m.filesCount, len(m.intermediate))
		time.Sleep(time.Second)
	}
	reduceSource := []KeyValue{}
	for _, kvs := range m.intermediate {
		reduceSource = append(reduceSource, kvs...)
	}
	sort.Sort(ByKey(reduceSource))
	i := 0
	for i < len(reduceSource) {
		j := i + 1
		for j < len(reduceSource) && reduceSource[j].Key == reduceSource[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, reduceSource[k].Value)
		}
		m.reduceReply = append(m.reduceReply, GetReduceJobReply{
			Word:   reduceSource[i].Key,
			Counts: values,
		})

		i = j
	}
	m.phase = "reduce"
	println("start reduce phase")
	m.reduceCount = len(m.reduceReply)
	for m.reduceCount != 0 {
		fmt.Printf("waiting for reduce done - reduceCount: %v \n", m.reduceCount)
		time.Sleep(time.Second)
	}
	m.phase = "done"
	m.done = true
}
