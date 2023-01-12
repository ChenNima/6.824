package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

type TakeMapArgs struct {
}

type TakeMapReply struct {
	// file name for current map job
	Filename string
	// number of map task
	MapNo int
	// NReduce for divide map res to buckets
	NReduce int
	// flag that need to change phase to reduce
	MapFinished bool
}

type CommitMapArgs struct {
	Filename string
}

type EmptyReply struct {
}

type TakeReduceArgs struct {
}

type TakeReduceReply struct {
	ReduceNo       int
	ReduceFinished bool
}

type CommitReducerArgs struct {
	ReduceNo int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
