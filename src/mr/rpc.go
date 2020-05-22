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

type EmptyArgs struct {
}

type EmptyReply struct {
}

type GetMapJobReply struct {
	Filename string
}

type ReturnMapArgs struct {
	WordCounts []KeyValue
	Filename   string
}

type GetPhaseReply struct {
	Phase string
}

type GetReduceJobReply struct {
	Word   string
	Counts []string
}

type ReturnReduceArgs struct {
	Word string
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
