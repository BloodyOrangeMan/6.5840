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

type Task struct {
	FileName  string
	Id        int
	StartTime time.Time
	Status    TaskStatus
	NReduce   int
	NMap      int
}

// HeartbeatArgs are the arguments that a worker will send when calling the Heartbeat RPC
type HeartbeatArgs struct{}

// HeartbeatReply is what the coordinator will reply with when the Heartbeat RPC is called
type HeartbeatReply struct {
	Response HeartbeatResponse
}

// ReportArgs are the arguments that a worker will send when calling the Report RPC
type ReportArgs struct {
	Request ReportRequest
}

// ReportReply is what the coordinator will reply with when the Report RPC is called
type ReportReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
