package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	TaskType int       // 0 represent map; 1 represent reduce
	TaskID int		   // index in mapl or reducel
	WorkerID int       // only record worker ID when task state is 1 or 2
	FileName []string  // filename for map input
	State int          // 0 presents idle; 1 presents in-progress; 2 presents completed
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestArgs struct {
	WorkerID int
}

type RequestReply struct {
	TaskType int
	TaskID int
	FileName []string
	NReduce int
	NMap int
}

type UpdateArgs struct {
	TaskType int
	TaskID int
	WorkerID int
}

type UpdateReply struct {
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
