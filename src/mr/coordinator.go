package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	mapl []Task
	reducel []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestArgs, reply *RequestReply) error {
	log.Printf("Handler: RequestTask started") // LOG 1
	c.mu.Lock()
	log.Printf("Handler: RequestTask acquired lock") // LOG 2
	defer c.mu.Unlock()
	fmt.Printf("Coordinator begin deal with request!\n")
	inProgressNum := 0
	for i, t := range c.mapl {
		if t.State == "idle" {
			t.State = "in-progress"
			t.WorkerID = args.WorkerID
			t.StartTime = time.Now()

			reply.TaskType = t.TaskType
			reply.TaskID = i
			reply.FileName = t.FileName
			return nil
		} else if t.State == "in-progress" {
			inProgressNum++
		}
	}
	if (inProgressNum != 0) {
		reply.TaskType = 2
		return nil
	}

	// only after map phase is finished can do reduce work
	for i, t := range c.reducel {
		if t.State == "idle" {
			t.State = "in-progress"
			t.WorkerID = args.WorkerID
			t.StartTime = time.Now()

			reply.TaskType = t.TaskType
			reply.TaskID = i
			reply.FileName = t.FileName
			return nil
		} else if t.State == "in-progress" {
			inProgressNum++
		}
	}
	return nil
}

func (c *Coordinator) UpdateTask(args *UpdateArgs, reply *UpdateReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskID := args.TaskID
	workerID := args.WorkerID

	switch args.TaskType {
	case 0: // map
		if c.mapl[taskID].State == "completed" {
			return nil
		}
		if c.mapl[taskID].WorkerID == workerID && c.mapl[taskID].State == "in-progress" {
			c.mapl[taskID].State = "completed"
		}
		
	case 1:
		if c.reducel[taskID].State == "completed" {
			return nil
		}
		if c.reducel[taskID].WorkerID == workerID && c.reducel[taskID].State == "in-progress" {
			c.reducel[taskID].State = "completed"
		}
	}
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	for _, t := range c.mapl {
		if t.State == "idle" || t.State == "in-progress" {
			return ret
		}
	}

	for _, t := range c.reducel {
		if t.State == "idle" || t.State == "in-progress" {
			return ret
		}
	}
	fmt.Printf("coodrdinator is done!\n")
	ret = true
	return ret
}

func (c *Coordinator) CheckTimeout() {
	for {
		time.Sleep(500 * time.Millisecond)
		c.mu.Lock()

		for _, t := range c.mapl {
			if t.State == "in-progress" && time.Since(t.StartTime) >= 10 * time.Second {
				t.State = "idle"
			}
	    }
		c.mu.Unlock()
	}


}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// files may be "pg1.txt pg2.txt pg3.txt"
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.mapl = make([]Task, len(files))
	c.reducel = make([]Task, nReduce)

	nMap := len(files)
	for i, fileName := range files {
		// i is index of files
		c.mapl[i] = Task{TaskType: 0, TaskID: i, FileName: fileName, State: "idle", NReduce: nReduce, NMap: nMap}
	}
	for i := 0; i < nReduce; i++ {
		c.reducel[i] = Task{TaskType: 1, TaskID: i, State: "idle", NReduce: nReduce, NMap: nMap}
	}
	go c.CheckTimeout()
	c.server()
	fmt.Printf("the server is working\n")
	return &c
}
