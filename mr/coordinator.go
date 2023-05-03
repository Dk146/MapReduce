package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task int64

const (
	MapTask Task = iota
	ReduceTask
)

type Status int64

const (
	Unstarted Status = iota
	Pending
	Finished
)

type Coordinator struct {
	// Your definitions here.
	// Map task number and filename
	MapJobs    map[int]*TaskStatus
	ReduceJobs map[int]*TaskStatus
	nReduce    int
	currentJob Task
}

type TaskStatus struct {
	filename    string
	status      Status
	startedTime int64
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AskJob(args *AskJobArgs, reply *AskJobReply) error {
	reply.NReduce = c.nReduce
	if c.currentJob == MapTask {
		reply.FileName, reply.TaskNumber = c.GetUnstartedMapJob()
		reply.TaskType = MapTask
	} else {
		reply.FileName, reply.TaskNumber = c.GetUnstartedReduceJob()
		reply.TaskType = ReduceTask
	}
	return nil
}

func (c *Coordinator) AckJob(arg *AckJobRequest, reply *AckJobResponse) error {
	if arg.TaskType == MapTask {
		c.MapJobs[arg.TaskNumber].status = Finished
		count := len(c.MapJobs)
		for _, v := range c.MapJobs {
			if v.status == Finished {
				count--
			}
		}
		if count == 0 {
			c.currentJob = ReduceTask
		}
	} else {
		c.ReduceJobs[arg.TaskNumber].status = Finished
	}
	return nil
}

func (c *Coordinator) GetUnstartedMapJob() (string, int) {
	for k, v := range c.MapJobs {
		if v.status == Unstarted {
			v.status = Pending
			v.startedTime = time.Now().Unix()
			return v.filename, k
		}
	}
	return "", -1
}

func (c *Coordinator) GetUnstartedReduceJob() (string, int) {
	count := 0
	for k, v := range c.ReduceJobs {
		if v.status == Unstarted {
			// fmt.Println("key", k, "   val", v.status)
			v.status = Pending
			v.startedTime = time.Now().Unix()
			return v.filename, k
		} else if v.status == Pending {
			count++
		}
	}
	if count != 0 {
		return "", -2
	}
	return "", -1
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true
	// Your code here.
	for _, v := range c.MapJobs {
		if v.status != Finished {
			ret = false
		}
		if v.status == Pending {
			if time.Now().Unix()-v.startedTime >= 10 {
				v.status = Unstarted
			}
		}
	}
	if !ret {
		return ret
	}
	for _, v := range c.ReduceJobs {
		if v.status != Finished {
			ret = false
		}
		if v.status == Pending {
			if time.Now().Unix()-v.startedTime >= 10 {
				v.status = Unstarted
			}
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.MapJobs = make(map[int]*TaskStatus)
	pos := 0
	for _, v := range files {
		c.MapJobs[pos] = &TaskStatus{filename: v, status: Unstarted, startedTime: 0}
		pos++
	}

	c.ReduceJobs = make(map[int]*TaskStatus)
	for i := 0; i < nReduce; i++ {
		c.ReduceJobs[i] = &TaskStatus{filename: fmt.Sprint(i), status: Unstarted, startedTime: 0}
	}

	c.nReduce = nReduce
	c.currentJob = MapTask

	c.server()
	return &c
}
