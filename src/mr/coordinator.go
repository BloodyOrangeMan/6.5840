package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type SchedulePhase int

const (
	WaitPhase SchedulePhase = iota
	MapPhase
	ReducePhase
	DonePhase
)

type HeartbeatResponse struct {
	Phase     SchedulePhase
	Task      Task
	ShouldRun bool
}

type ReportRequest struct {
	TaskID  int
	Success bool
	Phase   SchedulePhase
	Err     string
}

// A laziest, worker-stateless, channel-based implementation of Coordinator
type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
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

// Heartbeat is an RPC method that is called by a worker to get a new task
func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	hb := heartbeatMsg{
		response: &reply.Response,
		ok:       make(chan struct{}),
	}
	c.heartbeatCh <- hb
	<-hb.ok
	return nil
}

// Report is an RPC method that is called by a worker to report the completion of a task
func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	report := reportMsg{
		request: &args.Request,
		ok:      make(chan struct{}),
	}
	c.reportCh <- report
	<-report.ok
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	select {
	case _, ok := <-c.doneCh:
		return !ok // if channel is closed, then return true
	default:
		return false
	}
}

func (c *Coordinator) handleHeartbeat(hb *heartbeatMsg) {
	// Find a task for the worker
	task, shouldRun := c.findTask()
	hb.response.Phase = c.phase
	hb.response.Task = task
	hb.response.Task.StartTime = time.Now()
	hb.response.ShouldRun = shouldRun
	hb.ok <- struct{}{}
}

func (c *Coordinator) handleReport(report *reportMsg) {
	if report.request.Success {
		for i, task := range c.tasks {
			if task.Id == report.request.TaskID {
				c.tasks[i].Status = Completed
				break
			}
		}
	} else {
		log.Printf("Error from worker for task %d in phase %d: %s", report.request.TaskID, report.request.Phase, report.request.Err)
	}
	report.ok <- struct{}{}
}

func (c *Coordinator) findTask() (Task, bool) {
	for _, task := range c.tasks {
		if task.Status == Idle {
			task.Status = InProgress
			return task, true
		}
	}
	return Task{}, false
}

func (c *Coordinator) scheduler() {
	ticker := time.NewTicker(100 * time.Millisecond) // Ticker for periodically checking phase transitions

	for {
		select {
		case hb := <-c.heartbeatCh:
			c.handleHeartbeat(&hb)
		case report := <-c.reportCh:
			c.handleReport(&report)
		case <-ticker.C:
			c.checkPhaseTransition() // Check if all tasks are done and transition phase if needed
		default:
			if c.phase == DonePhase {
				close(c.doneCh)
				ticker.Stop()
				return
			}
		}
	}
}

func (c *Coordinator) checkPhaseTransition() {
	allDone := true
	for _, task := range c.tasks {
		if task.Status != Completed {
			allDone = false
			break
		}
	}
	if allDone {
		if c.phase == MapPhase {
			c.phase = ReducePhase
			// Initialize reduce tasks
			c.tasks = make([]Task, c.nReduce)
			for i := 0; i < c.nReduce; i++ {
				c.tasks[i] = Task{
					Id:     i,
					Status: Idle,
					NMap:   c.nMap,
				}
			}
		} else if c.phase == ReducePhase {
			c.phase = DonePhase
			c.doneCh <- struct{}{}
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i] = Task{
			FileName: file,
			Id:       i,
			Status:   Idle,
			NReduce:  nReduce,
			NMap:     len(files),
		}
	}
	c := &Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		phase:       MapPhase,
		tasks:       tasks,
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}),
	}
	c.server()
	go c.scheduler()
	return c
}
