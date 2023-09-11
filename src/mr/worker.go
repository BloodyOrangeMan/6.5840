package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// ByKey is a type for a slice of KeyValue pairs that implements sort.Interface
// to sort by Key.
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// Call the Heartbeat RPC to get a task from the coordinator
		hbReply, err := CallHeartbeat()
		if err != nil {
			return
		}

		if hbReply.Response.ShouldRun {
			switch hbReply.Response.Phase {
			case MapPhase:
				handleMapTask(hbReply.Response.Task, mapf)
			case ReducePhase:
				handleReduceTask(hbReply.Response.Task, reducef)
			case DonePhase:
				return
			}
		}
		time.Sleep(1 * time.Second) // Wait for some time before requesting the next task
	}
}

func CallHeartbeat() (HeartbeatReply, error) {
	args := HeartbeatArgs{}
	var reply HeartbeatReply
	ok := call("Coordinator.Heartbeat", &args, &reply)
	if !ok {
		return HeartbeatReply{}, fmt.Errorf("Heartbeat call failed")
	}
	return reply, nil
}

func CallReport(request ReportRequest) error {
	args := ReportArgs{
		Request: request,
	}
	var reply ReportReply
	ok := call("Coordinator.Report", &args, &reply)
	if !ok {
		return fmt.Errorf("Report call failed")
	}
	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	input, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		CallReport(ReportRequest{
			TaskID:  task.Id,
			Success: false,
			Phase:   MapPhase,
			Err:     err.Error(),
		})
		return
	}

	kva := mapf(task.FileName, string(input))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		index := ihash(kv.Key) % task.NReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	for i, kvs := range intermediate {
		ofile, _ := os.Create(reduceName(task.Id, i))
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Printf("Failed to encode kv %v: %v", kv, err)
			}
		}
		ofile.Close()
	}

	CallReport(ReportRequest{
		TaskID:  task.Id,
		Success: true,
		Phase:   MapPhase,
		Err:     "",
	})
}

func reduceName(mapTask int, reduceTask int) string {
	return fmt.Sprintf("mr-%d-%d", mapTask, reduceTask)
}

func handleReduceTask(task Task, reducef func(string, []string) string) {
	var kva []KeyValue
	for i := 0; i < task.NMap; i++ {
		file, err := os.Open(reduceName(i, task.Id))
		if err != nil {
			log.Fatalf("cannot open %v", reduceName(i, task.Id))
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	CallReport(ReportRequest{
		TaskID:  task.Id,
		Success: true,
		Phase:   ReducePhase,
		Err:     "",
	})
}
