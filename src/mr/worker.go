package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// each worker produce at max NResuce intermediate files 
// first using temperory files "mr-tmp-* record" all kv pairs stores in one bucket
// after writen done, close the file and rename it to "mr-mapTaskID-reduceID"
func mapTaskHelper(mapTaskID int, nReduce int, kvs []KeyValue) {
	buckets := make([][]KeyValue, nReduce) 

	// in case of 

	for _, kv := range kvs {
		bID := ihash(kv.Key) % nReduce
		buckets[bID] = append(buckets[bID], kv)
	}

	for i := 0; i < nReduce; i++ {
		if buckets[i] == nil {
			continue
		}
		reduceID := i
		// make temporary files in case of error fail
		//pattern := fmt.Sprintf("")
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			panic(err)
		}
		tmpName := tmpFile.Name()

		enc := json.NewEncoder(tmpFile)
		for _, kv := range buckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal(err)
			}
		}
		tmpFile.Close()

		// rename after close file
		finalName := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceID)
		//atomic rename
		os.Rename(tmpName, finalName)
	}
}

//debug function print all key and value pairs in a list
func printKV(kv []KeyValue) {
	for _, v := range kv {
		fmt.Printf("{%s, %s}", v.Key, v.Value)
	}
	fmt.Printf("\n")
}

func aggreagateIntermediateFiles(nMap int, reduceID int) []KeyValue {
	kva := []KeyValue{}
	for x := 0; x < nMap; x++ {
		//read relate intermediate files to this reduce worker
		fileName := fmt.Sprintf("mr-%d-%d", x, reduceID)
		//fmt.Printf("I am in aggregate function, fileName I need is: %s\n", fileName)

		file, err := os.Open(fileName)
		//defer file.Close()
		if os.IsNotExist(err) {
			continue
		}
		if (err != nil) {
			log.Fatal("error:", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break;
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	//fmt.Printf("I am in aggregate function, reduceID = %d\n", reduceID)
	return kva
}

// reducef accept a key and a list containning all his value, and put the output in a file
func reduceAndOutputOneline(preKey string, valList []string,
	 tmpFile *os.File, reducef func(string, []string) string) {
	outPut := reducef(preKey, valList)

	line := fmt.Sprintf("%v %v\n", preKey, outPut)
	tmpFile.WriteString(line)
}

func reduceHelper(intermediate []KeyValue, tmpFile *os.File, 
	reducef func(string, []string) string, reduceID int) {
	// loop all kv pairs in list, for every single key build a []keyValue
	// which stores all the value for this key, and pass the key and list to reducef
	// at last write "key reduceOutput" to the file tmp-out-reduceID
	tmpOutName := tmpFile.Name()
	valList := []string{}
	preKey := intermediate[0].Key
	for i := 0; i < len(intermediate); i++ {
		kv := intermediate[i]
		key := kv.Key
		val := kv.Value

		//preValList := valList
		if key != preKey {
			// reducef return the number of occurrences of this word
			//outPut := reducef(preKey, valList)

			//line := fmt.Sprintf("%v %v\n", preKey, outPut)
			//tmpFile.WriteString(line)
			reduceAndOutputOneline(preKey, valList, tmpFile, reducef)

			valList = []string{val}
			preKey = key
		} else {
			valList = append(valList, val)
		}
	}
	reduceAndOutputOneline(preKey, valList, tmpFile, reducef)

	tmpFile.Close()
	// rename after close file
	finalName := fmt.Sprintf("mr-out-%d",reduceID)
	//atomic rename
	os.Rename(tmpOutName, finalName)

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	i := 0
	for {
		fmt.Printf("I am worker: %d\n", i)
		requestArgs := RequestArgs{WorkerID: i}
		requestReply := RequestReply{}

		ok := call("Coordinator.RequestTask", &requestArgs, &requestReply)
		if !ok {
			fmt.Printf("call failed!\n")
			// Coordinator might have finished or is temporarily down.
            // In the crash test, it's safest to exit the worker here.
            return 
		}
		taskID := requestReply.TaskID
		fileName := requestReply.FileName
		taskType := requestReply.TaskType
		NReduce := requestReply.NReduce
		NMap := requestReply.NMap


		fmt.Printf("taskID is: %d, fileName is: %s, taskType is : %d, NReduce is: %d, NMap is: %d\n",
		 taskID, fileName, taskType, NReduce, NMap)

		switch taskType {
		case 0:
			//map, ihash and store correspond intermediate files
			fileContent, err := os.ReadFile(fileName)
			if err != nil {
				log.Fatal("error:", err)
			}
			kvs := mapf(fileName, string(fileContent))
			mapTaskHelper(taskID, NReduce, kvs)


			updateArgs := UpdateArgs{TaskID: taskID, TaskType: taskType, WorkerID: i}
			updateReply := UpdateReply{}
			ok := call("Coordinator.UpdateTask", &updateArgs, &updateReply)
			if !ok {
				fmt.Printf("call failed!\n")
				// Coordinator might have finished or is temporarily down.
            	// In the crash test, it's safest to exit the worker here.
            	return 
			}
		case 1:
			//reduce, for each intermediate files in a certain reduceID sort pairs, for loop each pairs in current reduceID
			// aggreagate all values belong to the same key to a list, and pass key and list 
			// to reducel get one line %v, %v output, save the line in mr-out-reduceID
			// after file close rename filename]

			// read all files into a list, and sort this list
			intermediate := aggreagateIntermediateFiles(NMap, taskID)
			if len(intermediate) == 0 {
				i++
				continue
			}
			sort.Sort(ByKey(intermediate))

			//printKV(intermediate)

			tmpFile, err := os.CreateTemp(".", "mrout-tmp-*")
			if err != nil {
				panic(err)
			}

			fmt.Printf("intermediate list length is: %d!\n", len(intermediate))

			// save the output of reducef on intermediate pairs to files
			reduceHelper(intermediate, tmpFile, reducef, taskID)

			updateArgs := UpdateArgs{TaskID: taskID, TaskType: taskType, WorkerID: i}
			updateReply := UpdateReply{}
			ok := call("Coordinator.UpdateTask", &updateArgs, &updateReply)
			if !ok {
				fmt.Printf("call failed!\n")
			}
		case 2:
			//wait
			time.Sleep(500 * time.Millisecond)
		case 3:
			os.Exit(0)
		}
		i++
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
