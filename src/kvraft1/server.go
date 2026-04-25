package kvraft

import (
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"

)


type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu           sync.Mutex
	db map[string]string             // datebase contains key/value pair
	lastAppliedSeq map[int64]int     // clientID -> the last applied sequence number
	lastOpResult map[int64]Result	 // clientID -> Result

}

func (kv *KVServer) isDuplicate(clientID int64, seqNum int) bool {

}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// argument and return value should be Op struct in order to consistent with
	// submit function in rsm.go
	op, ok := req.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. type assertion, if not ok return
	if !ok {
		fmt.Println("type assertion failed")
		return
	}
	// 2. check duplicate
	if kv.isDuplicate(op.ClientID, op.SeqNum) {
		if (op.Operation != "Get") {
			// if the request is put and duplicate, just return the lastest result
			return kv.lastOpResult[op.clientID]
		}
	}

	// 3. do operation in service database
	res := Result{}
	switch op.Operation {
	case "Get":
		val, ok := kv.db[op.Key]
		if !ok {
			res.Err = rpc.ErrNoKey
			return res
		}
		res.Val = val
		res.Err = rpc.OK
	case "Put":
		kv.db[op.Key] = op.Val
		res.Err = rpc.OK
		// 4. record the result to kv
		kv.lastAppliedSeq[op.ClientID] = op.SeqNum
		kv.lastOpResult[op.ClientID] = res
	}
	return res
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	op := Op{
		Operation: Get, 
		Key: args.Key, 
		ClientID: args.ClientID
		SeqNum: args.SeqNum
	}
	
	ok, res := kv.rsm.Submit(op)

	reply.

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.m = make(map[string]string)
	return []tester.IService{kv, kv.rsm.Raft()}
}
