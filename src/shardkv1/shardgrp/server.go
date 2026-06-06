package shardgrp

import (
	"sync/atomic"
	"sync"
	"fmt"
	"bytes"


	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Result struct {
	Value string
	Err rpc.Err
	Version rpc.Tversion
}

type DBValue struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid // the groupID for this specific group

	// Your code here
	mu           sync.Mutex

	// You need a way to track which shards this group currently owns
    // There are usually 12 shards (shardcfg.NShards)
	// servingShards[i] is true if this group is responsible for Shard i
	serveringShards [shardcfg.NShards]bool

	// shardConfigNums tracks the version (Num) of the config that 
	// most recently updated the status of this specific shard.
	// shardgrps must remember the largest Num  they have seen for each shard.
	// update when get installShard
	shardConfigNums [shardcfg.NShards]shardcfg.Tnum

	// maintain all kv data for every shard in this group
	// We use an array of maps, one for each Shard ID. 
	// This makes migration (Freeze/Install) much easier
	shardsData [shardcfg.NShards]map[string]DBValue

	// Store deduplication information for each shard! 
	// Each shard has its own independent ClientID -> Result mapping.
	lastOpResult [shardcfg.NShards]map[int64]Result
	// each shard has its own last applied put seq num
	lastAppliedSeq [shardcfg.NShards]map[int64]int
}

type PersistState struct {
	ServeringShards [shardcfg.NShards]bool
	ShardConfigNums [shardcfg.NShards]shardcfg.Tnum
	ShardsData [shardcfg.NShards]map[string]DBValue
	lastOpResult [shardcfg.NShards]map[int64]Result
	lastAppliedSeq [shardcfg.NShards]map[int64]int
}

func (kv *KVServer) executeOp(op Op) Result {
	shard := op.Shard
	var res Result

	switch op.Operation {
	case "Get":
		verVal, ok := kv.shardsData[shard][op.Key]
		if !ok {
			res.Err = rpc.ErrNoKey
			return res
		}
		res.Value = verVal.Value
		res.Version = verVal.Version
		res.Err = rpc.OK
		return res

	case "Put":
		verVal, ok := kv.shardsData[shard][op.Key]
		if !ok && verVal.Version != 0 {
			res.Err = rpc.ErrNoKey
			return res
		}
		// If versions don't match, return ErrVersion.
		if op.Version != verVal.Version {
			res.Err = rpc.ErrVersion
			return res
		}
		// success, modify database
		kv.shardsData[shard][op.Key] = DBValue{Value: op.Value, Version: op.Version + 1}
		res.Err = rpc.OK
		// record the result to kv
		kv.lastAppliedSeq[shard][op.ClientID] = op.SeqNum
		kv.lastOpResult[shard][op.ClientID] = res
	}
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// argument should be Op struct in order to consistent with
	// submit function in rsm.go
	op, ok := req.(rsm.Op)
	var res Result

	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("S%d Applying %v", kv.me, op)

	// 1. type assertion, if not ok return
	if !ok {
		fmt.Println("type assertion failed")
	}

	switch op.Operation {
	case "Put", "Get":
		// first check to make sure:
		// 1. the group still contains this shard
		// 2. and the config Num is match
		if !kv.serveringShards[op.ShardID] || op.ConfigNum != kv.shardConfigNums[op.ShardID] {
			res.Err = rpc.ErrWrongGroup
			return
		}

		// then check has duplicate put operations or not
		if op.Operation != "Get" && kv.lastAppliedSeq[op.Shard][op.ClientID] >= op.SeqNum {
			return kv.lastOpResult[op.Shard][op.ClientID]
		}
		// truly execute the operation
		res = kv.executeOp(op)

	case "FreezeShard":




	}




	// 2. check duplicate
	if op.Operation == "Put" && kv.lastAppliedSeq[op.ClientID] >= op.SeqNum {
		// if the request is put and duplicate, just return the lastest result
		return kv.lastOpResult[op.ClientID]
	}

	// 3. do operation in service database
	res := Result{}
	switch op.Operation {
	case "Get":
		// if current group don't have this shard, return err ErrwrongGroup
		shard := shardcfg.Key2Shard(op.Key)
		if !kv.serveringShards[shard] {
			res.Err = rpc.ErrWrongGroup
			return
		}
		verVal, ok := kv.shardsData[shard][op.Key]
		if !ok {
			res.Err = rpc.ErrNoKey
			return res
		}
		res.Value = verVal.Value
		res.Version = verVal.Version
		res.Err = rpc.OK

	case "Put":
		// if current group don't have this shard, return err ErrwrongGroup
		shard := shardcfg.Key2Shard(op.Key)
		if !kv.serveringShards[shard] {
			res.Err = rpc.ErrWrongGroup
			return
		}
		verVal, ok := kv.shardsData[shard][op.Key]
		if !ok && verVal.Version != 0 {
			res.Err = rpc.ErrNoKey
			return res
		}
		// If versions don't match, return ErrVersion.
		if op.Version != verVal.Version {
			res.Err = rpc.ErrVersion
			return res
		}
		// success, modify database
		kv.shardsData[shard][op.Key] = DBValue{Value: op.Value, Version: op.Version + 1}
		res.Err = rpc.OK
		// record the result to kv
		kv.lastAppliedSeq[op.ClientID] = op.SeqNum
		kv.lastOpResult[op.ClientID] = res
	}
	return res
}


func (kv *KVServer) Snapshot() []byte {
	// Your code here
	// lock before serialize in case of panaic
	//  when the data is writing by other goroutines
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var ps PersistState
	//ps.Db = kv.db

	ps.ServeringShards = kv.serveringShards
	ps.ShardConfigNums = kv.shardConfigNums
	ps.ShardsData = kv.shardsData
	ps.LastOpResult = kv.lastOpResult
	ps.lastAppliedSeq = kv.lastAppliedSeq

	e.Encode(ps)
	
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	// while rsm invoke restore(), may have other clients change the data eg. kv.db
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ps PersistState

	if d.Decode(&ps) != nil {
		panic("resetore Persist: failed to decode server state")
	} else {
		kv.serveringShards = ps.ServeringShards
		kv.shardConfigNums = ps.ShardConfigNums
		kv.shardsData = ps.ShardsData
		kv.lastOpResult = ps.LastOpResult
		kv.lastAppliedSeq = ps.lastAppliedSeq
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	// find which shard has this key
	shard := shardcfg.Key2Shard(key)

	op := rsm.Op{
		Operation: "Get", 
		Key: args.Key, 
		ClientID: args.ClientID,
		SeqNum: args.SeqNum,
		ShardID: shard,
		ConfigNum: kv.shardConfigNums[shard],
	}
	
	DPrintf("S%d received %v", kv.me, op)

	// for a key whose shard is not assigned to the shardgrp
	// return rpc.ErrWrongGroup

	kv.mu.Lock()
	
	// this shardgrp didn't respond for this shard
	if !kv.serveringShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return 
	}
	kv.mu.Unlock()

	err, result := kv.rsm.Submit(op)

	// if server is not leader
    if err != rpc.OK {
        reply.Err = err
        return
    }

	res := result.(Result)
    reply.Err = res.Err
    reply.Value = res.Value
	reply.Version = res.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here	

	// find which shard has this key
	shard := shardcfg.Key2Shard(key)

	kv.mu.Lock()
	// this shardgrp didn't respond for this shard
	if !kv.serveringShards[shard] {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return 
	}

	kv.mu.Unlock()

	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	op := rsm.Op{
		Operation: "Put",
		Key: args.Key,
		Value: args.Value,
		ClientID: args.ClientID,
		SeqNum: args.SeqNum,
		Version: args.Version,
		ShardID: shard,
		ConfigNum: kv.shardConfigNums[shard]
	}

	DPrintf("S%d received %v", kv.me, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(Result)
	reply.Err = res.Err
}

func copyMap(m map[string]DBValue) map[string]DBValue {
	res := make(map[string]DBValue)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyLastOpResult(m map[int64]Result) map[int64]Result {
	res := make(map[int64]Result)
	for clientID, commandResult := range m {
		res[clientID] = commandResult
	}
	return res
}

func copyLastAppliedSeq(m map[int64]int) map[int64]int{
	res := make(map[int64]int)
	for clientID, seq := range m {
		res[clientID] = seq
	}
	return
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()

	// reject old config version num
	// If we are ALREADY at a higher version, we might have already deleted the data!
	if configNumForShard <= kv.shardConfigNums[shardID] {
		// if the data is gone, we can't request this old request
		if kv.shardsData[shardID] == nil {
			reply.Err = rpc.ErrNoKey
			kv.mu.Unlock()
			return
		}
		// if we still have the data, return it
		// let executeMoves to check the installShard is done or not
		reply.Data = copyMap(kv.shardsData[shardID])
		reply.LastOpResult = copyLastOpResult(kv.lastOpResult[shardID])
		reply.lastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shardID])
		reply.Err = rpc.OK
		reply.Num = kv.shardConfigNums[shardID]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "FreezeShard",
		Key: args.Key,
		Value: args.Value,
		ShardID: shardID,
		ConfigNum: configNumForShard
		Data: copyMap(kv.shardsData[shardID])
	}

	DPrintf("S%d received freezeShard %v", kv.me, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(Result)
	reply.Err = res.Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	// You may need initialization code here.
	// 1. Initialize the maps first (so Restore has something to fill)
	// iterate through all shard to initialize
	for i := 0; i < shardcfg.NShards; i++ {
		// important, must initialize kv data map for each shard
		kv.shardsData[i] = make(map[string]DBValue)
		kv.lastOpResult[i] = make(map[int64]Result)
		kv.lastAppliedSeq[i] = make(map[int64]Result)

		// in default, the biggest config version num for all shard is 0
		kv.shardConfigNums[i] = 0

		// in default, the group don't have any shard
		kv.serveringShards[i] = false
	}

	// 3.5  APPLY THE HINT HERE:
    // Check if this server belongs to the very first group (Gid1)
	if kv.gid == shardcfg.Gid1 {
		// If we are Group 1, we start by owning EVERYTHING.
		for i := 0; i < shardcfg.NShards; i++ {
			kv.serveringShards[i] = true
		}
	} 

	// 2. Read the existing snapshot from the persister
	snapshotData := persister.ReadSnapshot()

	// 3. Restore the state if a snapshot exists
    // (This before RSM starts, make sure the KV maps are ready)
	kv.Restore(snapshotData)

	// 4. Start the RSM/Raft
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)


	return []tester.IService{kv, kv.rsm.Raft()}
}
