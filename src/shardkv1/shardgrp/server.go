package shardgrp

import (
	"sync/atomic"
	"sync"
	"fmt"
	"bytes"
	//"log"


	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)


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
	shardsData [shardcfg.NShards]map[string]shardrpc.DBValue

	// Store deduplication information for each shard! 
	// Each shard has its own independent ClientID -> Result mapping.
	lastOpResult [shardcfg.NShards]map[int64]shardrpc.Result
	// each shard has its own last applied put seq num
	lastAppliedSeq [shardcfg.NShards]map[int64]int
	// To avoid raft timeout and repeate send to DoOp
	// shardID -> configNum snd type
	//pendingMigration map[shardcfg.Tshid]MigrationTask
}

type KVPersistState struct {
	ServeringShards [shardcfg.NShards]bool
	ShardConfigNums [shardcfg.NShards]shardcfg.Tnum
	ShardsData [shardcfg.NShards]map[string]shardrpc.DBValue
	LastOpResult [shardcfg.NShards]map[int64]shardrpc.Result
	LastAppliedSeq [shardcfg.NShards]map[int64]int
}

type MigrationTask struct {
	Num shardcfg.Tnum
	OpType string // freeze, install, delete
}

func (kv *KVServer) executeOp(op rsm.Op) shardrpc.Result {
	shard := op.ShardID
	var res shardrpc.Result

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

		//DPrintf("SERVER %d GID %d: execute Get for key %s (Shard %d). Op.Num is %d, Current ConfigNum for shard: is %d", 
        //    kv.me, kv.gid, op.Key, shard, op.ConfigNum, kv.shardConfigNums[shard])
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
		kv.shardsData[shard][op.Key] = shardrpc.DBValue{Value: op.Value, Version: op.Version + 1}
		res.Err = rpc.OK
		res.Value = op.Value
		res.Version = op.Version + 1
		// record the result to kv
		kv.lastAppliedSeq[shard][op.ClientID] = op.SeqNum
		kv.lastOpResult[shard][op.ClientID] = res
	}
	//DPrintf("SERVER %d GID %d: execute Put for key %s (Shard %d). Op.Num is %d, Current ConfigNum for shard: is %d", 
    //    kv.me, kv.gid, op.Key, shard, op.ConfigNum, kv.shardConfigNums[shard])

	return res
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// argument should be Op struct in order to consistent with
	// submit function in rsm.go
	op, ok := req.(rsm.Op)
	var res shardrpc.Result
	shard := op.ShardID

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. type assertion, if not ok return
	if !ok {
		fmt.Println("type assertion failed")
	}

	switch op.Operation {
	case "Put", "Get":

		// 1. If the server is BEHIND the client's config, it's not "wrong," it's just "slow."
    	//    Return a retry error (like ErrNoKey or a custom ErrRetry) or just return ErrWrongGroup 
    	//    ONLY if the server's version is >= the client's version.
    	//if kv.shardConfigNums[shard] < op.ConfigNum {
			// Your server is lagging. Return ErrRetry. 
			// (The clerk will wait and try again, giving Raft time to catch up).
        //	res.Err = rpc.ErrRetry // Tells the clerk to retry the SAME group
        //	return res
    	//}

		if kv.shardConfigNums[shard] != op.ConfigNum {
			// The client is stale. Return ErrWrongGroup. 
			// (The clerk will then call Query to get the new config)
			res.Err = rpc.ErrWrongGroup
			return res
		}

		// first check to make sure:
		// 1. the group still contains this shard
		// 2. and the config Num is match
		if !kv.serveringShards[shard] {
			DPrintf("SERVER %d GID %d: REJECTING %s for key %s (Shard %d). Current ConfigNum for shard: %d is %d. servingShards is FALSE", 
            	kv.me, kv.gid, op.Operation, op.Key, shard, kv.shardConfigNums[shard], kv.shardConfigNums[shard])
			res.Err = rpc.ErrWrongGroup
			return res
		}

		// then check has duplicate put operations or not
		if op.Operation != "Get" && kv.lastAppliedSeq[shard][op.ClientID] >= op.SeqNum {
			return kv.lastOpResult[shard][op.ClientID]
		}
		// truly execute the operation
		res = kv.executeOp(op)

	case "FreezeShard":
		// first check Num, freeze only when request config num >= current config in grp
		DPrintf("SERVER %d GID %d: APPLYING FreezeShard for Shard %d, NewConfigNum %d. (Current Config for this shard was %d)", 
        	kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])

		if op.ConfigNum >= kv.shardConfigNums[shard] {
			kv.serveringShards[shard] = false
			kv.shardConfigNums[shard] = op.ConfigNum
			res.Err = rpc.OK
		} else {
			res.Err = rpc.OK
		}
	case "InstallShard":
		// install only when request num greater than current config, in case of duplicate install
		// only install when request num strictly greater than current num
		DPrintf("SERVER %d GID %d: APPLYING InstallShard for Shard %d, NewConfigNum %d. (Current Config for this shard was %d)", 
        	kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])

		if op.ConfigNum > kv.shardConfigNums[shard] {
			kv.shardsData[shard] = copyMap(op.Data)
			kv.lastAppliedSeq[shard] = copyLastAppliedSeq(op.LastAppliedSeq)
			kv.lastOpResult[shard] = copyLastOpResult(op.LastOpResult)
			kv.shardConfigNums[shard] = op.ConfigNum
			kv.serveringShards[shard] = true

			DPrintf("SERVER %d GID %d: Shard %d is now ACTIVE", kv.me, kv.gid, shard)
			res.Err = rpc.OK
		} else {
			DPrintf("SERVER %d GID %d: REJECTED InstallShard for Shard %d (Stale Num: %d <= %d)", 
            	kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])
			res.Err = rpc.OK
		}

	case "DeleteShard":
		DPrintf("SERVER %d GID %d: APPLYING DeleteShard for Shard %d, NewConfigNum %d. (Current Config for this shard was %d)", 
        	kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])

		if op.ConfigNum >= kv.shardConfigNums[shard] {
			kv.serveringShards[shard] = false
			kv.shardsData[shard] = nil
			kv.lastAppliedSeq[shard] = nil
			kv.lastOpResult[shard] = nil
			kv.shardConfigNums[shard] = op.ConfigNum
			DPrintf("SERVER %d GID %d: DeleteShard success for Shard %d, NewConfigNum %d. (Current Config for this shard was %d)", 
			kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])
			res.Err = rpc.OK
		} else {
			DPrintf("SERVER %d GID %d: REJECTED DeleteShard for Shard %d (Stale Num: %d <= %d)", 
				kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])
			res.Err = rpc.OK
		}
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

	var ps KVPersistState
	//ps.Db = kv.db

	ps.ServeringShards = kv.serveringShards
	ps.ShardConfigNums = kv.shardConfigNums
	ps.ShardsData = kv.shardsData
	ps.LastOpResult = kv.lastOpResult
	ps.LastAppliedSeq = kv.lastAppliedSeq

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

	var ps KVPersistState

	if d.Decode(&ps) != nil {
		panic("resetore Persist: failed to decode server state")
	} else {
		kv.serveringShards = ps.ServeringShards
		kv.shardConfigNums = ps.ShardConfigNums
		kv.shardsData = ps.ShardsData
		kv.lastOpResult = ps.LastOpResult
		kv.lastAppliedSeq = ps.LastAppliedSeq
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	
	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)
	//fmt.Printf("DEBUG: Key %s belongs to Shard %d\n", args.Key, shard)

	DPrintf("S%d received get(key: %s) clientID: %d, seqNum: %d for shard %d",
	 kv.me, args.Key, args.ClientID, args.SeqNum, shard)

	op := rsm.Op{
		Operation: "Get", 
		Key: args.Key, 
		ClientID: args.ClientID,
		SeqNum: args.SeqNum,
		ShardID: shard,
		ConfigNum: kv.shardConfigNums[shard],
	}

	err, result := kv.rsm.Submit(op)

	// if server is not leader
    if err != rpc.OK {
        reply.Err = err
        return
    }

	res := result.(shardrpc.Result)
    reply.Err = res.Err
    reply.Value = res.Value
	reply.Version = res.Version

	DPrintf("S%d after get(key: %s) clientID: %d, seqNum: %d for shard %d, get reply (val: %s, ver: %d, err: %s)",
	 kv.me, args.Key, args.ClientID, args.SeqNum, shard, reply.Value, reply.Version, reply.Err)

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here	

	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)

	DPrintf("S%d received put(key: %s, value: %s, version: %d) clientID: %d, seqNum: %d for shard %d, configNum %d",
	 kv.me, args.Key, args.Value, args.Version, args.ClientID, args.SeqNum, shard, kv.shardConfigNums[shard])

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
		ConfigNum: kv.shardConfigNums[shard],
	}

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	DPrintf("S%d after put(key: %s, value: %s, version: %d) clientID: %d, seqNum: %d for shard %d, the kv.lastAppliedSeq: %d, kv.lastOpResult's (val: %s, err: %s, ver: %d)", 
	kv.me, args.Key, args.Value, args.Version, args.ClientID, args.SeqNum, shard, kv.lastAppliedSeq[shard][op.ClientID], kv.lastOpResult[shard][op.ClientID].Value, kv.lastOpResult[shard][op.ClientID].Err, kv.lastOpResult[shard][op.ClientID].Version)

	res := result.(shardrpc.Result)
	reply.Err = res.Err
}

func copyMap(m map[string]shardrpc.DBValue) map[string]shardrpc.DBValue {
	res := make(map[string]shardrpc.DBValue)
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyLastOpResult(m map[int64]shardrpc.Result) map[int64]shardrpc.Result {
	res := make(map[int64]shardrpc.Result)
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
	return res
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
	// if req.Num < kv.Num: Return OK. (It’s a duplicate from the past).
	// if req.Num == kv.Num: Return OK. (It’s a retry or another shard for the current config).
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
		reply.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shardID])
		reply.Err = rpc.OK
		reply.Num = kv.shardConfigNums[shardID]
		kv.mu.Unlock()
		return
	}

	// check the request is on raft or not, if yes retry later, don;t send to raft again
	//if pendingTask, ok := kv.pendingMigration[shardID]; ok && pendingTask.OpType == "Freeze" && pendingTask.Num >= args.Num {
	//	reply.Err = rpc.OK
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.pendingMigration[shardID] = MigrationTask{Num: args.Num, OpType: "Freeze"}

	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "FreezeShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
	}

	DPrintf("S%d received freezeShard for shard %d, %v", kv.me, shardID, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	reply.Data = copyMap(kv.shardsData[shardID])
	reply.LastOpResult = copyLastOpResult(kv.lastOpResult[shardID])
	reply.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shardID])
	reply.Num = kv.shardConfigNums[shardID]

	DPrintf("S%d after received freezeShard for shard %d, %v", kv.me, shardID, reply)

	res := result.(shardrpc.Result)
	reply.Err = res.Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()
	// reject old config version num
	// If we are ALREADY at a higher version, we might have already deleted the data!
	if configNumForShard <= kv.shardConfigNums[shardID] {
		reply.Err = rpc.OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "InstallShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
		Data: copyMap(args.Data),
		LastAppliedSeq: copyLastAppliedSeq(args.LastAppliedSeq),
		LastOpResult: copyLastOpResult(args.LastOpResult),
	}

	DPrintf("S%d received InstallShard for shard %d, %v", kv.me, shardID, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(shardrpc.Result)
	reply.Err = res.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()
	// reject old config version num
	// If we are ALREADY at a higher version, we might have already deleted the data!
	// if is equal must also submit, bc freezeShard already update configNum for the shard
	if configNumForShard < kv.shardConfigNums[shardID] {
		reply.Err = rpc.OK
		DPrintf("Reject old config version num for shard %d in DeleteShard rpc handler, request config num: %d, old num is: %d", shardID, configNumForShard,  kv.shardConfigNums[shardID])
		kv.mu.Unlock()
		return
	}

	// check the request is on raft or not, if yes retry later, don;t send to raft again
	//if pendingTask, ok := kv.pendingMigration[shardID]; ok && pendingTask.OpType == "Delete" && pendingTask.Num >= args.Num {
	//	reply.Err = rpc.OK
	//	kv.mu.Unlock()
	//	return
	//}
		
	//kv.pendingMigration[shardID] = MigrationTask{Num: args.Num, OpType: "Delete"}

	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "DeleteShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
	}

	DPrintf("S%d received DeleteShard for shard %d, %v", kv.me, shardID, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(shardrpc.Result)
	reply.Err = res.Err
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
	//kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	//kv.pendingMigration = make(map[shardcfg.Tshid]MigrationTask)

	// Your code here
	// You may need initialization code here.
	// 1. Initialize the maps first (so Restore has something to fill)
	// iterate through all shard to initialize
	for i := 0; i < shardcfg.NShards; i++ {
		// important, must initialize kv data map for each shard
		kv.shardsData[i] = make(map[string]shardrpc.DBValue)
		kv.lastOpResult[i] = make(map[int64]shardrpc.Result)
		kv.lastAppliedSeq[i] = make(map[int64]int)

		// in default, the biggest config version num for all shard is 0
		kv.shardConfigNums[i] = 1

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
