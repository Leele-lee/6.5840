package shardgrp

import (
	"sync/atomic"
	"sync"
	"fmt"
	"bytes"
	//"log"
	"time"


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
	//serveringShards [shardcfg.NShards]bool

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
	// Each shard has its own independent ClientID -> put Result mapping.
	lastOpResult [shardcfg.NShards]map[int64]shardrpc.Result
	// each shard has its own independent clientID -> last applied put seq num
	lastAppliedSeq [shardcfg.NShards]map[int64]int
	// To avoid raft timeout and repeate send to DoOp
	// shardID -> configNum snd type
	//pendingMigration map[shardcfg.Tshid]MigrationTask
	currConfigNum shardcfg.Tnum
	shardStatus [shardcfg.NShards]status
	config shardcfg.ShardConfig
}

type KVPersistState struct {
	//ServeringShards [shardcfg.NShards]bool
	ShardConfigNums [shardcfg.NShards]shardcfg.Tnum
	ShardsData [shardcfg.NShards]map[string]shardrpc.DBValue
	LastOpResult [shardcfg.NShards]map[int64]shardrpc.Result
	LastAppliedSeq [shardcfg.NShards]map[int64]int
	Config shardcfg.ShardConfig
	ShardStatus [shardcfg.NShards]status
	CurrConfigNum shardcfg.Tnum
}

type MigrationTask struct {
	Num shardcfg.Tnum
	OpType string // freeze, install, delete
}


type status int

// 定义具体的状态常量（利用 iota 从 0 开始自动递增）
const (
	// NotOwned (0)：我不拥有这个分片，也不打算拥有它。
	NotOwned status = iota
	// Active (1)：我拥有数据且配置正确，允许读写。
	Active
	// Frozen (2)：我正在把数据给别人，数据已锁定，禁止写入，允许读取（仅限迁移读取）
	Frozen
)

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
		if !ok && op.Version != 0 {
			res.Err = rpc.ErrNoKey
			return res
		}
		// If versions don't match, return ErrVersion.
		if ok && op.Version != verVal.Version {
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

func (kv *KVServer) totalKeyCount() int {
    count := 0
    for i := 0; i < shardcfg.NShards; i++ {
        if kv.shardsData[i] != nil {
            count += len(kv.shardsData[i])
        }
    }
    return count
}

func (kv *KVServer) debugSpace() {
    for i := 0; i < shardcfg.NShards; i++ {
        if len(kv.shardsData[i]) > 0 {
            DPrintf("SERVER %d: Shard %d has %d keys", kv.me, i, len(kv.shardsData[i]))
        }
    }
}

// deal with op from group rpc, freezeShard, installShard and deleteShard
func (kv *KVServer) applyAdminOp(op rsm.Op) shardrpc.Result {
	var res shardrpc.Result
	shard := op.ShardID
	localNum := kv.shardConfigNums[shard]
	status := kv.shardStatus[shard]

	// 如果 op.Num 比我们已知的还小，说明这是来自过去的延迟包，必须丢弃。
	if op.ConfigNum < localNum {
		DPrintf("SERVER %d: Rejecting stale AdminOp %s for Shard %d (Op.Num %d < Current %d)", 
            kv.me, op.Operation, shard, op.ConfigNum, kv.shardConfigNums[shard])
		res.Err = rpc.ErrStale
		return res
	}

	switch op.Operation {
	case "FreezeShard":
		switch {
		case op.ConfigNum == localNum:
			switch status {
			case Frozen:
				// repeat rpc, may lost reply just return all data
				res.Data = copyMap(kv.shardsData[shard])
				res.LastOpResult = copyLastOpResult(kv.lastOpResult[shard])
				res.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shard])
				res.Num = kv.shardConfigNums[shard]
				res.Err = rpc.OK
			case NotOwned:
				// already delete, return ErrAlreadyDone
				// controller should move to the next shard
				res.Err = rpc.ErrAlreadyDone
			default:
				res.Err = rpc.ErrWrongGroup
			}

		case op.ConfigNum > localNum:
			if status != Active {
				res.Err = rpc.ErrWrongGroup
				break
			}

			res.Data = copyMap(kv.shardsData[shard])
			res.LastOpResult = copyLastOpResult(kv.lastOpResult[shard])
			res.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shard])
			res.Num = kv.shardConfigNums[shard]

			kv.shardStatus[shard] = Frozen
			kv.shardConfigNums[shard] = op.ConfigNum
			res.Err = rpc.OK
		}
	case "InstallShard":
		switch {
		case op.ConfigNum == localNum:
			if status == Active {
				// repeate install, may lost reply
				// reply don't have important msg
				res.Err = rpc.OK
			} else {
				// not normal!
				res.Err = rpc.ErrWrongGroup
			}
		case op.ConfigNum > localNum:
			if status != NotOwned {
				// not normal
				res.Err = rpc.ErrWrongGroup
				break
			}
			kv.shardsData[shard] = copyMap(op.Data)
			kv.lastAppliedSeq[shard] = copyLastAppliedSeq(op.LastAppliedSeq)
			kv.lastOpResult[shard] = copyLastOpResult(op.LastOpResult)

			kv.shardStatus[shard] = Active
			kv.shardConfigNums[shard] = op.ConfigNum
			res.Err = rpc.OK
		}
	case "DeleteShard":
		switch {
		case op.ConfigNum == localNum:
			switch {
			case status == Frozen:
				kv.shardsData[shard] = nil
				kv.lastAppliedSeq[shard] = nil
				kv.lastOpResult[shard] = nil
				kv.shardStatus[shard] = NotOwned
				res.Err = rpc.OK
			case status == Active:
				// not normal
				res.Err = rpc.ErrWrongGroup
			default:
				// status == NotOwned
				// repeat rpc, already delete
				res.Err = rpc.OK
			}
		case op.ConfigNum > localNum:
			res.Err = rpc.ErrWrongGroup
		}
		//DPrintf("SERVER %d group: %d: After Delete Shard %d, DB size is %d", kv.me, kv.gid, op.ShardID, kv.totalKeyCount())
		//kv.debugSpace()
	}
	return res
}

// deal with op from client, put/get 
func (kv *KVServer) applyClientOp(op rsm.Op) shardrpc.Result {
	shard := op.ShardID
	res := shardrpc.Result{}
	status := kv.shardStatus[shard]

	switch status {
	case NotOwned:
		DPrintf("S%d applying op: %v for shard %d, status: %d, op configNum: %d, local configNum: %d, bc status is NotOwned return ErrWrongGroup ",
		 kv.me, op, shard, status, op.ConfigNum, kv.shardConfigNums[shard])
		res.Err = rpc.ErrWrongGroup
	case Frozen:
		res.Err = rpc.ErrRetry
	case Active:
		if op.Operation != "Get" && kv.lastAppliedSeq[shard][op.ClientID] >= op.SeqNum {
			DPrintf("S%d applying op: %v for shard %d, status: %d, op configNum: %d, local configNum: %d, status is alive and return %v ",
		 	 kv.me, op, shard, status, op.ConfigNum, kv.shardConfigNums[shard], kv.lastOpResult[shard][op.ClientID])

			return kv.lastOpResult[shard][op.ClientID]
		}
		res = kv.executeOp(op)
	}
	return res
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	// argument should be Op struct in order to consistent with
	// submit function in rsm.go
	op, ok := req.(rsm.Op)
	var res shardrpc.Result
	//shard := op.ShardID

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 1. type assertion, if not ok return
	if !ok {
		fmt.Println("type assertion failed")
	}

	switch op.Operation {
	case "Put", "Get" :
		res = kv.applyClientOp(op)
	case "FreezeShard", "InstallShard", "DeleteShard" :
		res = kv.applyAdminOp(op)
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

	//ps.ServeringShards = kv.serveringShards
	ps.ShardConfigNums = kv.shardConfigNums
	ps.ShardsData = kv.shardsData
	ps.LastOpResult = kv.lastOpResult
	ps.LastAppliedSeq = kv.lastAppliedSeq
	ps.Config = kv.config
	ps.ShardStatus = kv.shardStatus
	ps.CurrConfigNum = kv.currConfigNum

	e.Encode(ps)

	DPrintf("SERVER %d Snapshotting: ConfigNum Array is %v", kv.me, kv.currConfigNum)
	
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
		//kv.serveringShards = ps.ServeringShards
		kv.shardConfigNums = ps.ShardConfigNums
		kv.shardsData = ps.ShardsData
		kv.lastOpResult = ps.LastOpResult
		kv.lastAppliedSeq = ps.LastAppliedSeq
		kv.config = ps.Config
		kv.shardStatus = ps.ShardStatus
		kv.currConfigNum = ps.CurrConfigNum
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	
	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)
	//DPrintf("DEBUG: Key %s belongs to Shard %d\n", args.Key, shard)
	//kv.mu.Lock()
	//DPrintf("S%d group: %d, received get(key: %s) clientID: %d, seqNum: %d, Op configNum: %d, local configNum: %d for shard %d, status: %d",
	 //kv.me, kv.gid, args.Key, args.ClientID, args.SeqNum, args.ConfigNum, kv.shardConfigNums[shard], shard, kv.shardStatus[shard])

	//if args.ConfigNum != kv.shardConfigNums[shard] || kv.shardStatus[shard] != Active {
	//	DPrintf("S%d group: %d, get rpc handler reject, op configNum: %d, local configNum: %d, shard status: %d", 
	//		kv.me, kv.gid, args.ConfigNum, kv.shardConfigNums[shard], kv.shardStatus[shard])
	//	reply.Err = rpc.ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()

	op := rsm.Op{
		Operation: "Get", 
		Key: args.Key, 
		ClientID: args.ClientID,
		SeqNum: args.SeqNum,
		ShardID: shard,
		ConfigNum: args.ConfigNum,
	}

	term1, leader1 := kv.rsm.DebugRaftState()
	start := time.Now()

	err, result := kv.rsm.Submit(op)

	term2, leader2 := kv.rsm.DebugRaftState()


    DPrintf(
        "GID=%d S=%d Get(%s) Submit done: "+
            "before(term=%d leader=%v) "+
            "after(term=%d leader=%v) "+
            "err=%v elapsed=%v",
        kv.gid,
        kv.me,
        args.Key,
        term1,
        leader1,
        term2,
        leader2,
        err,
        time.Since(start),
    )

	// if server is not leader
    if err != rpc.OK {
        reply.Err = err
        return
    }

	res := result.(shardrpc.Result)
    reply.Err = res.Err
    reply.Value = res.Value
	reply.Version = res.Version

	//DPrintf("S%d group: %d after get(key: %s) clientID: %d, seqNum: %d for shard %d, op configNum: %d local configNum: %d, status: %d, get reply (val: %s, ver: %d, err: %s)",
	// kv.me, kv.gid, args.Key, args.ClientID, args.SeqNum, shard, args.ConfigNum, kv.shardConfigNums[shard], kv.shardStatus[shard], reply.Value, reply.Version, reply.Err)

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here	

	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)

	//DPrintf("S%d received put(key: %s, value: %s, version: %d) clientID: %d, seqNum: %d, configNum: %d for shard %d",
	// kv.me, args.Key, args.Value, args.Version, args.ClientID, args.SeqNum, args.ConfigNum, shard)

	//DPrintf("S%d received put %s for shard %d, op configNum: %d, local configNum: %d", kv.me, args, shard, args.ConfigNum, kv.shardConfigNums[shard])

	//kv.mu.Lock()
	// this shardgrp didn't respond for this shard
	//if kv.shardStatus[shard] != Active {
	//	reply.Err = rpc.ErrWrongGroup
	//	DPrintf("S%d received put %s for shard %d, status: %d, op configNum: %d, local configNum: %d, but return ErrWrongGroup ", kv.me, args, shard, kv.shardStatus[shard], args.ConfigNum, kv.shardConfigNums[shard])

	//	kv.mu.Unlock()
	//	return
	//}

	//kv.mu.Unlock()

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
		ConfigNum: args.ConfigNum,
	}

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	//DPrintf("S%d after put(key: %s, value: %s, version: %d) clientID: %d, seqNum: %d for shard %d, the kv.lastAppliedSeq: %d, kv.lastOpResult's (val: %s, err: %s, ver: %d)", 
	//kv.me, args.Key, args.Value, args.Version, args.ClientID, args.SeqNum, shard, kv.lastAppliedSeq[shard][op.ClientID], kv.lastOpResult[shard][op.ClientID].Value, kv.lastOpResult[shard][op.ClientID].Err, kv.lastOpResult[shard][op.ClientID].Version)

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

func copyConfig(conf shardcfg.ShardConfig) shardcfg.ShardConfig {
	var newConf shardcfg.ShardConfig

	newConf.Num = conf.Num
	newConf.Shards = conf.Shards

	newConf.Groups = make(map[tester.Tgid][]string)
	for gid, servers := range conf.Groups {
		serversCopy := make([]string, len(servers))
		copy(serversCopy, servers)

		newConf.Groups[gid] = serversCopy
	}
	return newConf
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()
	if configNumForShard < kv.shardConfigNums[shardID] {
		reply.Err = rpc.ErrStale
		DPrintf("S%d group %d rejected FreezeShard for shard %d, op num < local num (%d < %d)", 
		 kv.me, kv.gid, shardID, args.Num, kv.shardConfigNums[shardID] )
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "FreezeShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
	}

	DPrintf("S%d received freezeShard for shard %d, %v", kv.me, shardID, op)
	//DPrintf("S%d group: %d received FreezeShard for shard %d, op num is: %d, server num is: %d, shardStatus: %d", 
		//kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], kv.shardStatus[shardID])


	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}

	res := result.(shardrpc.Result)
	// COPY THE DATA FROM THE RESULT TO THE REPLY
    reply.Data = res.Data
    reply.LastAppliedSeq = res.LastAppliedSeq
    reply.LastOpResult = res.LastOpResult
    reply.Err = res.Err
	reply.Num = res.Num

	DPrintf("S%d group: %d after received freezeShard for shard %d, op num: %d, server num: %d, reply: %v", 
		kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], reply)

}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()
	if configNumForShard < kv.shardConfigNums[shardID] {
		reply.Err = rpc.ErrStale
		DPrintf("S%d group %d rejected FreezeShard for shard %d, op num < local num (%d < %d)", 
		 kv.me, kv.gid, shardID, args.Num, kv.shardConfigNums[shardID])
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
	//DPrintf("S%d group: %d received Install for shard %d, op num is: %d, server num is: %d, shardStatus: %d", 
		//kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], kv.shardStatus[shardID])


	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(shardrpc.Result)
	reply.Err = res.Err

	//DPrintf("S%d group: %d after received installShard for shard %d, op num: %d, server num: %d, Err: %s, reply: %v", 
	//	kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], res.Err, reply)

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	shardID := args.Shard
	configNumForShard := args.Num

	kv.mu.Lock()
	if configNumForShard < kv.shardConfigNums[shardID] {
		reply.Err = rpc.ErrStale
		DPrintf("S%d group %d rejected FreezeShard for shard %d, op num < local num (%d < %d)", 
		 kv.me, kv.gid, shardID, args.Num, kv.shardConfigNums[shardID])
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "DeleteShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
	}

	DPrintf("S%d received DeleteShard for shard %d, %v", kv.me, shardID, op)	
	//DPrintf("S%d group: %d received DeleteShard for shard %d, op num is: %d, server num is: %d, shardStatus: %d", 
		//kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], kv.shardStatus[shardID])


	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res := result.(shardrpc.Result)
	reply.Err = res.Err

	DPrintf("S%d group: %d after received deleteShard for shard %d, op num: %d, server num: %d, reply: %v", 
		kv.me, kv.gid, shardID, op.ConfigNum, kv.shardConfigNums[shardID], reply)

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
	kv.rsm.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutinStartServerShardGrpes for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(shardcfg.ShardConfig{})

	//labgob.Register(rsm.RSMOp{})

	kv := &KVServer{gid: gid, me: me}
	//kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	DPrintf("S%d group: %d restarted", kv.me, kv.gid)
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
		kv.shardConfigNums[i] = 0
		kv.shardStatus[i] = NotOwned
	}
	kv.currConfigNum = 0

	// if group server is the first group gid1, not recover from snapshot
	//if kv.gid == shardcfg.Gid1 && persister.SnapshotSize() == 0{
	//	DPrintf("SERVER %d: Initializing GID1 as owner of all shards for Config 1", kv.me)
	///	for i := 0; i < shardcfg.NShards; i++ {
		//	kv.shardStatus[i] = Active
		//	kv.shardConfigNums[i] = 1
		//}
		//kv.currConfigNum = 1
	//}

	// 2. Read the existing snapshot from the persister
	snapshotData := persister.ReadSnapshot()

	// 3. Restore the state if a snapshot exists
    // (This before RSM starts, make sure the KV maps are ready)
	kv.Restore(snapshotData)

	// 关键条件：只有当我没从快照恢复任何数据（SnapshotSize == 0），
    // 且我是 GID 1 时，才手动初始化为版本 1。
    if kv.gid == shardcfg.Gid1 && persister.SnapshotSize() == 0 {
        DPrintf("SERVER %d: Cold Start GID1 to Version 1", kv.me)
        for i := 0; i < shardcfg.NShards; i++ {
            kv.shardConfigNums[i] = 1
            kv.shardStatus[i] = Active
        }
        kv.currConfigNum = 1
    }

	// 4. Start the RSM/Raft
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)


	return []tester.IService{kv, kv.rsm.Raft()}
}
