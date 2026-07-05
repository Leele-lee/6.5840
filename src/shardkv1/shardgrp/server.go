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
	// WaitInstall (1)：新配置说这块地归我，但我还没收到数据，禁止读写。
	WaitInstall
	// Active (2)：我拥有数据且配置正确，允许读写。
	Active
	// Frozen (3)：我正在把数据给别人，数据已锁定，禁止写入，允许读取（仅限迁移读取）
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

func (kv *KVServer) advanceServerConfig(newNum shardcfg.Tnum, newConfig shardcfg.ShardConfig) {
	kv.currConfigNum = newNum
	kv.config = newConfig

	for i := 0; i < shardcfg.NShards; i++ {
		// update all shards configNum in this group server
		kv.shardConfigNums[i] = newNum

		if newConfig.Shards[i] == kv.gid {
			// If the new configuration belongs to me, 
			// and it was already under my control before -> Keep Active

			// if not belong to me before, set to waitInstall
			if kv.shardStatus[i] != Active {
				kv.shardStatus[i] = WaitInstall
			}
		} else {
			// 在新版里不属于我了。
			// 如果我正在持有它，设为 Frozen 准备给别人。
			// 如果我根本没持有过，设为 NotOwned
			if kv.shardStatus[i] == Active {
				kv.shardStatus[i] = Frozen
			} else {
				kv.shardStatus[i] = NotOwned
			}
		}
	}
}

// deal with op from group rpc, freezeShard, installShard and deleteShard
func (kv *KVServer) applyAdminOp(op rsm.Op) shardrpc.Result {
	var res shardrpc.Result
	shard := op.ShardID
	// 如果 op.Num 比我们已知的还小，说明这是来自过去的延迟包，必须丢弃。
	if op.ConfigNum < kv.shardConfigNums[shard] {
		DPrintf("SERVER %d: Rejecting stale AdminOp %s for Shard %d (Op.Num %d < Current %d)", 
            kv.me, op.Operation, shard, op.ConfigNum, kv.shardConfigNums[shard])
		res.Err = rpc.ErrWrongGroup
		return res
	}

    // --- 2. 全员步进逻辑 (解决分片 7 卡死) ---
    // 如果这个操作的版本号领先于服务器的全局版本，触发一次全员状态对齐
	if op.ConfigNum > kv.currConfigNum {
		kv.advanceServerConfig(op.ConfigNum, op.Config)
	}

	// --- 3. 记录最大 Num (满足手册要求) ---
    // 此时 op.Num >= kv.shardConfigNums[shard]
	// actually already did in advanServerConfig, so can delete
	kv.shardConfigNums[shard] = op.ConfigNum

	switch op.Operation {
	case "FreezeShard" :
		// newconfig already not have this shard
		// 如果已经 Frozen 了，说明是重复请求，直接返回之前存好的数据
		if kv.shardStatus[shard] == Frozen {
			res.Err = rpc.OK
			res.Data = copyMap(kv.shardsData[shard])
			res.LastOpResult = copyLastOpResult(kv.lastOpResult[shard])
			res.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shard])
			res.Num = kv.shardConfigNums[shard]
		} else {
			kv.shardStatus[shard] = Frozen
			res.Err = rpc.OK
			res.Data = copyMap(kv.shardsData[shard])
			res.LastOpResult = copyLastOpResult(kv.lastOpResult[shard])
			res.LastAppliedSeq = copyLastAppliedSeq(kv.lastAppliedSeq[shard])
			res.Num = kv.shardConfigNums[shard]
		}
	case "InstallShard" :
		// newconfig must have this shard now, so it can only active and waitInstall status
		// through advanServerConfig
		// notice after update status in advanServerConfig, in this step 
		// the status of shardID can only be Active or waitInstall
		// 只有当我们在等待数据时才安装
		if kv.shardStatus[shard] == WaitInstall {
			kv.shardsData[shard] = copyMap(op.Data)
			kv.lastAppliedSeq[shard] = copyLastAppliedSeq(op.LastAppliedSeq)
			kv.lastOpResult[shard] = copyLastOpResult(op.LastOpResult)
			kv.shardStatus[shard] = Active
			res.Err = rpc.OK
		} else if kv.shardStatus[shard] == Active {
			// 如果已经是 Active，说明是重复请求，直接返回 OK 即可
			res.Err = rpc.OK
		} else {
			DPrintf("SERVER %d: Something wrong in shardStatus[%d] = %d. InstallShard in applyAdminOp. AdminOp %s.)", 
            	kv.me, shard, kv.shardStatus[shard], op.Operation)
		}
	case "DeleteShard" :
		// newconfig must not have this shard so it can have other status
		if kv.shardStatus[shard] != NotOwned {
			kv.shardsData[shard] = nil
			kv.lastAppliedSeq[shard] = nil
			kv.lastOpResult[shard] = nil
			//DPrintf("SERVER %d GID %d: DeleteShard success for Shard %d, NewConfigNum %d. (Current Config for this shard was %d)", 
			//kv.me, kv.gid, shard, op.ConfigNum, kv.shardConfigNums[shard])
			kv.shardStatus[shard] = NotOwned
			res.Err = rpc.OK
		} else {
			// if already delete, this is a repeate request, direct return rpc.OK
			res.Err = rpc.OK
		}
	}
	return res
}

// deal with op from client, put/get 
func (kv *KVServer) applyClientOp(op rsm.Op) shardrpc.Result {
	shard := op.ShardID
	if op.ConfigNum != kv.shardConfigNums[shard] || kv.shardStatus[shard] != Active {
		return shardrpc.Result{Err: rpc.ErrWrongGroup}
	}

	// then check has duplicate put operations or not
	if op.Operation != "Get" && kv.lastAppliedSeq[shard][op.ClientID] >= op.SeqNum {
		return kv.lastOpResult[shard][op.ClientID]
	}

	DPrintf("DoOP: %s (key: %s)configNum check okay! server configNum: %d, op num: %d", 
		op.Operation, op.Key, kv.shardConfigNums[shard], op.ConfigNum)
	return kv.executeOp(op)
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
	case "FreezeShard", "IntallShard", "DeleteShard" :
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
		//kv.serveringShards = ps.ServeringShards
		kv.shardConfigNums = ps.ShardConfigNums
		kv.shardsData = ps.ShardsData
		kv.lastOpResult = ps.LastOpResult
		kv.lastAppliedSeq = ps.LastAppliedSeq
		kv.config = ps.Config
		kv.shardStatus = ps.ShardStatus
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	
	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)
	DPrintf("DEBUG: Key %s belongs to Shard %d\n", args.Key, shard)

	//DPrintf("S%d received get(key: %s) clientID: %d, seqNum: %d, Op configNum: %d, local configNum: %d for shard %d",
	// kv.me, args.Key, args.ClientID, args.SeqNum, args.ConfigNum, kv.shardConfigNums[shard], shard)
	kv.mu.Lock()
	if args.ConfigNum != kv.shardConfigNums[shard] || kv.shardStatus[shard] != Active {
		reply.Err = rpc.ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := rsm.Op{
		Operation: "Get", 
		Key: args.Key, 
		ClientID: args.ClientID,
		SeqNum: args.SeqNum,
		ShardID: shard,
		ConfigNum: args.ConfigNum,
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

	//DPrintf("S%d after get(key: %s) clientID: %d, seqNum: %d for shard %d, op configNum: %d local ocnfigNum: %d, get reply (val: %s, ver: %d, err: %s)",
	// kv.me, args.Key, args.ClientID, args.SeqNum, shard, args.ConfigNum, kv.shardConfigNums[shard], reply.Value, reply.Version, reply.Err)

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here	

	// find which shard has this key
	shard := shardcfg.Key2Shard(args.Key)

	//DPrintf("S%d received put(key: %s, value: %s, version: %d) clientID: %d, seqNum: %d, configNum: %d for shard %d",
	// kv.me, args.Key, args.Value, args.Version, args.ClientID, args.SeqNum, args.ConfigNum, shard)

	//DPrintf("S%d received put %s for shard %d, op configNum: %d, local configNum: %d", kv.me, args, shard, args.ConfigNum, kv.shardConfigNums[shard])

	kv.mu.Lock()
	// this shardgrp didn't respond for this shard
	if args.ConfigNum != kv.shardConfigNums[shard] || kv.shardStatus[shard] != Active {
		reply.Err = rpc.ErrWrongGroup
		DPrintf("S%d received put %s for shard %d, op configNum: %d, local configNum: %d, but return ErrWrongGroup ", kv.me, args, shard, args.ConfigNum, kv.shardConfigNums[shard])

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


	op := rsm.Op{
		Operation: "FreezeShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
		Config: copyConfig(args.Config),
	}

	DPrintf("S%d received freezeShard for shard %d, %v", kv.me, shardID, op)

	err, result := kv.rsm.Submit(op)
	if err != rpc.OK {
		reply.Err = err
		return
	}


	DPrintf("S%d after received freezeShard for shard %d, %v", kv.me, shardID, reply)

	res := result.(shardrpc.Result)
	// COPY THE DATA FROM THE RESULT TO THE REPLY
    reply.Data = res.Data
    reply.LastAppliedSeq = res.LastAppliedSeq
    reply.LastOpResult = res.LastOpResult
    reply.Err = res.Err
	reply.Num = res.Num
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	shardID := args.Shard
	configNumForShard := args.Num



	op := rsm.Op{
		Operation: "InstallShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
		Data: copyMap(args.Data),
		LastAppliedSeq: copyLastAppliedSeq(args.LastAppliedSeq),
		LastOpResult: copyLastOpResult(args.LastOpResult),
		Config: copyConfig(args.Config),
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



	op := rsm.Op{
		Operation: "DeleteShard",
		ShardID: shardID,
		ConfigNum: configNumForShard,
		Config: args.Config,
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
		kv.shardConfigNums[i] = 0
		kv.shardStatus[i] = NotOwned

		// if group server is the first group gid1, not recover from snapshot
		if kv.gid == shardcfg.Gid1 && persister.SnapshotSize() == 0{
			DPrintf("SERVER %d: Initializing GID1 as owner of all shards for Config 1", kv.me)
			for i := 0; i < shardcfg.NShards; i++ {
				kv.shardStatus[i] = Active
				kv.shardConfigNums[i] = 1
			}
			kv.currConfigNum = 1
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
