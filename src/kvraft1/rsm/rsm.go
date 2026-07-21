package rsm

import (
	"sync"
	"time"
	"fmt"
	"crypto/rand"
	"encoding/binary"
	//"log"

	"6.5840/labgob"
	"sync/atomic"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type OpID struct {
	Server int
	Epoch uint64
	ID uint64
}

type RSMOp struct {
	//OpID OpID // use as index in map waitCh
	OpID OpID
	Req any // op
}

// used in DoOp in server for Get / Put / FreezeShard / InstallShard / DeleteShard rpc
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // Get, Put
	Key string
	Value string

	ClientID int64
	SeqNum int
	Version rpc.Tversion

	// must add fields for lab5 shardkvsrv
	ShardID shardcfg.Tshid
	ConfigNum shardcfg.Tnum

	Data map[string]shardrpc.DBValue // the kv data in shard, used by installShard
	LastOpResult map[int64]shardrpc.Result
	LastAppliedSeq map[int64]int
	Config shardcfg.ShardConfig

}

func (rsm *RSM) DebugRaftState() (term int, isLeader bool) {
    return rsm.rf.GetState()
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	waitCh       map[OpID]chan any  // commit index in log -> commit chan waitting reply from GoOp
	lastApplied  int // last applied index in raft, it used for conditional invoke DoOp and Restore
	dead         int32

	epoch		 uint64
	nextID		 uint64
}

func (rsm *RSM) Kill() {
	atomic.StoreInt32(&rsm.dead, 1)
	rsm.rf.Kill()
}

func (rsm *RSM) killed() bool {
	z := atomic.LoadInt32(&rsm.dead)
	return z == 1
}

// reader goroutine listen from raft
// 1. reads the applyCh , 
// 2. should hand each committed operation to DoOp()
// 3. if raft size is bigger than maxraftstate bytes should call snapshot
func (rsm *RSM) applier() {

	for {
		if rsm.killed() { return }
		select {
		case msg, ok := <- rsm.applyCh:
			if !ok { return } 
			if msg.CommandValid {
				wrapped, ok := msg.Command.(RSMOp)
				if !ok {
					panic(fmt.Sprintf("RSM received unexpected command type %T", msg.Command))
				}

				// opID := OpID{Server: wrapped.Server, ID: wrapped.ID}
				opID := wrapped.OpID

				// CRITICAL: Ignore commands that are already applied 
				// (though Raft usually ensures this, it's good practice)
				if msg.CommandIndex <= rsm.lastApplied {
					continue
				}

				//3. pass the operation to service to operate
				//res := rsm.sm.DoOp(msg.Command)
				res := rsm.sm.DoOp(wrapped.Req)

				//2. update rsm.lastApplied
				rsm.lastApplied = msg.CommandIndex
			
				// 4. pass the result to the correspond request channel
				rsm.mu.Lock()
				ch, ok := rsm.waitCh[opID]
				rsm.mu.Unlock()

				//log.Printf(
				//	"RSM S=%d NOTIFY index=%d opID=%+v waiting=%v",
				//	rsm.me,
				//	msg.CommandIndex,
				//	wrapped.OpID,
				//	ok,
				//)

				if ok {
					select{
					case ch <- res:
					default:
						// Submit 可能已经超时，或者 channel 已有旧通知。
        				// 绝不能阻塞整个 applier。
					}
				}

				// 4. check we need snapshot or not
				if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
					// invoke server.snapshot and wait return
					snapshot := rsm.sm.Snapshot()
					// invoke raft.snapshot and pass the snap to it
					commandIndex := msg.CommandIndex
					rsm.rf.Snapshot(commandIndex, snapshot)
				}
			} else if msg.SnapshotValid {
				// CRITICAL: Only restore if the snapshot is NEWER than our current state
				if msg.SnapshotIndex <= rsm.lastApplied {
					continue
				}

				// update the index to the snapshot's index
				rsm.lastApplied = msg.SnapshotIndex
			
				// restore sate machine server
				rsm.sm.Restore(msg.Snapshot)
			}
		case <- time.After(200 * time.Millisecond):
			continue // even not received message, should check rsm were killed or not
		}
	}
}

func makeEpoch() uint64 {
	var buf [8]byte

	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("RSM: cannot generate epoch: %v", err))
	}

	epoch := binary.LittleEndian.Uint64(buf[:])

	// 0 可以保留表示“未初始化”。
	if epoch == 0 {
		return makeEpoch()
	}

	return epoch
}


func init() {
    // RSMOp 会作为 interface/any 中的具体类型写入 Raft log。
    labgob.Register(RSMOp{})
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	//labgob.Register(RSMOp{})
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitCh:       make(map[OpID]chan any),
		nextID:		  0,
		epoch:		  makeEpoch(),
	}

	// 必须在 applier 开始应用新日志之前恢复业务状态。
    snapshot := persister.ReadSnapshot()
    if len(snapshot) > 0 {
        rsm.sm.Restore(snapshot)
    }

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// use reader goroutine listen from raft
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	id := atomic.AddUint64(&rsm.nextID, 1)

	opID := OpID{Server: rsm.me, Epoch: rsm.epoch, ID: id}
	wrapped := RSMOp{OpID: opID, Req: req}

	// build the map waitCh
	ch := make(chan any, 1) // the channel can only transmit Result type, and buffered has size 1

	rsm.mu.Lock()
	rsm.waitCh[opID] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitCh, opID)
		rsm.mu.Unlock()
	}()

	_, _, isLeader := rsm.rf.Start(wrapped)

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select{
	case res :=<- ch:
		// ch pass DoOp result to srv/kv shard server
		// 收到的就是这个 OpID 对应的执行结果
        // 不需要再检查当前是否仍然是 leader
		//currentTerm, currIsLeader := rsm.rf.GetState()
        //if !currIsLeader || currentTerm != term {
        //    return rpc.ErrWrongLeader, nil
        //}
		return rpc.OK, res
	case <- timer.C:
		// 不知道请求是否已经提交。
        return rpc.ErrMaybe, nil
	}
}
