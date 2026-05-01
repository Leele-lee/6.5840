package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"


)

var useRaftStateMachine bool // to plug in another raft besided raft1


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
	waitCh       map[int]chan any  // commit index in log -> commit chan waitting reply from GoOp
	lastApplied  int // last applied index in raft, it used for conditional invoke DoOp and Restore
}

// reader goroutine listen from raft
// 1. reads the applyCh , 
// 2. should hand each committed operation to DoOp()
// 3. if raft size is bigger than maxraftstate bytes should call snapshot
func (rsm *RSM) applier() {

	for msg := range rsm.applyCh {
		// 1. check msg contains command or not
		if msg.CommandValid {
			// CRITICAL: Ignore commands that are already applied 
            // (though Raft usually ensures this, it's good practice)
			if msg.CommandIndex <= rsm.lastApplied {
				continue
			}

			//2. update rsm.lastApplied
			rsm.lastApplied = msg.CommandIndex

			//3. pass the operation to service to operate
			res := rsm.sm.DoOp(msg.Command)
		
			// 4. pass the result to the correspond request channel
			rsm.mu.Lock()
			ch, ok := rsm.waitCh[msg.CommandIndex]
			rsm.mu.Unlock()

			if ok {
				ch <- res
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
	}

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
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waitCh:      make(map[int]chan any),
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

	// 2. call rf.Start
	index, term, isLeader := rsm.rf.Start(req)
	if !isLeader {
		// not leader, try another server
		return rpc.ErrWrongLeader, nil 
	}

	// is leader
	// build the map waitCh
	ch := make(chan any, 1) // the channel can only transmit Result type, and buffered has size 1
	rsm.mu.Lock()
	rsm.waitCh[index] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitCh, index)
		rsm.mu.Unlock()
	}()

	// 3. wait for reply from DoOp or timeout
	select {
	case res := <- ch:
		// Lost leadership before commit by checking term
		currentTerm, currIsLeader := rsm.rf.GetState()
		if !currIsLeader || currentTerm != term {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, res
	case <- time.After(2000 * time.Millisecond):
		return rpc.ErrWrongLeader, nil
	}
}
