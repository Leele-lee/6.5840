package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"sort"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// each log entry contains command for state machine, 
// and term when entry was received by leader (first index is 1)
type LogEntry struct {
	Term int
	Command interface{}
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// hearbeat intervals, follow the rule: 
// the leader send heartbeat RPCs no more than ten times per second
const heartbeatInterval = 100 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	lastResetTime time.Time
	electionTimeout time.Duration

	lastHeartbeatTime time.Time

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan raftapi.ApplyMsg
	applyCond *sync.Cond // conditional varibale for Applier to pass commit to service/testser

	// Persistent state on all servers
	currentState State
	currentTerm int   
	votedFor int	  // candidateId that received vote in current term (or null if none)
	logs []LogEntry        // log entries; 

	// Volatile state on all servers
	commitIndex int   // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int   // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders(Reinitialized after election)

	//for each server, index of the next log entry to send to that server
	//  (initialized to leader last log index + 1)
	nextIndex []int

	//for each server, index of highest log entry known to be replicated 
	// on server (initialized to 0, increases monotonically)
	matchIndex []int 

	// for snapshot
	lastIncludedIndex int
	lastIncludedTerm int
}

// translate raft index to a physical slice index in remaining logs
func (rf *Raft) getPhysicalIndex(raftIndex int) int {
	return raftIndex - rf.lastIncludedIndex
}

// get the entry for raft index in logs slice
func (rf *Raft) getLogEntry(raftIndex int) LogEntry {
	return rf.logs[rf.getPhysicalIndex(raftIndex)]
}

// get the raft index of the last entry
func (rf *Raft) getLastIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

// get the term in the logs
func (rf *Raft) getLastTerm() int {
	return rf.logs[len(rf.logs) - 1].Term
}

type PersistState struct {
	CurrentTerm int
	VotedFor int
	Logs []LogEntry

	//snapshot inforamtion should also be persisted
	LastIncludedIndex int
	LastIncludedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.currentState == Leader {
		isleader = true
	}
	return term, isleader
}

// take raft's persistent state and encode(serialize) the state as an array of 
// bytes in order to pass it to persister.
func (rf *Raft) serializeRaftState() []byte {
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var ps PersistState
	ps.CurrentTerm = rf.currentTerm
	ps.VotedFor = rf.votedFor
	ps.Logs = rf.logs
	ps.LastIncludedIndex = rf.lastIncludedIndex
	ps.LastIncludedTerm = rf.lastIncludedTerm

	e.Encode(ps)

	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	raftState := rf.serializeRaftState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftState, snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var ps PersistState

	if d.Decode(&ps) != nil {
		panic("readPersist: failed to decode Raft state")
	} else {
		rf.currentTerm = ps.CurrentTerm
		rf.votedFor = ps.VotedFor

		rf.logs = make([]LogEntry, len(ps.Logs))
		copy(rf.logs, ps.Logs)

		rf.lastIncludedIndex = ps.LastIncludedIndex
		rf.lastIncludedTerm = ps.LastIncludedTerm
		
		rf.lastApplied = ps.LastIncludedIndex
		rf.commitIndex = ps.LastIncludedIndex
	}
}

// how many bytes in Raft's persisted log?
// used for the service to know when to snapshot(the log is too bigger)
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// notice can not discard old logs by direct slice logs
	// Truncate rf.logs.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastIncludedIndex >= index {
		return
	}

	pindex := rf.getPhysicalIndex(index)

	rf.lastIncludedTerm = rf.logs[pindex].Term

	remainingLogs := rf.logs[pindex:]
	newlogs := make([]LogEntry, len(remainingLogs))
	copy(newlogs, remainingLogs)

	rf.logs = newlogs

	// Update lastIncludedIndex
	rf.lastIncludedIndex = index

	// Call rf.persister.Save(state, snapshot).
	raftState := rf.serializeRaftState()
	rf.persister.Save(raftState, snapshot)

	// ADD ANNOTATION HERE
    tester.Annotate(fmt.Sprintf("S%d", rf.me), "Snapshot", 
	fmt.Sprintf("lastIncludexIndex set to: %d, term: %d", 
    pindex, rf.lastIncludedTerm))
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		oldTerm := rf.currentTerm
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = -1

		rf.persist()

		// ADD ANNOTATION HERE
    	tester.Annotate(fmt.Sprintf("S%d", rf.me), "Step Down", 
		fmt.Sprintf("Term %d -> %d. Found higher term from candidate S%d in RequestVote", 
        oldTerm, args.Term, args.CandidateId))
	}

	currentLastLogIndex := rf.getLastIndex()
	currentLastLogEntry := rf.getLogEntry(currentLastLogIndex)

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date 
	// as receiver’s log, grant vote
    // Raft determines which of two logs is more up-to-date by comparing the index
	//  and term of the last entries in the logs. If the logs have last entries with different terms,
	//  then the log with the later term is more up-to-date. 
	// If the logs end with the same term, then whichever log is longer is more up-to-date.

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > currentLastLogEntry.Term {
			reply.VoteGranted = true
		} else if args.LastLogTerm == currentLastLogEntry.Term {
			if args.LastLogIndex >= currentLastLogIndex {
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false

				// Log why we refused
        		tester.Annotate(fmt.Sprintf("S%d", rf.me), "Vote Denied", 
            	fmt.Sprintf("Refused S%d. Has same term: %d, but has longer log: %d, VotedFor: %d", 
            	args.CandidateId, args.LastLogTerm, currentLastLogIndex, rf.votedFor))
				return
			}
		} else {
			reply.VoteGranted = false

			// Log why we refused
        	tester.Annotate(fmt.Sprintf("S%d", rf.me), "Vote Denied", 
            fmt.Sprintf("Refused S%d. Has larger term: %d, VotedFor: %d", 
            args.CandidateId, currentLastLogEntry.Term, rf.votedFor))
			return
		}

		rf.votedFor = args.CandidateId
		rf.persist()

		// ADD ANNOTATION HERE
        tester.Annotate(fmt.Sprintf("S%d", rf.me), "Vote Granted", 
        fmt.Sprintf("Voted for S%d in Term %d", args.CandidateId, args.Term))

		// when granting vote, reset election timeout
		rf.resetElectionTimer()

		// ADD ANNOTATION HERE
    	tester.Annotate(fmt.Sprintf("S%d", rf.me), "Reset Election Timeout", 
        fmt.Sprintf("Success vote for S%d", args.CandidateId))
		return
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int
	LeaderID int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	XTerm int    // term in the conflicting entry (if any)
	XIndex int   // index of first entry with that term (if any)
	XLen int 	 // log length
}

// appendEntry handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		oldTerm := rf.currentTerm
		rf.currentTerm = args.Term
		rf.currentState = Follower
		rf.votedFor = -1

		// ADD ANNOTATION HERE
    	tester.Annotate(fmt.Sprintf("S%d", rf.me), "Step Down", 
		fmt.Sprintf("Term %d -> %d. Found higher term from leader S%d in AE", 
        oldTerm, args.Term, args.LeaderID))
	}

	// THE FIX: Snapshot Bounds Check
	// If the leader is talking about an index we've already snapshotted, 
	// we can't perform the consistency check.
	if rf.lastIncludedIndex > args.PreLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1
		reply.XIndex = -1
		// be used as nextIndex in sendAppendEntries reply handler
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	reply.XLen = len(rf.logs) - 1 + rf.lastIncludedIndex
	// perform check. If that check fails, you return Success = false and you stop immediately. 
	// You do not perform the deletion or the appending

	if len(rf.logs) - 1 + rf.lastIncludedIndex < args.PreLogIndex {
		// receiver log's length smaller than leader log's length
		// c. follower's log is too short
		// so need add lacking entry
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false

		// ADD ANNOTATION HERE
    	tester.Annotate(fmt.Sprintf("S%d", rf.me), "follower's log too short", 
        fmt.Sprintf("XTerm: %d, XIndex: %d, XLen: %d", reply.XTerm, reply.XIndex, reply.XLen))

		return
	} else {
		// receiver log's length >= leader log's length
		// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		if rf.getLogEntry(args.PreLogIndex).Term != args.PreLogTerm {
			reply.Success = false
			reply.XTerm = rf.getLogEntry(args.PreLogIndex).Term
			// find the XIndex, index of first entry with that conflict term
			i := args.PreLogIndex
			for i > 0 && rf.getLogEntry(i).Term == reply.XTerm {
				i--;
			}
			reply.XIndex  = i

			// ADD ANNOTATION HERE
    		tester.Annotate(fmt.Sprintf("S%d", rf.me), "AE false mesg", 
        	fmt.Sprintf("XTerm: %d, XIndex: %d, XLen: %d", reply.XTerm, reply.XIndex, reply.XLen))

			return
		} else {
			// If an existing entry conflicts with a new one (same index but different terms), 
			// delete the existing entry and all that follow it
			reply.Success = true

			// update time if receive a valid rpcs call
			rf.resetElectionTimer()

			// ADD ANNOTATION HERE
    		//tester.Annotate(fmt.Sprintf("S%d", rf.me), "Reset Election Timeout", 
        	//fmt.Sprintf("Success receive AE from %d", args.LeaderID))

			// The Log Cleanup & Append.
			// append log entries, can not direct append bc of disorder rpc arrive
			// wrong direct append: rf.logs = append(rf.logs, args.Entries)
			// safety append: If an existing entry conflicts with a new one 
			// (same index but different terms), delete the existing entry and all that follow it
			for i, entry := range args.Entries {
				checkConflictIndex := args.PreLogIndex + 1 + i
				if len(rf.logs) + rf.lastIncludedIndex > checkConflictIndex {
					if rf.getLogEntry(checkConflictIndex).Term != entry.Term {
						// truant
						rf.logs = rf.logs[:rf.getPhysicalIndex(checkConflictIndex)]
						// append
						newEntries := make([]LogEntry, len(args.Entries[i:]))
						copy(newEntries, args.Entries[i:])
						rf.logs = append(rf.logs, newEntries...)
						rf.persist()
						break
					}
				} else {
					newEntries := make([]LogEntry, len(args.Entries[i:]))
					copy(newEntries, args.Entries[i:])
					rf.logs = append(rf.logs, newEntries...)
					rf.persist()
					break
				}
			}
		}
	}

	//  update commitIndex. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, 
	// index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := args.PreLogIndex + len(args.Entries)
		if args.LeaderCommit < lastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}

		// Now signal the 'Applier' to push these to the applyCh
    	rf.applyCond.Signal() 
	}
}

// background goroutine, wake up when commitIndex is update and send new applyMsg to Applych
// each time a new entry is committed to the log, each Raft peer 
// should send an ApplyMsg to the service (or tester) through applych
func (rf *Raft) applier() {
	for rf.killed() == false {
		time.Sleep(15 * time.Millisecond)
		rf.mu.Lock()
		// check commitIndex is update or not
		// must using for loop to check instead of if 
		// bc "Spurious Wakeup", conditional variable can occasionally wake up even if nobody called Signal()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait() // This atomics: Releases Lock + Sleeps
		}

		// --- ADD THIS GUARD ---
        // If a snapshot was installed while we were sleeping,
        // our lastApplied might be way behind the new physical log start.
        if rf.lastApplied < rf.lastIncludedIndex {
            rf.lastApplied = rf.lastIncludedIndex
        }

		// We woke up and we have the lock! 
        // Grab all the committed messages.
		var msgs []raftapi.ApplyMsg
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			newMsg := raftapi.ApplyMsg {
				CommandValid: true,
				Command: rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			msgs = append(msgs, newMsg)
		}

		// must unlock before send ApplyMsg to the applyCh
		rf.mu.Unlock()

		// Send messages to the service. 
        // If the channel blocks here, we DON'T hold the Raft lock!
        // Heartbeats and elections can still happen in the background.
		for _, msg := range msgs {
			if !rf.killed() {
				rf.applyCh <- msg
			}
		}
	}
}

//appendEntry rpc to server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// append new command from client to leader logs and replicate to followers
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// In Lab 3D, the index returned by Start() must be the Logical Raft Index, 
// not the length of the slice
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// append command to our local log entry
	term = rf.currentTerm
	newLogEntry := LogEntry{Term: term, Command: command}
	rf.logs = append(rf.logs, newLogEntry)
	rf.persist()
	// len(rf.logs) - 1 is physical index
	index = rf.getLastIndex()

	// broadcast logs to followers
	go rf.broadcastAppendEntries()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// add for kvraft rsm.go
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, 
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)" 
// this check also need a conditional varibale
// N is median
func (rf *Raft) updateCommitIndex() {
	// copy logs to a newlist matchIndexList and replace matchIndexList[me] to the 
	// last index in rf.logs
	matchIndexList := make([]int, len(rf.peers))
	copy(matchIndexList, rf.matchIndex)
	matchIndexList[rf.me] = len(rf.logs) - 1 + rf.lastIncludedIndex

	// sort the list from smallest to largest
	sort.Ints(matchIndexList)

	// find the median in list -- N
	medianIndex := (len(matchIndexList) - 1)/2
	N := matchIndexList[medianIndex]

	if N > rf.commitIndex && rf.getLogEntry(N).Term == rf.currentTerm {
		rf.commitIndex = N
	}
}

type InstallSnapshotArgs struct {
	Term int               // leader's term
	LeaderID int
	LastIncludedIndex int  // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int   // term of lastIncludedIndex
	Data []byte			   // the entire snapshot
}

type InstallSnapshotReply struct {
	Term int              // currentTerm, for leader to update itself
}

// sendSnapshot handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	//return immediatly if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		oldTerm := rf.currentTerm
        rf.currentTerm = args.Term
        rf.currentState = Follower
        rf.votedFor = -1

        // ADD ANNOTATION HERE
        tester.Annotate(fmt.Sprintf("S%d", rf.me), "Step Down", 
        fmt.Sprintf("Term %d -> %d. Found higher term from leader S%d in IS", 
        oldTerm, args.Term, args.LeaderID))
	}

	// reject stale snapshot
	// bc of take care that these snapshots only advance the service's state, 
	// and don't cause it to move backwards
	if args.LastIncludedIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	// If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	// Keep entries after LastIncludedIndex if they match, otherwise wipe
	indexinP := rf.getPhysicalIndex(args.LastIncludedIndex)
	if indexinP > 0 && indexinP < len(rf.logs) && rf.getLogEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		remainingLogs := rf.logs[indexinP:]
		newLogs := make([]LogEntry, len(remainingLogs))
		copy(newLogs, remainingLogs)
		rf.logs = newLogs
	} else {
		rf.logs = []LogEntry{
			{Term : args.LastIncludedTerm},
		}
	}

	// update snapshot information
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// update the commitIndex and lastApplied
	// why not update matchIndex and nextIndex?
	// bc these are leader-only variables

	// why update this before send to channel?
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	// persistent snapshot
	raftState := rf.serializeRaftState()
	rf.persister.Save(raftState, args.Data)

	// handover to service
	// use the applyCh to send the snapshot to service in an ApplyMsg
	// Take care that these snapshots only advance the service's state, 
	// and don't cause it to move backwards
	msg := raftapi.ApplyMsg {
		SnapshotValid : true,
		Snapshot : args.Data,
		SnapshotTerm : args.LastIncludedTerm,
		SnapshotIndex : args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	if !rf.killed() {
		rf.applyCh <- msg
	}
}


func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, 
	reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) handleSendInstallSnapshotReply(i int, args *InstallSnapshotArgs, 
	reply *InstallSnapshotReply, term int) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// handler sendSnapshot reply
		// Critical: Check if the world changed while we were waiting
		if rf.currentState != Leader || rf.currentTerm != term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentState = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			return
		}
		// SUCCESS! Update the follower's progress
		// The follower is now exactly at the snapshot index
		if args.LastIncludedIndex > rf.matchIndex[i] {
			rf.matchIndex[i] = args.LastIncludedIndex
			rf.nextIndex[i] = rf.matchIndex[i] + 1
		}
}

func (rf *Raft) sendInstallSnapshotToPeer(i int, term int) {
	rf.mu.Lock()
	if rf.currentState != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	
	// direct read snapshot form memeory
	args := InstallSnapshotArgs {
		Term : term,
		LeaderID : rf.me,
		LastIncludedIndex : rf.lastIncludedIndex,
		LastIncludedTerm : rf.lastIncludedTerm,
		Data : rf.persister.ReadSnapshot(),
	}

	reply := InstallSnapshotReply {}

	rf.mu.Unlock()

	ok := rf.sendSnapshot(i, &args, &reply)
	if !ok {
		return
	}
	rf.handleSendInstallSnapshotReply(i, &args, &reply, term)
}

// can be used by leader to send heartbeats and logs to peers
// if need to send log instead of heartbeat you should change the code
// the Entry can not be direct set to nil
// i represent peer id
func (rf *Raft) sendAppendEntriesToPeer(i int, term int) {
	rf.mu.Lock()

	var entry []LogEntry
	// You must ensure the Term matches the specific "incumbency" 
	// (the specific term of leadership) you are currently serving.

	// before really call AppendEntries, you unlock, so you must recheck
	// state and term
	if rf.currentState != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	// Check if the entry we need is gone
    if rf.nextIndex[i] <= rf.lastIncludedIndex {
		// We can't use AppendEntries! We MUST send a snapshot instead.
		rf.mu.Unlock()
		go rf.sendInstallSnapshotToPeer(i, term)
		return
    }

	preLogIndex := rf.nextIndex[i] - 1
	preLogTerm := rf.getLogEntry(preLogIndex).Term

	// if leader has more entry than follower, send AE otherwise send hearbeat(no entry)
	if len(rf.logs) - 1 + rf.lastIncludedIndex >= rf.nextIndex[i] {
		entry = rf.logs[rf.getPhysicalIndex(rf.nextIndex[i]):]
	} else {
		entry = nil
	}

	args := AppendEntriesArgs {
		Term: term,
		LeaderID: rf.me,
		PreLogIndex: preLogIndex,
		PreLogTerm: preLogTerm,
		Entries: entry,
		LeaderCommit: rf.commitIndex,
    }
	rf.mu.Unlock()
	reply := AppendEntriesReply {}

	ok := rf.sendAppendEntries(i, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// handle AppendEntries reply
	// Critical: Check if the world changed while we were waiting
	if rf.currentState != Leader || rf.currentTerm != term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentState = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success == false {
		// first check follower's log is too short or not
		// case c: (follower's log is too short): nextIndex = XLen
		if reply.XTerm == -1 {
			rf.nextIndex[i] = reply.XLen
		} else {  // then check the leader has conflict term or not
			j := args.PreLogIndex
			for j > 0 && rf.getLogEntry(j).Term != reply.XTerm {
				j--
			}
			if j == 0 {  // case a: (leader doesn't have XTerm): nextIndex = XIndex
				rf.nextIndex[i] = reply.XIndex
			} else {   // Case b: (leader has XTerm): nextIndex = leader's last entry for XTerm
				rf.nextIndex[i] = j
			}
		}
		if rf.nextIndex[i] < 1 {
			rf.nextIndex[i] = 1
		}

		// if false, retry immedaitly
		go rf.sendAppendEntriesToPeer(i, term)

	} else {
		// if AppendEntry success, you should update matchIndex
		newMatchIndex := args.PreLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[i] {
			rf.matchIndex[i] = newMatchIndex
		}
		// update nextIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1

		// must check success in majority or not
		// if yes, update commitIndex and signal
		// reply to client after the job is done(this done in server.go)
		oldCommitIndex := rf.commitIndex
        rf.updateCommitIndex()
        if rf.commitIndex > oldCommitIndex {
             rf.applyCond.Signal()
       }
	}
}

// Once a candidate wins an election, it becomes leader. 
// It then sends heartbeat messages to all of the other servers 
// to establish its authority and prevent new elections.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()

	// don't check term here, bc if you check term, you will effect efficiency
	// go may delay, when term 5 leader become follower in term 6
	// and then become leader again, go routine wake up, yous hould immediatly
	// send heartbeat, and repeate sending at at the same term is okay
	// if you worried about stale term, bc disconnected, it will be check before
	// send AppendEntries at sendAppendEntriesToPeer

	//bc check leader and set term is at the same block of lock code
	//it don't need check term again. if you are leader, send heartbeat at current term
	// but before really send AppendEntries, there has unlock time, so must re-check state and term
	// (in sendAppendEntriesToPeer)
	// check the term is equal to the term when decide send heartbeats's term or not
	if rf.currentState != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	//rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i, term)
	}
	rf.mu.Unlock()
}

// (a) it wins the election
// Once a candidate wins an election, it becomes leader. 
// It then sends heartbeat messages to all of the other servers to establish its authority 
// and prevent new elections
func (rf *Raft) becomeLeader() {
	rf.currentState = Leader

	// ADD ANNOTATION HERE
    tester.Annotate(fmt.Sprintf("S%d", rf.me), "Becoming Leader", fmt.Sprintf("For term: %d", rf.currentTerm))

	// nextIndex[] and matchIndex[] need to be reinitialized after election
	// lastLogIndex need change to rf.lastIncludedIndex + len(rf.logs) - 1 in 3C
	lastLogIndex := rf.getLastIndex()
	for i, _ := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.lastHeartbeatTime = time.Now()
	// send heartbeats
	go rf.broadcastAppendEntries()
}

// A candidate continues in candidate state until one of three things happens: 
// (a) it wins the election, A candidate wins an election if it receives 
// votes from a majority of the servers in the full cluster for the same term.

// (b) another server establishes itself as leader.  While waiting for votes, 
// a candidate may receive an AppendEntries RPC from another server claiming to be leader. 
// If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, 
// then the candidate recognizes the leader as legitimate and returns to follower state. 
// If the term in the RPC is smaller than the candidate’s current term, then the candidate 
// rejects the RPC and continues in candidate state

// (c) a period of time goes by with no winner

func (rf *Raft) handleVoteReply(termStartAtElection int, voteNum *int, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != Candidate || rf.currentTerm != termStartAtElection {
		//rf.mu.Unlock()
		return
	}

	// if RPC request or response contains term T > currentTerm, set T = currentTerm,
	// convert to followers
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentState = Follower
		rf.votedFor = -1
		//rf.mu.Unlock()
		rf.persist()
		return
	}
	// (a) it wins the election
	// Once a candidate wins an election, it becomes leader. 
	// It then sends heartbeat messages to all of the other servers to establish its authority 
	// and prevent new elections

	if reply.VoteGranted {
		*voteNum++
		    
		// ADD ANNOTATION HERE
    	tester.Annotate(fmt.Sprintf("S%d", rf.me), "Handle Vote", 
        fmt.Sprintf("Vote num is: %d", 
        *voteNum))

		if *voteNum > len(rf.peers)/2 {
			rf.becomeLeader()
			return
		}
	}
	//rf.mu.Unlock()
}

// You must reset election timeout when:
// 1. receiving valid AppendEntries
// 2. granting vote
// 3. starting new election

func (rf *Raft) resetElectionTimer() {
	rf.lastResetTime = time.Now()
	// electionTimeout is a random number in 300 to 600
	rf.electionTimeout = time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.currentState == Leader {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.currentState = Candidate

	// ADD ANNOTATION HERE
    tester.Annotate(fmt.Sprintf("S%d", rf.me), "Election Start", 
    fmt.Sprintf("Starting election for Term %d", rf.currentTerm))

	termStartAtElection := rf.currentTerm

	// vote for itself
	rf.votedFor = rf.me;

	rf.persist()

	// reset election timer
	rf.resetElectionTimer()

	lastLogIndex := rf.getLastIndex()
	LastLogTerm := rf.getLastTerm()

	voteNum := 1
	// issues RequestVote RPCs in parallel to each of the other servers in the cluster
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		// goroutine make sure send request in parallel
		go func(i int) {
			args := RequestVoteArgs{
				Term: termStartAtElection, 
				CandidateId: rf.me, 
				LastLogIndex: lastLogIndex,
				LastLogTerm: LastLogTerm,
			}
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(i, &args, &reply)
			if ok {
				rf.handleVoteReply(termStartAtElection, &voteNum, &reply)
			}
		}(i)
	}
	rf.mu.Unlock()
}



func (rf *Raft) ticker() {

	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		switch rf.currentState {
		case Follower, Candidate:
			// check election time-out
			if time.Since(rf.lastResetTime) >= rf.electionTimeout {
				rf.resetElectionTimer()
				go rf.startElection()
			}
		case Leader:
			// send heartbeat periodically
			if time.Since(rf.lastHeartbeatTime) >= heartbeatInterval {
				rf.lastHeartbeatTime = time.Now()
				go rf.broadcastAppendEntries()
			}
		}
		rf.mu.Unlock()

		// Sleep for a short time (don't sleep for the whole timeout!)
        // It's better to check the clock frequently.
		time.Sleep(15 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, 
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//used for applier()
	rf.applyCh = applyCh

	// bind lock to the condition
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).

	// when servers start up, they begin as followers
	rf.currentState = Follower;
	rf.votedFor = -1;
	rf.resetElectionTimer();
	
	rf.logs = make([]LogEntry, 1);
	rf.logs[0] = LogEntry{Term: 0}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers));
	rf.matchIndex = make([]int, len(peers));

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm  = 0

	// initialize from state persisted before a crash
	// When the tester restarts a crashed server, it calls Make() again. 
	// Without this line, the server would wake up with "amnesia" (Term 0, empty Log).
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()


	return rf
}
