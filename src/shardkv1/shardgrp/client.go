package shardgrp

import (

	"math/big"
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
	"crypto/rand"
)

const Debug = false // Set to false to turn off logs when you are done

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientID int64  // unique Id for this clerk
	seqNum int		// incrementing counter for requests
	recentLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, 
		servers: servers,
		clientID: nrand(),
		seqNum: 0,
		recentLeader: 0,
	}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	// don't need lock, bc one clerk for one goroutine
	ck.seqNum++
	serverId := ck.recentLeader
	// a Clerk may have to send an RPC multiple times until it finds a kvserver that replies positively.
	args := rpc.GetArgs{
		Key: key,
		ClientID: ck.clientID,
		SeqNum: ck.seqNum,
	}

	DPrintf("C%d retrying...", ck.clientID)

	for {
		DPrintf("C%d retrying...", ck.clientID)

		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)

		if !ok {
			// ck.server.Call() returned false (the network dropped the packet)
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}

		val := reply.Value
		ver := reply.Version
		err := reply.Err

		if err == rpc.OK {
			ck.recentLeader = serverId
			return val, ver, err
		} else if err == rpc.ErrNoKey {
			ck.recentLeader = serverId
			return "", 0, rpc.ErrNoKey
		} else if err == rpc.ErrWrongLeader {
			// Not the leader or Server is down
        	// Move to the NEXT server (Round Robin)
        	serverId = (serverId + 1) % len(ck.servers)
			continue
		} else if err == rpc.ErrWrongGroup {
			ck.recentLeader = serverId
			return val, ver, err
		} else {
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	ck.seqNum++
	serverId := ck.recentLeader

	args := rpc.PutArgs{
		Key: key, 
		Value: value, 
		Version: version,
		ClientID: ck.clientID,
		SeqNum: ck.seqNum,
	}

	isRetransmission := false

	for {
		DPrintf("C%d retrying...", ck.clientID)

		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		// if ok return false, it implies request lost or reply lost
		if !ok {
			// isduplicate only return true if the first reply lost and make second try
			// Network failure, the next attempt will be a retransmission
			isRetransmission = true
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == rpc.OK {
			ck.recentLeader = serverId
			return rpc.OK

		} else if reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoKey{
			ck.recentLeader = serverId
			// if this not the first attempt, when we got this error about ErrVersion, we can not sure
			// whether the first attempt is success or not, there are two situation:
			// a. the first attempt is success but reply lost, the second retry got ErrVersion
			// b. the first attempt and second are both failed, other clients exceeded them and success, the second
			//    retry also got ErrVersion
			// we can not distinguished these two situation, so we instead direct return ErrVersion, we return ErrMaybe
			if isRetransmission == true {
				return rpc.ErrMaybe
			}
			return reply.Err // rpc.ErrVersion or rpc.ErrNoKey

		} else if reply.Err == rpc.ErrWrongLeader {
			// the request don't execute by service, 
			// so there is no chance that the first attempt success when the second attempt return ErrVersion
			serverId = (serverId + 1) % len(ck.servers)
			continue

		} else if err == rpc.ErrWrongGroup {
			ck.recentLeader = serverId
			return val, ver, err
			
		} else {
			// For any other unexpected error (like a server-side timeout)
			// the request is done by service, it may be successed already, so the next attempt should worry about 
			// tha if it return ErrVersion, is there a chance the first attempt is success, so should return ErrMaybe, 
			// so we should set isRetransmission to true for next attempt
			isRetransmission = true
        	serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
