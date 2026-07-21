package shardgrp

import (

	"math/big"
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
	"crypto/rand"
	"6.5840/shardkv1/shardgrp/shardrpc"
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
	//clientID int64  // unique Id for this clerk
	//seqNum int		// incrementing counter for requests
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
		//clientID: nrand(),
		//seqNum: 0,
		recentLeader: 0,
	}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist.

// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)

// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string, configNum shardcfg.Tnum, clientID int64, seqNum int) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	// don't need lock, bc one clerk for one goroutine
	//ck.seqNum++
	serverId := ck.recentLeader
	// a Clerk may have to send an RPC multiple times until it finds a kvserver that replies positively.
	args := rpc.GetArgs{
		Key: key,
		ClientID: clientID,
		SeqNum: seqNum,
		ConfigNum: configNum,
	}

	// TRY ONLY 5 TIMES through all servers, then give up
	// and return rpc.ErrMaybe, often happened when controller partition happened
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0 ; i < len(ck.servers); i++ {
			//DPrintf("GET rpc: for (key: %s, clientID: %d, SeqNum: %d) send to server %d",
			//  key, clientID, seqNum, serverId)

			reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)

			// if ok return false, it implies request lost or reply lost
			// or current server died
			if !ok {
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
				//time.Sleep(100 * time.Millisecond)
				return "", 0, rpc.ErrNoKey
			} else if err == rpc.ErrWrongLeader {
				// Not the leader or Server is down
				// Move to the NEXT server (Round Robin)
				DPrintf("Client %d get detected wrong leader for key %s in server %d", clientID, key, serverId)
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue
			} else if err == rpc.ErrWrongGroup {
				ck.recentLeader = serverId
				DPrintf("Client get detected wrong group, querying shardctrler...")

				return val, ver, err
			} else if err == rpc.ErrRetry {
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				serverId = (serverId + 1) % len(ck.servers)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// IMPORTANT: If we cycled through everyone and no leader was found,
    // return a temporary error so the ShardKV Clerk can refresh its config.
    return "", 0, rpc.ErrMaybe 
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
func (ck *Clerk) Put(key string, value string, version rpc.Tversion,
	 configNum shardcfg.Tnum, clientID int64, seqNum int) rpc.Err {
	// Your code here
	//ck.seqNum++
	serverId := ck.recentLeader

	args := rpc.PutArgs{
		Key: key, 
		Value: value, 
		Version: version,
		ClientID: clientID,
		SeqNum: seqNum,
		ConfigNum: configNum,
	}

	isRetransmission := false

	// TRY ONLY 5 TIMES through all servers, then give up
	for attempt := 0; attempt < 5; attempt++ {
		for i := 0; i < len(ck.servers); i++ {
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
			DPrintf("Put(key: %s, val: %s, clientID: %d, ver: %d) to server %d received %s ",
			 key, value, clientID, version, serverId, reply.Err)
			// if ok return false, it implies request lost or reply lost
			// or current server died
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

			} else if reply.Err == rpc.ErrVersion {
				ck.recentLeader = serverId
				// if this not the first attempt, when we got this error about ErrVersion, 
				// we can not sure whether the first attempt is success or not, 
				// there are two situation:
				// a. the first attempt is success but reply lost, the second retry got ErrVersion
				// b. the first attempt and second are both failed, other clients exceeded them and success, 
				// the second retry also got ErrVersion
				//
				// we can not distinguished these two situation, 
				// so we instead direct return ErrVersion, we return ErrMaybe
				if isRetransmission == true {
					return rpc.ErrMaybe
				}
				return reply.Err // rpc.ErrVersion or rpc.ErrNoKey

			} else if reply.Err == rpc.ErrNoKey {
				ck.recentLeader = serverId
				return reply.Err

			} else if reply.Err == rpc.ErrWrongLeader {
				// the request don't execute by service, 
				// so there is no chance that the first attempt success
				//  when the second attempt return ErrVersion
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
				continue

			} else if reply.Err == rpc.ErrWrongGroup {
				ck.recentLeader = serverId
				return reply.Err

			} else if reply.Err == rpc.ErrRetry {
				time.Sleep(100 * time.Millisecond)
				continue

			} else {
				// For any other unexpected error (like a server-side timeout)
				// the request is done by service, it may be successed already, so the next attempt should worry about 
				// tha if it return ErrVersion, is there a chance the first attempt is success, so should return ErrMaybe, 
				// so we should set isRetransmission to true for next attempt
				isRetransmission = true
				serverId = (serverId + 1) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return rpc.ErrMaybe
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) (map[string]shardrpc.DBValue, map[int64]shardrpc.Result, map[int64]int, rpc.Err) {
	// Your code here
	//serverId := ck.recentLeader

	args := shardrpc.FreezeShardArgs {
		Shard: s,
		Num: num,
	}

	start := ck.recentLeader

	//尝试 group 中每台 server 一次
	//→ 找到 leader 就返回结果
	//→ 全部失败则返回 ErrMaybe
	for offset := 0; offset < len(ck.servers); offset++ {
		serverID := (start + offset) % len(ck.servers)
		reply := shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[serverID], "KVServer.FreezeShard", &args, &reply)

		// ck.server.Call() returned false (the network dropped the packet)
		// can happened when controller partition
		if !ok {
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			continue
		}

		// 收到了业务结果，记录这个 server
		ck.recentLeader = serverID

		if reply.Err == rpc.OK {
			//DPrintf("C%d FreezeShard for shard %d, Num %d Success to Group Leader %d", ck.clientID, s, num, serverId)
			return reply.Data, reply.LastOpResult, reply.LastAppliedSeq, reply.Err
		}
		return nil, nil, nil, reply.Err

	}
	return nil, nil, nil, rpc.ErrMaybe
}


func (ck *Clerk) InstallShard(s shardcfg.Tshid, data map[string]shardrpc.DBValue, 
	lastOpResult map[int64]shardrpc.Result, lastAppliedSeq map[int64]int, num shardcfg.Tnum) rpc.Err {
	// Your code here
	//serverId := ck.recentLeader
	args := shardrpc.InstallShardArgs {
		Shard: s,
		Data: data,
		LastOpResult: lastOpResult,
		LastAppliedSeq: lastAppliedSeq,
		Num: num,
	}

	start := ck.recentLeader

	for offset := 0; offset < len(ck.servers); offset++ {
		serverID := (start + offset) % len(ck.servers)
		reply := shardrpc.InstallShardReply {}
		ok := ck.clnt.Call(ck.servers[serverID], "KVServer.InstallShard", &args, &reply)

		// ck.server.Call() returned false (the network dropped the packet)
		// can happened when controller partition
		if !ok {
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			continue
		}

		ck.recentLeader = serverID
		return reply.Err
	}

	return rpc.ErrMaybe
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	//serverId := ck.recentLeader
	args := shardrpc.DeleteShardArgs {
		Shard: s,
		Num: num,
	}

	start := ck.recentLeader

	for offset := 0; offset < len(ck.servers); offset++ {
		serverID := (start + offset) % len(ck.servers)
		reply := shardrpc.DeleteShardReply {}
		ok := ck.clnt.Call(ck.servers[serverID], "KVServer.DeleteShard", &args, &reply)

		// ck.server.Call() returned false (the network dropped the packet)
		// can happened when controller partition
		if !ok {
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			continue
		}

		ck.recentLeader = serverID
		return reply.Err
	}
	return rpc.ErrMaybe
}
