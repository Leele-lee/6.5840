package kvraft

import (
	"math/big"
	"time"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"crypto/rand"
)


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

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.seqNum = 0
	ck.recentLeader = 0
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

	// You will have to modify this function.
	// don't need lock, bc one clerk for one goroutine
	ck.seqNum++
	serverId := ck.recentLeader
	// a Clerk may have to send an RPC multiple times until it finds a kvserver that replies positively.
	args := rpc.GetArgs{
		Key: key,
		ClientID: ck.clientID,
		SeqNum: ck.seqNum,
	}
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
		for !ok {
			// ck.server.Call() returned false (the network dropped the packet)
			// keep re-sending an RPC until it receives a reply
			ok = ck.clnt.Call(ck.servers[serverId], "KVServer.Get", &args, &reply)
			// We sleep for a few milliseconds and LOOP back to try again.
       		time.Sleep(100 * time.Millisecond)
		}

		val := reply.Value
		ver := reply.Version
		err := reply.Err

		if err == rpc.OK {
			ck.recentLeader = serverId
			return val, ver, err
		} else if err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		} else if err == rpc.ErrWrongLeader {
			// Not the leader or Server is down
        	// Move to the NEXT server (Round Robin)
        	serverId = (serverId + 1) % len(ck.servers)
		}
		time.Sleep(50 * time.Millisecond)
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
	// You will have to modify this function.
	ck.seqNum++
	serverId := ck.recentLeader

	args := rpc.PutArgs{
		Key: key, 
		Value: value, 
		Version: version}
	reSent := 0

	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
		// if ok return false, it implies request lost or reply lost
		for !ok {
			ok = ck.clnt.Call(ck.servers[serverId], "KVServer.Put", &args, &reply)
			reSent++
			time.Sleep(100 * time.Millisecond)
		}

		ck.recentLeader = serverId
		err := reply.Err
		if reSent != 0 && err == rpc.ErrVersion{
			// ErrMaybe implied the request/reply may lost or other go first, 
			// you must determine in app
			return rpc.ErrMaybe
		} else if err == rpc.ErrWrongLeader {
			// Not the leader or Server is down
        	// Move to the NEXT server (Round Robin)
        	serverId = (serverId + 1) % len(ck.servers)
		} else if err == rpc.OK {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
}
