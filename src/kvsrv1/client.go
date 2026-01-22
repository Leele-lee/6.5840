package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"fmt"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		getArgs := rpc.GetArgs{Key: key}
		getReply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &getArgs, &getReply)
		if !ok {
			// ck.server.Call() returned false (the network dropped the packet)
			// this error belong to all other errors, need forever loop
			fmt.Printf("Call is failed!\n")
			// We sleep for a few milliseconds and LOOP back to try again.
       		//time.Sleep(100 * time.Millisecond)
			continue
		}

		val := getReply.Value
		ver := getReply.Version
		err := getReply.Err

		if err == rpc.OK {
			return val, ver, err
		} else if err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
		//time.Sleep(100 * time.Millisecond)
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
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{
		Key: key, 
		Value: value, 
		Version: version}
	reply := rpc.PutReply{}
	resent := 0

	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	for !ok {
		fmt.Printf("Call is failed!\n")
		ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		resent++
	}
	err := reply.Err

	if resent != 0 && err == rpc.ErrVersion{
		return rpc.ErrMaybe
	}
	return err
}
