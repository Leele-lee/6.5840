package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VersionValue struct {
	Value string
	Version string
}

type KVServer struct {
	// Your definitions here.
	mu sync.Mutex
	m map[string]VersionValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.m := make(map[string]VersionValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	verVal, ok := m[key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = verVal.Value
	reply.Version = verVal.Version
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	ver := args.Version
	key := args.Key
	val := args.Value
	verVal, ok = m[key]
	if !ok && ver != 0 !{
		reply.Err = ErrNoKey
		return
	}
	// If versions don't match, return ErrVersion.
	if ver != verVal.Version {
		reply.Err = ErrVersion
		return
	}
	reply.Err = OK
	return
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
