package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (

	"sync"
	"time"
	//"fmt"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"

	"6.5840/shardkv1/shardgrp" 
	"6.5840/shardkv1/shardcfg"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.

	// every group has a specific shardgrp.clerk
	// Map Group ID -> specific group clerk
	groupClerks map[tester.Tgid]*shardgrp.Clerk

	// You might need a mutex if your client is multi-threaded
    mu sync.Mutex 
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
		groupClerks: make(map[tester.Tgid]*shardgrp.Clerk),
	}
	// You'll have to add code here.
	return ck
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		// find which shard has this key
		shard := shardcfg.Key2Shard(key)
		
		// To put/get a key from a shardgrp, the shardkv clerk should create a 
		// shardgrp clerk for the shardgrp by calling shardgrp.MakeClerk , 
		// passing in the servers found in the configuration and the shardkv clerk's ck.clnt . 
		// Use the GidServers()  method from ShardConfig  to get the group for a shard

		// bc make shardgrp.clerk need current servers in group which could change at anytime
		// so we makeclerk lazily when we need, not at the shardkv.makeClerk

		// get current config
		config := ck.sck.Query()

		// find the group ID and servers for this shard in configuration
		gid, servers, ok := config.GidServers(shard)

		shardgrp.DPrintf("DEBUG: Key %s belongs to Shard %d, group %d\n", key, shard, gid)


		if !ok {
			// If we reach here, the shard isn't assigned yet (gid 0)
        	// or there's a temporary failure. Wait and retry.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Get (or create) the clerk for this specific group
		ck.mu.Lock()
		gClerk, ok := ck.groupClerks[gid]
		if !ok {
			// not exsit shargrp clerk for current shargrp ID, so we create one
			gClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.groupClerks[gid] = gClerk
		}
		ck.mu.Unlock()
		
		// ask that group for the key
		val, ver, err := gClerk.Get(key)
		
		// handle the result
		if err == rpc.ErrWrongGroup {
			// The config changed while we were talking to them!
            // Loop again to Query() the new config.
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			return val, ver, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	for {
		//  which shard is a key in?
		shard := shardcfg.Key2Shard(key)

		// get current config
		config := ck.sck.Query()

		// find the group ID and servers for this shard in configuration
		gid, servers, ok := config.GidServers(shard)
		if !ok {
			// If we reach here, the shard isn't assigned yet (gid 0)
        	// or there's a temporary failure. Wait and retry.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Get (or create) the clerk for this specific group

		ck.mu.Lock()
		gClerk, ok := ck.groupClerks[gid]
		if !ok {
			// not exsit shargrp clerk for current shargrp ID, so we create one
			gClerk = shardgrp.MakeClerk(ck.clnt, servers)
			ck.groupClerks[gid] = gClerk
		}
		ck.mu.Unlock()

		// put key to that group
		err := gClerk.Put(key, value, version)

		if err == rpc.ErrWrongGroup {
			continue
		} else {
			return err
		}
	}
}
