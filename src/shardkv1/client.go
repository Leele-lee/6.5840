package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (

	"math/big"
	"sync"
	"time"
	//"fmt"
	"math/rand"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"

	crand "crypto/rand"

	"6.5840/shardkv1/shardgrp" 
	"6.5840/shardkv1/shardcfg"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}


type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.

	// every group has a specific shardgrp.clerk
	// Map Group ID -> specific group clerk
	groupClerks map[tester.Tgid]*shardgrp.Clerk
	config *shardcfg.ShardConfig
	clientID int64  // unique Id for this clerk
	seqNum int		// incrementing counter for requests
	


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
		config: nil,  // lazily set 
		clientID: nrand(),
		seqNum: 0,
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
	//maxRetry := 30
	//for i := 0; i < maxRetry; i++ {
	for {
		// find which shard has this key
		shard := shardcfg.Key2Shard(key)
		
		// 1. SAFELY READ CACHED CONFIG
        ck.mu.Lock()
        config := ck.config
        ck.mu.Unlock()

		// 2. LAZY FETCH: If we don't have a config, get one now
        if config == nil {
			shardgrp.DPrintf("GET: for get key %s, the ck.config is nil", key)
            config = ck.sck.Query()
            ck.mu.Lock()
            ck.config = config
            ck.mu.Unlock()

        }

		// To put/get a key from a shardgrp, the shardkv clerk should create a 
		// shardgrp clerk for the shardgrp by calling shardgrp.MakeClerk , 
		// passing in the servers found in the configuration and the shardkv clerk's ck.clnt . 
		// Use the GidServers()  method from ShardConfig  to get the group for a shard

		// bc make shardgrp.clerk need current servers in group which could change at anytime
		// so we makeclerk lazily when we need, not at the shardkv.makeClerk

		// get current config
		//config := ck.sck.Query()

		shardgrp.DPrintf("GET: for get key %s current config is %v", key, config)
		// find the group ID and servers for this shard in configuration
		gid, servers, ok := config.GidServers(shard)

		shardgrp.DPrintf("DEBUG: Key %s belongs to Shard %d, group %d\n", key, shard, gid)


		if !ok {
			// If we reach here, the shard isn't assigned yet (gid 0)
        	// or there's a temporary failure. Wait and retry.
			time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
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
		val, ver, err := gClerk.Get(key, config.Num, ck.clientID, ck.seqNum)

		// handle the result
		if err == rpc.ErrWrongGroup || err == rpc.ErrMaybe || err == rpc.ErrWrongLeader {
			// The config changed while we were talking to them!
            // Loop again to Query() the new config.
			newConfig := ck.sck.Query()
            ck.mu.Lock()
            ck.config = newConfig
            ck.mu.Unlock()
			time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
			continue
		} else {
			return val, ver, err
		}
	}
	//return "", 0, rpc.ErrMaybe
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.

	ck.seqNum++
	for {
		//  which shard is a key in?
		shard := shardcfg.Key2Shard(key)

		// 1. SAFELY READ CACHED CONFIG
        ck.mu.Lock()
        config := ck.config
        ck.mu.Unlock()

		// 2. LAZY FETCH: If we don't have a config, get one now
        if config == nil {
			//shardgrp.DPrintf("GET: for get key %s, the ck.config is nil", key)

            config = ck.sck.Query()
            ck.mu.Lock()
            ck.config = config
            ck.mu.Unlock()
        }
		// get current config
		//config := ck.sck.Query()

		// find the group ID and servers for this shard in configuration
		gid, servers, ok := config.GidServers(shard)
		if !ok {
			// If we reach here, the shard isn't assigned yet (gid 0)
        	// or there's a temporary failure. Wait and retry.
			time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
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
		err := gClerk.Put(key, value, version, config.Num, ck.clientID, ck.seqNum)

		if err == rpc.ErrWrongGroup || err == rpc.ErrMaybe || err == rpc.ErrWrongLeader{
			newConfig := ck.sck.Query()
            ck.mu.Lock()
            ck.config = newConfig
            ck.mu.Unlock()
			time.Sleep(time.Duration(50+rand.Intn(50)) * time.Millisecond)
			continue
		} else {
			return err
		}
	}
}
