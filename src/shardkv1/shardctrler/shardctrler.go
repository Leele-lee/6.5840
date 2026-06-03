package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"log"
	"fmt"
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

const {
	currConfigKey = "current-config"
	nextConfigKey = "next-config"
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	//currConfigKeyNum shardgrp.Tnum // the latest configure version number
	//currConfigKey sharddcfg.ShardConfig
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// first check if there is a stored next configuration with 
	// a TASK higher configuration number than the current one
	nextStr, _, _ := sck.IKVClerk.Get(nextConfigKey)
	currStr, _, _ := sck.IKVClerk.Get(currConfigKey)

	if currStr == "" { return }
	currConfig := shardcfg.FromString(currStr)

	if nextStr != "" {
		nextConfig := shardcfg.FromString(nextStr)
		// check config version num
		if nextConfig.Num > currConfig.Num {
			// and if next config has a bigger config version, 
			// complete the shard moves necessary to reconfigure to the next one

			// 1. Re-run the moves (they must be idempotent!)
			sck.executeMoves(currConfig, nextConfig)

			// 2. FINALLY POST the new configuration
			safeUpdate(nextConfigKey, nextStr)
		}
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here

	// convert confifgure to string
	configString := cfg.String()

	// Choose a key that represents "Version 0"
    // (Ensure your Query() logic knows how to find this key later)
	key := "config-0"

	// put k/v pairs to kvsrv
	// set version to 0 bc this was the first time to put, the key not exsit yet
	//err := sck.IKVClerk.Put(key, configString, 0)
	//err2 := sck.IKVClerk.Put("current-config", configString, 0)
	safeUpdate(key, configString)
	safeUpdate(currConfigKey, configString)


	// update the latest configure version number
	sck.currConfigKey = cfg
}

// get current version and put version in new put, if superseded by another controller 
// it will retry get version and put again
func (sck *ShardCltler) safeUpdate(key String, newValue String) {
	for {
		// 1. get current version, if key not exist, the version will be default 0
		val, ver, err := sck.IKVClerk.Get(key)

		// --- OPTIMIZATION (Important for idempotency) ---
        // If the value is ALREADY what we want, we can stop! 
        // This handles "Case A" (where we succeeded but got ErrMaybe).
        if val == newValue {
            return 
        }

		// 2. PUT: Attempt update
        // If Get returned version 0 (key doesn't exist), we send 0.
        // If Get returned version 5, we send 5.
		putErr := sck.IKVClerk.Put(key, newValue, ver)
		if putErr = rpc.OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (sck *ShardCltler) executeMoves(oldConfig *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
	// A temporary map to store clerks for the duration of this function
	// using pointer instead copy clerk here, bc clerk variable changed instead of 
	// only read operation, groupID -> group clerk
	clerks := make(map[Tester.Tgid]*shardgrp.Clerk)

	// helper to get or create clerk
	getClerk := func(gid Tester.Tgid, config *shargrp.ShardConfig ) *shardcfg.Clerk {
		if ck, ok := clerks[gid]; ok {
			return ck
		}
		// If it doesn't exist, create it and store it
		servers := config.Groups[gid]
		ck := shardgrp.makeClerk(sck.clnt, servers)
		clerk[gid] = ck
		return ck
	}

	// i is shardID
	for i := 0; i < shardcfg.NShards; i++ {
		oldGrpID := oldConfig.Shards[i]
		newGrpID := new.Shards[i]
		oldGrpClerk := getClerk(i, oldConfig)
		newGrpClerk := getClerk(i, config)

		// shard's group not change or grp ID is 0(group no exist yet), ignore
		if oldGrpID == newGrpID || oldGrpID == 0{
			continue
		}
		
		// 1. else first freeze changed shards in old shardgrp
		// pass shardID and configNum ID
		data, err := oldGrpClerk.FreezeShard(i, oldConfig.Num)

		// 2. copy (install) the shard to the destination shardgrp
		err = newGrpClerk.InstallShard(i, data, conifg.Num)

		// 3. then delete the frozen shard
		err = oldGrpClerk.DeleteShard(i, oldConfig.Num)
	}

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.

	// STEP 1: PREPARE (Save the intent)
    // We store the "Next" config so if we crash, the next controller knows what to do
	// put new config as k/v pairs into kvsrv
	//err := sck.IKVClerk.Put("next-conifg", configString, 0)
	safeUpdate(nextConfigKey, newConfigString)

 	// STEP 2: EXECUTE (Move the data)
	oldConfig := sck.Query() // get the current/old config
	sck.executeMoves(oldConfig, new)

	// STEP 3: COMMIT / POST (Make it official)
    // By updating "current-config", you are telling the whole world the moves are done

	// convert confifgure to string
	newConfigString := new.String()

	// using "next-config" as key
	// (Ensure your Query() logic knows how to find this key later)
	// succeesed, update current-configure
	safeUpdate(currConfigKey, newConfigString)
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.

	// get version number from kvsrv and turn to ShardConfig
	val, _, err := sck.IKVClerk.Get(currConfigKey)
	if err != rpc.OK {
		log.Fatalf("no configure for current version")
	}
	return shardcfg.FromString(val)
}