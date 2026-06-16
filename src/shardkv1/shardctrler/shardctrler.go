package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"log"
	"time"
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

const (
	CurrConfigKey = "current-config"
	NextConfigKey = "next-config"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	//CurrConfigKeyNum shardgrp.Tnum // the latest configure version number
	//CurrConfigKey sharddcfg.ShardConfig
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) executeMoves(oldConfig *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	// A temporary map to store clerks for the duration of this function
	// using pointer instead copy clerk here, bc clerk variable changed instead of 
	// only read operation, groupID -> group clerk
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)

	// helper to get or create clerk
	getClerk := func(gid tester.Tgid, config *shardcfg.ShardConfig) *shardgrp.Clerk {
		if ck, ok := clerks[gid]; ok {
			return ck
		}
		// If it doesn't exist, create it and store it
		servers := config.Groups[gid]
		ck := shardgrp.MakeClerk(sck.clnt, servers)
		clerks[gid] = ck
		return ck
	}
	faile := false

	// i is shardID
	for i := shardcfg.Tshid(0); i < shardcfg.NShards; i++ {
		oldGrpID := oldConfig.Shards[i]
		newGrpID := new.Shards[i]
		oldGrpClerk := getClerk(oldGrpID, oldConfig)
		newGrpClerk := getClerk(newGrpID, new)

		// If there was an old group, we must Freeze it
        var data map[string]shardrpc.DBValue
        var lastOpResult map[int64]shardrpc.Result
        var lastAppliedSeq map[int64]int
		//err := rpc.OK

		// shard's group not change or grp ID is 0(group no exist yet), ignore
		if oldGrpID == newGrpID {
			continue
		}
		
		shardgrp.DPrintf("ExecuteMoves: shard %d move from old group %d to new group %d, from configNum %d to %d \n", i, oldGrpID, newGrpID, oldConfig.Num, new.Num)

		maxRetries := 50

		for retries := 0; retries < maxRetries; retries++ {
			if retries == maxRetries - 1 {
				faile = true
			}
			// 1. FREEZE PHASE
			if oldGrpID != 0 {
				d, r, s, err := oldGrpClerk.FreezeShard(i, new.Num)
				if err != rpc.OK {
					// Network failed or server not leader. Sleep and try again.
					time.Sleep(100 * time.Millisecond)
					continue 
				}
				data, lastOpResult, lastAppliedSeq = d, r, s
			}
			// 2. INSTALL PHASE
			// We only get here if Freeze succeeded (or oldGrp was 0)
			if newGrpID != 0 {
				//errInstall := getClerk(newGrpID, new).InstallShard(i, data, lastOpResult, lastAppliedSeq, new.Num)
				errInstall := newGrpClerk.InstallShard(i, data, lastOpResult, lastAppliedSeq, new.Num)
				if errInstall != rpc.OK {
					// If Install failed, we MUST retry the whole process for this shard.
					// Note: Since Freeze is idempotent, calling it again is safe.
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}
			// 3. Delete phase
			if oldGrpID != 0 {
				errDelete := oldGrpClerk.DeleteShard(i, new.Num)
				if errDelete != rpc.OK {
					time.Sleep(100 * time.Millisecond)
					continue
				}
			}

			shardgrp.DPrintf("ExecuteMoves: Shard %d successfully moved to GID %d", i, newGrpID)
			break;
		}
	}
	return faile
}

// get current version and put version in new put, if superseded by another controller 
// it will retry get version and put again
func (sck *ShardCtrler) safeUpdate(key string, newValue string) {
	for {
		// 1. get current version, if key not exist, the version will be default 0
		val, ver, err := sck.IKVClerk.Get(key)
		if err != rpc.OK && err != rpc.ErrNoKey {
			// 如果网络报错，必须重试 Get，不能带着错误的 ver 去 Put
			time.Sleep(100 * time.Millisecond)
			continue
		}

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
		if putErr == rpc.OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

}


// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// first check if there is a stored next configuration with 
	// a TASK higher configuration number than the current one
	nextStr, _, _ := sck.IKVClerk.Get(NextConfigKey)
	currStr, _, _ := sck.IKVClerk.Get(CurrConfigKey)

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
			sck.safeUpdate(CurrConfigKey, nextStr)
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
	sck.safeUpdate(key, configString)
	sck.safeUpdate(CurrConfigKey, configString)
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
	newConfigString := new.String()
	sck.safeUpdate(NextConfigKey, newConfigString)

 	// STEP 2: EXECUTE (Move the data)
	oldConfig := sck.Query() // get the current/old config
	killed := sck.executeMoves(oldConfig, new)

	// STEP 3: COMMIT / POST (Make it official)
    // By updating "current-config", you are telling the whole world the moves are done

	// using "next-config" as key
	// (Ensure your Query() logic knows how to find this key later)
	// succeesed, update current-configure
	if killed {
		return
	}
	sck.safeUpdate(CurrConfigKey, newConfigString)
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.

	// get version number from kvsrv and turn to ShardConfig
	val, _, err := sck.IKVClerk.Get(CurrConfigKey)
	if err != rpc.OK {
		log.Fatalf("no configure for current version")
	}
	return shardcfg.FromString(val)
}