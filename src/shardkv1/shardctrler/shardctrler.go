package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"log"
	"time"
	"sync"
	"math/big"
	"crypto/rand"
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

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	//CurrConfigKeyNum shardgrp.Tnum // the latest configure version number
	//CurrConfigKey sharddcfg.ShardConfig
	mu sync.Mutex
	clerks map[tester.Tgid]*shardgrp.Clerk
	controllerID int64

}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
	sck.controllerID = nrand()
	return sck
}

func (sck *ShardCtrler) getClerk(gid tester.Tgid, config *shardcfg.ShardConfig) *shardgrp.Clerk {
	// helper to get or create clerk
		sck.mu.Lock()
		defer sck.mu.Unlock()
		if ck, ok := sck.clerks[gid]; ok {
			return ck
		}
		// If it doesn't exist, create it and store it
		servers := config.Groups[gid]
		ck := shardgrp.MakeClerk(sck.clnt, servers)
		sck.clerks[gid] = ck
		return ck
}


func (sck *ShardCtrler) checkConfigChange(new *shardcfg.ShardConfig) bool {
	currVal, _, _ := sck.IKVClerk.Get(CurrConfigKey)
    if shardcfg.FromString(currVal).Num >= new.Num {
        shardgrp.DPrintf("CONTROLLER: Config %d already committed. Done.", new.Num)
        return true 
    }

    // 2. CHECK SUPERSESSION (The "Failure" exit)
    // Ask kvsrv: "Is there a NEWER plan than mine?"
    nextVal, _, _ := sck.IKVClerk.Get(NextConfigKey)
    if shardcfg.FromString(nextVal).Num > new.Num {
        shardgrp.DPrintf("CONTROLLER: Plan %d superseded by %s. Exiting.", new.Num, nextVal)
        return true 
    }
	return false
}

// move shards from current(old) group to new group which contains freeze shard in old group, 
// install shard to new group, delete shard in old group
// returns true only if THIS controller finished moving shards
// and should attempt to commit CurrConfig.
func (sck *ShardCtrler) executeMoves(oldConfig *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
    // A temporary map to store clerks for the duration of this function
    // using pointer instead copy clerk here, bc clerk variable changed instead of 
    // only read operation, groupID -> group clerk
    shardgrp.DPrintf("CONTROLLER: ExecuteMoves start")

    //clerks := make(map[tester.Tgid]*shardgrp.Clerk)

    // i is shardID
    for i := shardcfg.Tshid(0); i < shardcfg.NShards; i++ {
        oldGrpID := oldConfig.Shards[i]
        newGrpID := new.Shards[i]
        oldGrpClerk := sck.getClerk(oldGrpID, oldConfig)
        newGrpClerk := sck.getClerk(newGrpID, new)

        // If there was an old group, we must Freeze it
        var data map[string]shardrpc.DBValue
        var lastOpResult map[int64]shardrpc.Result
        var lastAppliedSeq map[int64]int
        //err := rpc.OK

        // shard's group not change or grp ID is 0(group no exist yet), ignore
        if oldGrpID == newGrpID {
            continue
        }
        
        shardgrp.DPrintf("CONTROLLER: ExecuteMoves: shard %d move from old group %d to new group %d, from configNum %d to %d \n", i, oldGrpID, newGrpID, oldConfig.Num, new.Num)

        //maxRetries := 50

        //for retries := 0; retries < maxRetries; retries++ {
        for {
            //if retries == maxRetries - 1 {
            //  killed = true
            //}
            shardgrp.DPrintf("CONTROLLER: ExecuteMoves: keep trying move shard %d from old group %d to new group %d, from configNum %d to %d \n", i, oldGrpID, newGrpID, oldConfig.Num, new.Num)

			if sck.checkConfigChange(new) {
				return false
			}

            // 1. FREEZE PHASE
            if oldGrpID != 0 {
                d, r, s, err := oldGrpClerk.FreezeShard(i, new.Num)
                if err != rpc.OK {
                    // Network failed or server not leader. Sleep and try again.
                    time.Sleep(20 * time.Millisecond)
                    continue 
                }
                data, lastOpResult, lastAppliedSeq = d, r, s
            }
            // 2. INSTALL PHASE
            // We only get here if Freeze succeeded (or oldGrp was 0)
			if sck.checkConfigChange(new) {
				return false
			}
            if newGrpID != 0 {
                errInstall := newGrpClerk.InstallShard(i, data, lastOpResult, lastAppliedSeq, new.Num)
                if errInstall != rpc.OK {
                    // If Install failed, we MUST retry the whole process for this shard.
                    // Note: Since Freeze is idempotent, calling it again is safe.
                    time.Sleep(20 * time.Millisecond)
                    continue
                }
            }
            // 3. Delete phase
			if sck.checkConfigChange(new) {
				return false
			}
            if oldGrpID != 0 {
                errDelete := oldGrpClerk.DeleteShard(i, new.Num)
                if errDelete != rpc.OK {
                    time.Sleep(20 * time.Millisecond)
                    continue
                }
            }

            shardgrp.DPrintf("CONTROLLER: ExecuteMoves: Shard %d successfully moved to GID %d", i, newGrpID)
            break;
        }
    }
    return true
}

// get current version and put version in new put, if superseded by another controller 
// it will retry get version and put again
// key is "current-config" or "next-config", newValue is the string version config
func (sck *ShardCtrler) safeUpdate(key string, newValue string) {
	for {
		shardgrp.DPrintf("CONTROLLER %d: SafeUpdate: keep trying to set current value to key: %s, value: %s", sck.controllerID, key, newValue)

		targetNum := shardcfg.FromString(newValue).Num
		// 1. get current version, if key not exist, the version will be default 0
		val, ver, err := sck.IKVClerk.Get(key)
		if err != rpc.OK && err != rpc.ErrNoKey {
			// 如果网络报错，必须重试 Get，不能带着错误的 ver 去 Put
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// 2. INSTANT EXIT: If another controller already did our work, stop immediately!
        // This is the biggest time-saver in Part C.
        if val == newValue {
            return 
        }

		if err == rpc.OK {
            // CRITICAL: If the key is already at our version or NEWER, exit.
            if shardcfg.FromString(val).Num >= targetNum {
                return 
            }
        }

		// --- OPTIMIZATION (Important for idempotency) ---
        // If the value is ALREADY what we want, we can stop! 
        // This handles "Case A" (where we succeeded but got ErrMaybe).
        //if val == newValue {
        //    return 
        //}

		// 2. PUT: Attempt update
        // If Get returned version 0 (key doesn't exist), we send 0.
        // If Get returned version 5, we send 5.
		putErr := sck.IKVClerk.Put(key, newValue, ver)
		if putErr == rpc.OK {
			shardgrp.DPrintf("CONTROLLER %d: SafeUpdate: success to set key: %s value: to %s", sck.controllerID, key, newValue)

			return
		}

		// 4. SMART RETRY: If putErr is ErrVersion (mismatch), don't sleep!
        // It means another controller updated the key. Jump back to 'Get' immediately
        // to see if they set the value we wanted.
        if putErr == rpc.ErrVersion {
            continue 
        }

		time.Sleep(50 * time.Millisecond)
	}

}


// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
    for {
        // first check if there is a stored next configuration with 
        // a TASK higher configuration number than the current one
        nextStr, _, _ := sck.IKVClerk.Get(NextConfigKey)
        currStr, _, _ := sck.IKVClerk.Get(CurrConfigKey)

        if currStr == "" { return }

		currConfig := shardcfg.FromString(currStr)
		var nextConfig *shardcfg.ShardConfig

		if nextStr == "" {
            nextConfig = currConfig
        } else {
            nextConfig = shardcfg.FromString(nextStr)
        }

        //if nextStr != "" {
            //nextConfig := shardcfg.FromString(nextStr)
            // check config version num
        if nextConfig.Num > currConfig.Num {
            shardgrp.DPrintf("CONTROLLER %d: Recovering Config %d, curr config is %d", sck.controllerID, nextConfig.Num, currConfig.Num)
            // and if next config has a bigger config version, 
            // complete the shard moves necessary to reconfigure to the next one

            // 1. Re-run the moves (they must be idempotent!)
            if sck.executeMoves(currConfig, nextConfig) {
                // PRE-COMMIT CHECK (Part C safety)
                // Ensure no one else moved NextConfig forward while we worked
                checkNext, _, _ := sck.IKVClerk.Get(NextConfigKey)
                //if checkNext == nextStr {
                //    sck.safeUpdate(CurrConfigKey, nextStr)
                //    shardgrp.DPrintf("CONTROLLER %d: Successfully recovered and committed %d", sck.controllerID, nextConfig.Num)
                //}
				if checkNext != nextStr {
					return
				}
				sck.safeUpdate(CurrConfigKey, nextStr)
				shardgrp.DPrintf("CONTROLLER %d: Successfully recovered and committed %d", sck.controllerID, nextConfig.Num)

                // 2. FINALLY POST the new configuration
                //sck.safeUpdate(CurrConfigKey, nextStr)
            } else {
                // We were superseded or killed, stop recovering
                return
            }
        } else {
            // curr == next, everything is up to date
			// curr.Num == next.Num, the cluster is stable.
        	shardgrp.DPrintf("CONTROLLER %d: recover Cluster is stable at Config %d", sck.controllerID, currConfig.Num)
            return
        }

        // Small sleep to prevent CPU spin if we loop
        time.Sleep(50 * time.Millisecond)
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
	shardgrp.DPrintf("CONTROLLER: InitConfig: start has configNum %d", cfg.Num)


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

	oldConfig := sck.Query()
	newConfigString := new.String()
	for {
		shardgrp.DPrintf("CONTROLLER %d: changeConfigTo: start change configNum from %d to %d", sck.controllerID, oldConfig.Num, new.Num)

		// conditional put next-config make sure only one controller can start changeConfigTo in concurrent
		// situation(when all controller have the same configNum)

		// 1. Get the current version of the "NextConfigKey"
		val, ver, err := sck.IKVClerk.Get(NextConfigKey)

		// If network fails, retry the Get
		if err != rpc.OK && err != rpc.ErrNoKey {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// 2. If someone already posted a "Next" config that is >= ours, we lost the race!
		if err == rpc.OK {
			existingNext := shardcfg.FromString(val)

			if existingNext.Num > new.Num {
				shardgrp.DPrintf("CONTROLLER: exsit next config is %d. Lost race for Config %d, aborting",existingNext.Num, new.Num)
				return
			}

			if existingNext.Num == new.Num {
				// The plan is already exactly what we want! 
				// We can stop trying to Put and move to the Execution phase.
				// even the num is same but it could be from different controller
				if val != new.String() {
					shardgrp.DPrintf("CONTROLLER %d: Plan %d already posted by other controller, return.", sck.controllerID, new.Num)
					return
				}
				shardgrp.DPrintf("CONTROLLER %d: Plan %d already posted. Moving to execution.", sck.controllerID, new.Num)
				break 
			}
		}

		// 3. ATOMIC UPDATE: Try to post our plan
		// Use the version from step 1. If this fails, it means another controller 
		// won the race in the last millisecond.
		putErr := sck.IKVClerk.Put(NextConfigKey, newConfigString, ver)
		if putErr != rpc.OK {
			shardgrp.DPrintf("CONTROLLER %d: update nextconfigKey %s failed, err: %s", sck.controllerID, newConfigString, putErr)

			//time.Sleep(50 * time.Millisecond)
			continue
		} else {
			shardgrp.DPrintf("CONTROLLER %d: changeConfigTo: update nextconfigKey from %d to %d, %s success", sck.controllerID, oldConfig.Num, new.Num, newConfigString)
			break
		}
	}
		

	// STEP 1: PREPARE (Save the intent)
	// We store the "Next" config so if we crash, the next controller knows what to do
	// put new config as k/v pairs into kvsrv
	//err := sck.IKVClerk.Put("next-conifg", configString, 0)
		
	//sck.safeUpdate(NextConfigKey, newConfigString)

	// STEP 2: EXECUTE (Move the data)
	//oldConfig := sck.Query() // get the current/old config
	//successMove := sck.executeMoves(oldConfig, new)
	if sck.executeMoves(oldConfig, new) {
		checkNext, _, _ := sck.IKVClerk.Get(NextConfigKey)
		if checkNext != newConfigString {
			return
		}
		sck.safeUpdate(CurrConfigKey, new.String())
	}
	shardgrp.DPrintf("CONTROLLER %d: changeConfigTo: Config %d is now OFFICIAL (Current Config is now %d)", sck.controllerID, new.Num, new.Num)
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