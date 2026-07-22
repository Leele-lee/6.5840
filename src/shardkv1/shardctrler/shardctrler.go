package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"log"
	"time"
	"sync"
	"sync/atomic"
	"math/big"
	crand "crypto/rand"
	"math/rand"
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
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu sync.Mutex
	controllerID int64

}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	sck.controllerID = nrand()
	return sck
}

// 当前存储的 NextConfig
// 必须等于我刚刚恢复的完整计划/已经完成的changeConfigTo——workingConfig
// 只有明确确认安全，才返回 true；
// 读取失败、不确定、状态变化，全部返回 false。
// return false when 已提交 被替代 临时错误
func (sck *ShardCtrler) mayCommit(workingConfig *shardcfg.ShardConfig) bool {
	currVal, _, err1 := sck.IKVClerk.Get(CurrConfigKey)
	nextVal, _, err2 := sck.IKVClerk.Get(NextConfigKey)

	// 如果因为partition连不上 kvsrv
	// this part actually no use, bc if partition the clerk will sleep retry 
	// instead of return
    if err1 != rpc.OK || err2 != rpc.OK {
        // 重点：我们返回 false，表示“不确定配置是否改变”。
        return false 
    }

	if currVal == "" || nextVal == "" {
		return false
	}

	curr := shardcfg.FromString(currVal)
	//next := shardcfg.FromString(nextVal)

	// 任务已经正式完成了 (Committed)
    // 如果 CurrConfig 已经达到或超过了我们正在执行的版本，说明已经有人提交了
    if curr.Num >= workingConfig.Num {
        shardgrp.DPrintf("CONTROLLER: Config %d already committed. Done.", workingConfig.Num)
        return false
    }

	// 必须还是同一份计划，而不仅是相同 Num
	// because different configNum can represent different config for controllers
	if nextVal != workingConfig.String() {
		return false
	}
	return true
}

// used to determine whether to proceed with moveOneShard
// return true: There is already clear evidence proving that I should stop.
// 1. Curr.Num >= working.Num
// 2. NextConfig: Key Differences
func (sck *ShardCtrler) shouldStop(expectedStr string, expectedNum shardcfg.Tnum) bool {
	currStr, _, currErr := sck.IKVClerk.Get(CurrConfigKey)

    if currErr == rpc.OK && currStr != "" {
        curr := shardcfg.FromString(currStr)

        // Another controller has already committed this
        // configuration or a newer one.
        if curr.Num >= expectedNum {
            return true
        }
    }	

	nextStr, _, nextErr := sck.IKVClerk.Get(NextConfigKey)

	if nextErr == rpc.OK && nextStr != "" {
		// A different complete plan now occupies NextConfig
		if nextStr != expectedStr {
            return true
        }
	}

	// Reading failed or the plan is still current.
    // We cannot prove that this controller should stop.
    return false
}


// called by executeMOves, move shardID from old group to new group, 
// seperatly call freezeShard -> installShard -> deleteShard rpc
// if receive rpc.ErrStale or rpc.ErrWrongGroup set aborted to true 
// if receive rpc.ErrMaybe and rpc.ErrRetry will sleep/continue
// if receive rpc.OK, can jump to the next phase
// check aborted at start of each loop, in case of other shards already stopped
func (sck *ShardCtrler) moveOneShard(shardID shardcfg.Tshid, oldConfig *shardcfg.ShardConfig,
	 newConfig *shardcfg.ShardConfig, expectedStr string, aborted *atomic.Bool) bool {
		oldGrpID := oldConfig.Shards[shardID]
        newGrpID := newConfig.Shards[shardID]

		 // 每个 goroutine 使用自己的 Clerk
		var oldGrpClerk *shardgrp.Clerk
		var newGrpClerk *shardgrp.Clerk

		if oldGrpID != 0 {
			oldServers := oldConfig.Groups[oldGrpID]
			oldGrpClerk = shardgrp.MakeClerk(sck.clnt, oldServers)
		}
		if newGrpID != 0 {
			newServers := newConfig.Groups[newGrpID]
			newGrpClerk = shardgrp.MakeClerk(sck.clnt, newServers)
		}

		var data map[string]shardrpc.DBValue
		var lastOpResult map[int64]shardrpc.Result
		var lastAppliedSeq map[int64]int

		// Freeze until success.
		if oldGrpID != 0 {
			for {
				if aborted.Load() {
					return false
				}
				d, r, s, err := oldGrpClerk.FreezeShard(shardID, newConfig.Num)

				switch err {
				case rpc.OK:
					data, lastOpResult, lastAppliedSeq = d, r, s
					goto installPhase

				case rpc.ErrAlreadyDone:
					// already freeze, install and delete for this shard
					return true

				case rpc.ErrStale, rpc.ErrWrongGroup:
					// 结束整个 executeMoves
					aborted.Store(true)
					return false

				case rpc.ErrRetry:
					// 配置检查 goroutine 会负责设置 aborted
					if aborted.Load() {
						return false
					}
					//time.Sleep(100 * time.Millisecond)
					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)
    				continue

				case rpc.ErrMaybe:
					// No extra watcher goroutine.
					// This call may block while this controller is partitioned.
					// Once reconnected, it can immediately observe that it was
					// superseded and exit.
					if sck.shouldStop(expectedStr, newConfig.Num) {
						aborted.Store(true)
						return false
					}

					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)
					continue	
				}
			}
		}
		
	installPhase:
		if newGrpID != 0 {
			for {
				if aborted.Load() {
					return false
				}
				err := newGrpClerk.InstallShard(shardID, data, lastOpResult, lastAppliedSeq, newConfig.Num)
				switch err {
				case rpc.OK:
					goto deletePhase

				case rpc.ErrStale, rpc.ErrWrongGroup:
					aborted.Store(true)
					return false

				case rpc.ErrRetry:
					// 配置检查 goroutine 会负责设置 aborted
					if aborted.Load() {
						return false
					}
					//time.Sleep(100 * time.Millisecond)
					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)

    				continue

				case rpc.ErrMaybe:
					// No extra watcher goroutine.
					// This call may block while this controller is partitioned.
					// Once reconnected, it can immediately observe that it was
					// superseded and exit.
					if sck.shouldStop(expectedStr, newConfig.Num) {
						aborted.Store(true)
						return false
					}

					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)
					continue
				}
			}
		}

	deletePhase:
		if oldGrpID != 0 {
			for {
				if aborted.Load() {
					return false
				}
				err := oldGrpClerk.DeleteShard(shardID, newConfig.Num)
				switch err {
				case rpc.OK:
					return true

				case rpc.ErrStale, rpc.ErrWrongGroup:
					aborted.Store(true)
					return false

				case rpc.ErrRetry:
					if aborted.Load() {
						return false
					}
					//time.Sleep(100 * time.Millisecond)
					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)

    				continue

				case rpc.ErrMaybe:
					// No extra watcher goroutine.
					// This call may block while this controller is partitioned.
					// Once reconnected, it can immediately observe that it was
					// superseded and exit.
					if sck.shouldStop(expectedStr, newConfig.Num) {
						aborted.Store(true)
						return false
					}

					time.Sleep(time.Duration(50 + rand.Intn(70)) * time.Millisecond)
					continue			
				}
			}
		}
		return true
}

// called by changeConfigTo and initControl
// move all changed group's shards from current(old) group to new group 
// freeze shard in old group, install shard to new group, delete shard in old group
// executeMoves returns true only if THIS controller finished moving all shards 
//
// It returns false if aborted is true

func (sck *ShardCtrler) executeMoves(
    oldConfig *shardcfg.ShardConfig,
    newConfig *shardcfg.ShardConfig,
	expectedStr string,
) bool {
    var aborted atomic.Bool

    for shardID := shardcfg.Tshid(0); shardID < shardcfg.NShards; shardID++ {

        if oldConfig.Shards[shardID] == newConfig.Shards[shardID] {
            continue
        }

		// The previous shard may have returned duplicate OK or
        // AlreadyDone. Before touching another shard, check whether
        // another controller has already completed the plan.
        if sck.shouldStop(expectedStr, newConfig.Num) {
            return false
        }

        if !sck.moveOneShard(
            shardID,
            oldConfig,
            newConfig,
			expectedStr,
            &aborted,
        ) {
            return false
        }
    }

    return true
}

// use CAS(compare and swap, version put)save currConfig or nextConfig to kvsrv
// direct return if currConfig/nextConfig's num is bigger than target num
// key is "current-config" or "next-config", newValue is the string version config

// Get 当前值和 key version
// → 条件 Put
// → 处理并发冲突或网络不确定性
// 当前代码会反复执行 Get → Put，在已有版本达到目标时退出，在 ErrVersion 时重新读取，在临时错误时等待后重试
func (sck *ShardCtrler) safeUpdate(key string, newValue string) {
	for {
		shardgrp.DPrintf("CONTROLLER %d: SafeUpdate: keep trying to set current value to key: %s, value: %s",
		 sck.controllerID, key, newValue)

		targetNum := shardcfg.FromString(newValue).Num
		// 1. get current version, if key not exist, the version will be default 0
		val, ver, err := sck.IKVClerk.Get(key)
		if err != rpc.OK && err != rpc.ErrNoKey {
			// 如果网络报错，必须重试 Get，不能带着错误的 ver 去 Put
			time.Sleep(time.Duration(50 + rand.Intn(100)) * time.Millisecond)
			continue
		}

		// 2. INSTANT EXIT: If another controller already did our work, stop immediately!
        // This is the biggest time-saver in Part C.
		if err == rpc.OK {
            // CRITICAL: If the key is already at our version or NEWER, exit.
            if shardcfg.FromString(val).Num >= targetNum {
                return 
            }
        }

		// 3. PUT: Attempt update
        // If Get returned version 0 (key doesn't exist), we send 0.
        // If Get returned version 5, we send 5.
		putErr := sck.IKVClerk.Put(key, newValue, ver)
		if putErr == rpc.OK {
			shardgrp.DPrintf("CONTROLLER %d: SafeUpdate: success to set key: %s value: to %s",
			 sck.controllerID, key, newValue)

			return
		}

		// 4. SMART RETRY: If putErr is ErrVersion (mismatch), don't sleep!
        // It means another controller updated the key. Jump back to 'Get' immediately
        // to see if they set the value we wanted.
        if putErr == rpc.ErrVersion {
            continue 
        }

		time.Sleep(time.Duration(50 + rand.Intn(100)) * time.Millisecond)
	}

}


// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.

// InitController() 的工作是恢复：
// CurrConfig < NextConfig

// 所代表的未完成迁移。
// 你的当前结构会反复读取 Curr 和 Next；
// 如果发现 Next.Num == Curr.Num+1，就恢复迁移并提交；成功后继续重新读取
func (sck *ShardCtrler) InitController() {
    for {
        // first check if there is a stored next configuration with 
        // a TASK higher configuration number than the current one
		// read next first and then curr
        nextStr, _, nextErr := sck.IKVClerk.Get(NextConfigKey)
        currStr, _, currErr := sck.IKVClerk.Get(CurrConfigKey)

		// in case of unreliable network cause return "", ErrTimeOut
        // Curr is required.
        //
        // ErrNoKey is abnormal after InitConfig, but there is
        // nothing this controller can recover without Curr.
        if currErr == rpc.ErrNoKey || currStr == "" {
            return
        }

		// Temporary Curr read failure: retry.
        if currErr != rpc.OK {
            time.Sleep(time.Duration(50+rand.Intn(70)) *time.Millisecond)
            continue
        }

        // No published recovery plan.
        if nextErr == rpc.ErrNoKey {
            return
        }

		// Temporary Next read failure: retry.
        if nextErr != rpc.OK {
            time.Sleep(time.Duration(50+rand.Intn(70)) *time.Millisecond)
            continue
        }

		if nextStr == "" {
            // rpc.OK with an empty value is malformed metadata.
            return
        }

		currConfig := shardcfg.FromString(currStr)
		nextConfig := shardcfg.FromString(nextStr)

		// Another controller already completed this plan.
        if nextConfig.Num <= currConfig.Num {
            return
        }

		// We can only recover the immediate next configuration.
        if nextConfig.Num != currConfig.Num+1 {
            // This can be a temporarily inconsistent observation.
            // Re-read both keys rather than acting on it.
            time.Sleep(time.Duration(50+rand.Intn(70)) *time.Millisecond)
            continue
        }

        // Recover the exact plan stored in NextConfig.
        if !sck.executeMoves(currConfig, nextConfig, nextStr) {
            time.Sleep(time.Duration(50+rand.Intn(70)) *time.Millisecond)
            continue
        }

        // Another controller may have committed while we migrated,
        // or the validation read may have failed.
        if !sck.mayCommit(nextConfig) {
            time.Sleep(time.Duration(50+rand.Intn(70)) *time.Millisecond)
            continue
        }

        sck.safeUpdate(CurrConfigKey, nextStr)

        // Re-read Curr/Next.
        //
        // Normally the next iteration observes:
        //     Curr.Num == Next.Num
        // and returns.
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

// getIntermediateConfig 这个函数的作用是确保系统的连续性。
// 它的核心逻辑是：“如果我要去的地方（Target）太远了，我必须先确定‘下一站’（v+1）在哪里。”
func (sck *ShardCtrler) getIntermediateConfig(curr *shardcfg.ShardConfig, target *shardcfg.ShardConfig) *shardcfg.ShardConfig {
	// 目标版本刚好就是当前版本的下一步
	if target.Num == curr.Num + 1 {
		return target
	}
	// 目标版本太远了 (例如 Curr=1, Target=5)
	if target.Num > curr.Num + 1{
		return nil
	}
	return nil
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// the controller call changConfigTo must one step at a time.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.

	// This is an infinite loop; 
	// it only exits when CurrentConfig actually reaches the target version(new).
	// 这是一个死循环，直到 CurrentConfig 真的达到了目标版本才退出
	for {
		// get currenfigNum
		currConfig := sck.Query()
		shardgrp.DPrintf("CONTROLLER %d: changeConfigTo: start change configNum from %d to %d",
		 sck.controllerID, currConfig.Num, new.Num)

		// 如果当前的实际版本已经达到或超过了 Tester 要求的版本，大功告成！
		if currConfig.Num >= new.Num {
			shardgrp.DPrintf("CONTROLLER %d: Done! Current %d >= Target %d",
			 sck.controllerID, currConfig.Num, new.Num)
            return
		}

		// conditional put next-config make sure only one controller can start changeConfigTo in concurrent
		// situation(when all controller have the same configNum)

		// 1. Get the current version of the "NextConfigKey"
		// 2. 获取集群的“意图” (对应你的 NextConfigKey)
		val, ver, err := sck.IKVClerk.Get(NextConfigKey)

		var stepConfig *shardcfg.ShardConfig
		// 情况 A: 墙上没蓝图 (ErrNoKey) 
		// 或者 情况 B: 墙上的蓝图已经干完了 (existingNext.Num <= currConfig.Num)
		if err == rpc.ErrNoKey || shardcfg.FromString(val).Num <= currConfig.Num {
			// 我们把 curr + 1 当作新的蓝图
			stepConfig = sck.getIntermediateConfig(currConfig, new)

			if stepConfig == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			putErr := sck.IKVClerk.Put(NextConfigKey, stepConfig.String(), ver)
			if putErr != rpc.OK {
				// 抢输了！说明别人刚好贴了一张蓝图上去。
        		// 没关系，直接 continue，下一轮循环就能读到别人贴的蓝图了。
				continue
			}
			 // 抢赢了！nextConfig 现在就是我们要执行的目标
			shardgrp.DPrintf("CONTROLLER %d: I won the race for NextConfig %d",
			 sck.controllerID, stepConfig.Num)
		} else {
			// 墙上已经有一张还没干完的蓝图了 (Next.Num > Curr.Num)
    		// 哪怕这张蓝图不是我贴的，如果符合next.Num = curr.Num + 1我也得认！
			nextConfig := shardcfg.FromString(val)
			stepConfig = sck.getIntermediateConfig(currConfig, nextConfig)
			if stepConfig == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			shardgrp.DPrintf("CONTROLLER %d: Helping finish existing NextConfig %d",
			 sck.controllerID, stepConfig.Num)
		}

		// 到这一步，nextConfig 一定是我们要执行的下一个目标
		// 接下来去执行迁移：
		if sck.executeMoves(currConfig, stepConfig, stepConfig.String()) {
			// 迁移完了，把蓝图变成“已盖好的楼层
			if sck.mayCommit(stepConfig) {
				sck.safeUpdate(CurrConfigKey, stepConfig.String())
				shardgrp.DPrintf("CONTROLLER %d: Config %d is now OFFICIAL", sck.controllerID, stepConfig.Num)
			}
		}
	}
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