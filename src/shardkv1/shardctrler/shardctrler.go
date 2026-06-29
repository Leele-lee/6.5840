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


func (sck *ShardCtrler) checkConfigChange(workingConfig *shardcfg.ShardConfig) bool {
	currVal, _, _ := sck.IKVClerk.Get(CurrConfigKey)
	nextVal, _, _ := sck.IKVClerk.Get(NextConfigKey)

	curr := shardcfg.FromString(currVal)
	next := shardcfg.FromString(nextVal)

	// 情况 A：任务已经正式完成了 (Committed)
    // 如果 CurrConfig 已经达到或超过了我们正在执行的版本，说明已经有人提交了
    if curr.Num >= workingConfig.Num {
        shardgrp.DPrintf("CONTROLLER: Config %d already committed. Done.", workingConfig.Num)
        return true
    }

    // 2. CHECK SUPERSESSION (The "Failure" exit)
    // Ask kvsrv: "Is there a NEWER plan than mine?"
	// 情况 B：任务被更新的计划取代了 (Superseded)
    // 如果墙上的 NextConfig 已经比我们现在执行的版本更高了，
    // 说明我们正在做“无用功”，应该立刻停止，回到主循环去执行那个更新的 Next。
    if next.Num > workingConfig.Num {
        shardgrp.DPrintf("CONTROLLER: Plan %d superseded by %s. Exiting.", workingConfig.Num, nextVal)
        return true 
    }
	return false
}

func (sck *ShardCtrler) moveOneShard(shardID shardcfg.Tshid, oldConfig *shardcfg.ShardConfig,
	 newConfig *shardcfg.ShardConfig) {
		oldGrpID := oldConfig.Shards[shardID]
        newGrpID := newConfig.Shards[shardID]

		var oldGrpClerk *shardgrp.Clerk
		if oldGrpID != 0 {
			oldServers := oldConfig.Groups[oldGrpID]
			oldGrpClerk = shardgrp.MakeClerk(sck.clnt, oldServers)
		}

		var newGrpClerk *shardgrp.Clerk
		if newGrpID != 0 {
			newServers := newConfig.Groups[newGrpID]
			newGrpClerk = shardgrp.MakeClerk(sck.clnt, newServers)
		}

		for {
			// 每一轮重试前，先检查集群是否已经达到了目标版本
        	// 如果别人已经帮我们把活干完了，直接退出
			if sck.checkConfigChange(newConfig) {
				return
			}
			var data map[string]shardrpc.DBValue
			var lastOpResult map[int64]shardrpc.Result
			var lastAppliedSeq map[int64]int

			// freeze
			if oldGrpID != 0 {
				d, r, s, err := oldGrpClerk.FreezeShard(shardID, newConfig.Num)
				if err != rpc.OK {
					// 如果失败（网络问题或不是 Leader），小睡后重试整个循环
					time.Sleep(30 * time.Millisecond)
					continue
				}
				data, lastOpResult, lastAppliedSeq = d, r, s
			}

			// 再次检查进度，防止在 Freeze 耗时过长期间配置已变更
        	if sck.checkConfigChange(newConfig) { return }
			// instsall
			if newGrpID != 0 {
				err := newGrpClerk.InstallShard(shardID, data, lastOpResult, lastAppliedSeq, newConfig.Num)
            	if err != rpc.OK {
                	// Install 失败必须重试。
               		// 注意：服务器端 shardgrp 必须处理好幂等性（基于 Num 判断）
                	time.Sleep(30 * time.Millisecond)
                	continue
            	}
        	}

			if sck.checkConfigChange(newConfig) { return }
			// delete
			if oldGrpID != 0 {
				err := oldGrpClerk.DeleteShard(shardID, newConfig.Num)
            	if err != rpc.OK {
                	time.Sleep(30 * time.Millisecond)
                	continue
				}
			}

			// 该分片所有步骤成功，安全退出协程
        	shardgrp.DPrintf("CONTROLLER: Shard %d successfully moved to Group %d (Config %d)", shardID, newGrpID, newConfig.Num)
			return
		}
}

// move shards from current(old) group to new group which contains freeze shard in old group, 
// install shard to new group, delete shard in old group
// executeMoves returns true only if THIS controller
// finished moving all shards and may attempt to
// commit CurrConfig.
//
// It returns false if:
//   - another controller already committed,
//   - a newer configuration superseded this one,
//   - or this controller abandoned the migration.
func (sck *ShardCtrler) executeMoves(oldConfig *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
    // A temporary map to store clerks for the duration of this function
    // using pointer instead copy clerk here, bc clerk variable changed instead of 
    // only read operation, groupID -> group clerk
    shardgrp.DPrintf("CONTROLLER: ExecuteMoves start")

	var wg sync.WaitGroup
	//done := make(chan struct{})

    // i is shardID
    for i := shardcfg.Tshid(0); i < shardcfg.NShards; i++ {
		// shard's group not change or grp ID is 0(group no exist yet), ignore
        if oldConfig.Shards[i] == new.Shards[i] {
            continue
        }
		wg.Add(1)

		go func(shardID shardcfg.Tshid) {
			defer wg.Done()
			sck.moveOneShard(shardID, oldConfig, new)
		}(i)
	}

	// 开启一个监控协程
    //go func() {
    //    wg.Wait()
    //    close(done)
    //}()

	    // 设置一个合理的单次配置变更最大时限（例如 30 秒）
    // 如果 30 秒还没迁完分片，这在分布式系统里通常意味着出了严重问题
    //select {
    //ase <-done:
    //    return !sck.checkConfigChange(new) // 正常完成
    //case <-time.After(30 * time.Second):
    //    shardgrp.DPrintf("CONTROLLER: executeMoves TIMEOUT! Breaking wg.Wait()")
    //    return false // 强制退出，让 Controller 回到主循环重试
    //}

	wg.Wait()
	return !sck.checkConfigChange(new)
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
        //if val == newValue {
         //   return 
        //}

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

        if nextConfig.Num > currConfig.Num {
			// 必须一直做，直到成功。不要因为 executeMoves 返回 false 就 return。
            shardgrp.DPrintf("CONTROLLER %d: Recovering Config %d, curr config is %d", sck.controllerID, nextConfig.Num, currConfig.Num)
            // and if next config has a bigger config version, 
            // complete the shard moves necessary to reconfigure to the next one

            // 1. Re-run the moves (they must be idempotent!)
			start := time.Now()
            if sck.executeMoves(currConfig, nextConfig) {
				shardgrp.DPrintf("CONTROLLER: initController: Config %d finished in time %v", nextConfig.Num, time.Since(start))
				sck.safeUpdate(CurrConfigKey, nextStr)
				shardgrp.DPrintf("CONTROLLER %d: Successfully recovered and committed %d", sck.controllerID, nextConfig.Num)
				// 成功了一次迁移，不要直接退出，循环回去看看还有没有更高版本的遗留任务
				continue
			}
			// 如果 executeMoves 失败了，休息一下继续试
            time.Sleep(100 * time.Millisecond)
            continue
        }
        // curr == next, everything is up to date
		// curr.Num == next.Num, the cluster is stable.
        shardgrp.DPrintf("CONTROLLER %d: recover Cluster is stable at Config %d", sck.controllerID, currConfig.Num)
        return
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

	//oldConfig := sck.Query()
	//newConfigString := new.String()
	// // This is an infinite loop; 
	// it only exits when CurrentConfig actually reaches the target version(new).
	// 这是一个死循环，直到 CurrentConfig 真的达到了目标版本才退出
	for {
		// get currenfigNum
		currConfig := sck.Query()
		shardgrp.DPrintf("CONTROLLER %d: changeConfigTo: start change configNum from %d to %d", sck.controllerID, currConfig.Num, new.Num)

		
		// 如果当前的实际版本已经达到或超过了 Tester 要求的版本，大功告成！
		if currConfig.Num >= new.Num {
			shardgrp.DPrintf("CONTROLLER %d: Done! Current %d >= Target %d", sck.controllerID, currConfig.Num, new.Num)
            return
		}

		// conditional put next-config make sure only one controller can start changeConfigTo in concurrent
		// situation(when all controller have the same configNum)

		// 1. Get the current version of the "NextConfigKey"
		// 2. 获取集群的“意图” (对应你的 NextConfigKey)
		val, ver, err := sck.IKVClerk.Get(NextConfigKey)

		var nextConfig *shardcfg.ShardConfig
		// 情况 A: 墙上没蓝图 (ErrNoKey) 
		// 或者 情况 B: 墙上的蓝图已经干完了 (existingNext.Num <= currConfig.Num)
		if err == rpc.ErrNoKey || shardcfg.FromString(val).Num <= currConfig.Num {
			// 我们把 Tester 给我们的 new 当作新的蓝图
			nextConfig = new

			putErr := sck.IKVClerk.Put(NextConfigKey, nextConfig.String(), ver)
			if putErr != rpc.OK {
				// 抢输了！说明别人刚好贴了一张蓝图上去。
        		// 没关系，直接 continue，下一轮循环就能读到别人贴的蓝图了。
				continue
			}
			 // 抢赢了！nextConfig 现在就是我们要执行的目标
			shardgrp.DPrintf("CONTROLLER %d: I won the race for NextConfig %d", sck.controllerID, nextConfig.Num)
		} else {
			// 墙上已经有一张还没干完的蓝图了 (Next.Num > Curr.Num)
    		// 哪怕这张蓝图不是我贴的，我也得认！
			nextConfig = shardcfg.FromString(val)
			shardgrp.DPrintf("CONTROLLER %d: Helping finish existing NextConfig %d", sck.controllerID, nextConfig.Num)
		}

		// 到这一步，nextConfig 一定是我们要执行的下一个目标
		// 接下来去执行迁移：
		if sck.executeMoves(currConfig, nextConfig) {
			// 迁移完了，把蓝图变成“已盖好的楼层
			sck.safeUpdate(CurrConfigKey, nextConfig.String())
            shardgrp.DPrintf("CONTROLLER %d: Config %d is now OFFICIAL", sck.controllerID, nextConfig.Num)
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