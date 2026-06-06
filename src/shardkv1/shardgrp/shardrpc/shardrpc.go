package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardgrp"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	//State []byte
	Data map[string]shardgrp.DBValue
	LastOpResult map[int64]shardgrp.Result
	lastAppliedSeq map[int64]int
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	//State []byte
	Data map[string]shardgrp.DBValue
	LastOpResult map[int64]shardgrp.Result
	lastAppliedSeq map[int64]int
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
