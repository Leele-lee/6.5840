package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type Result struct {
	Value string
	Err rpc.Err
	Version rpc.Tversion
	Data map[string]DBValue
	LastOpResult map[int64]Result
	LastAppliedSeq map[int64]int
	Num shardcfg.Tnum
}

type DBValue struct {
	Value string
	Version rpc.Tversion
}

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
	Config shardcfg.ShardConfig
}

type FreezeShardReply struct {
	//State []byte
	Data map[string]DBValue
	LastOpResult map[int64]Result
	LastAppliedSeq map[int64]int
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	//State []byte
	Data map[string]DBValue
	LastOpResult map[int64]Result
	LastAppliedSeq map[int64]int
	Num   shardcfg.Tnum
	Config shardcfg.ShardConfig
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
	Config shardcfg.ShardConfig
}

type DeleteShardReply struct {
	Err rpc.Err
}
