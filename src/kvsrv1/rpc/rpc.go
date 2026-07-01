package rpc

import (
	"6.5840/shardkv1/shardcfg"
)

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	// for shardsrv delay
	ErrRetry = "ErrRetry"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	ClientID int64
	SeqNum int
	ConfigNum shardcfg.Tnum
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientID int64
	SeqNum int
	ConfigNum shardcfg.Tnum
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

