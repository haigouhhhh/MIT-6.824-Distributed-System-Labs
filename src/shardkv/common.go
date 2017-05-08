package shardkv

import (
	"bytes"
	"encoding/gob"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK = "OK"
	ErrNoKey = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

const (
	KVCmd = "KVCmd"
	ShardConfigCmd = "ConfigCmd"
)

type Err string

// Put or Append
type PutAppendArgs struct {
			// You'll have to add definitions here.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
			// You'll have to add definitions here.
			// Field names must start with capital letters,
			// otherwise RPC will break.
	ClientId uint64
	SerialNo uint64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key      string
	// You'll have to add definitions here.
	ClientId uint64
	SerialNo uint64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type AddShardsArgs struct {
	ConfigNum int
	Shards    []int
}

func (arg *AddShardsArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Shards)

	return Op{
		Type:ShardConfigCmd,
		ConfigNum: arg.ConfigNum,
		ConfigCmd: "ADD_SHARDS",
		ConfigCmdArgs: w.Bytes(),
	}

}

func (arg *AddShardsArgs) fromOP(op *Op) {
	r := bytes.NewBuffer(op.ConfigCmdArgs)
	d := gob.NewDecoder(r)
	var shards []int

	d.Decode(&shards)

	arg.ConfigNum = op.ConfigNum
	arg.Shards = shards

}

type AddShardsReply struct {
	WrongLeader bool
	Err         Err
}

type RemoveShardsArgs struct {
	ConfigNum int
	Shards    []int
}

func (arg *RemoveShardsArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Shards)

	return Op{
		Type:ShardConfigCmd,
		ConfigNum: arg.ConfigNum,
		ConfigCmd: "REMOVE_SHARDS",
		ConfigCmdArgs: w.Bytes(),
	}

}

func (arg *RemoveShardsArgs) fromOP(op *Op) {
	r := bytes.NewBuffer(op.ConfigCmdArgs)
	d := gob.NewDecoder(r)
	var shards []int

	d.Decode(&shards)

	arg.ConfigNum = op.ConfigNum
	arg.Shards = shards

}

type RemoveShardsReply struct {
	WrongLeader bool
	Err         Err
}

type MoveShardsArgs struct {
	ConfigNum    int
	Shards       []int
	Gid          int
	GroupServers []string
}

func (arg *MoveShardsArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Shards)
	e.Encode(arg.Gid)
	e.Encode(arg.GroupServers)

	return Op{
		Type:ShardConfigCmd,
		ConfigNum: arg.ConfigNum,
		ConfigCmd: "MOVE_SHARDS",
		ConfigCmdArgs: w.Bytes(),
	}

}

func (arg *MoveShardsArgs) fromOP(op *Op) {
	r := bytes.NewBuffer(op.ConfigCmdArgs)
	d := gob.NewDecoder(r)
	var shards []int
	var gid int
	var groups []string

	d.Decode(&shards)
	d.Decode(&gid)
	d.Decode(&groups)

	arg.ConfigNum = op.ConfigNum
	arg.Shards = shards
	arg.Gid = gid
	arg.GroupServers = groups

}

type MoveShardsReply struct {
	WrongLeader bool
	Err         Err
}

type CopyShardsArgs struct {
	ConfigNum      int
	Shards         []int
	Gid            int
	KVData         map[string]string
	ClientSerialNo map[uint64]uint64
}

func (arg *CopyShardsArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Shards)
	e.Encode(arg.Gid)
	e.Encode(arg.KVData)
	e.Encode(arg.ClientSerialNo)

	return Op{
		Type:ShardConfigCmd,
		ConfigNum: arg.ConfigNum,
		ConfigCmd: "COPY_SHARDS",
		ConfigCmdArgs: w.Bytes(),
	}

}

func (arg *CopyShardsArgs) fromOP(op *Op) {
	r := bytes.NewBuffer(op.ConfigCmdArgs)
	d := gob.NewDecoder(r)
	var shards []int
	var gid int
	var kvData        map[string]string
	var clientSerialNo map[uint64]uint64

	d.Decode(&shards)
	d.Decode(&gid)
	d.Decode(&kvData)
	d.Decode(&clientSerialNo)

	arg.ConfigNum = op.ConfigNum
	arg.Shards = shards
	arg.Gid = gid
	arg.KVData = kvData
	arg.ClientSerialNo = clientSerialNo

}

type CopyShardsReply struct {
	WrongLeader bool
	Err         Err
}
