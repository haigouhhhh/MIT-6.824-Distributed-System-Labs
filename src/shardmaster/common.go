package shardmaster

import (
	"bytes"
	"encoding/gob"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings

	ClientId uint64
	SerialNo uint64
}

func (arg *JoinArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Servers)

	return Op{
		Op:"JOIN",
		ArgValue:w.Bytes(),
		ClientId:arg.ClientId,
		SerialNo:arg.SerialNo,
	}

}

func (arg *JoinArgs) fromOP(op *Op) {
	r := bytes.NewBuffer(op.ArgValue)
	d := gob.NewDecoder(r)
	var servers map[int][]string

	d.Decode(&servers)

	arg.Servers = servers
	arg.SerialNo = op.SerialNo
	arg.ClientId = op.ClientId

}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int

	ClientId uint64
	SerialNo uint64
}

func (arg *LeaveArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.GIDs)

	return Op{
		Op:"LEAVE",
		ArgValue:w.Bytes(),
		ClientId:arg.ClientId,
		SerialNo:arg.SerialNo,
	}

}

func (arg *LeaveArgs) fromOP(op *Op) {

	r := bytes.NewBuffer(op.ArgValue)
	d := gob.NewDecoder(r)
	var gids []int

	d.Decode(&gids)

	arg.GIDs = gids
	arg.SerialNo = op.SerialNo
	arg.ClientId = op.ClientId

}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int

	ClientId uint64
	SerialNo uint64
}

func (arg *MoveArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Shard)
	e.Encode(arg.GID)

	return Op{
		Op:"MOVE",
		ArgValue:w.Bytes(),
		ClientId:arg.ClientId,
		SerialNo:arg.SerialNo,
	}

}

func (arg *MoveArgs) fromOP(op *Op) {

	r := bytes.NewBuffer(op.ArgValue)
	d := gob.NewDecoder(r)
	var shard int
	var gid int

	d.Decode(&shard)
	d.Decode(&gid)

	arg.Shard = shard
	arg.GID = gid
	arg.SerialNo = op.SerialNo
	arg.ClientId = op.ClientId

}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number

	ClientId uint64
	SerialNo uint64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (arg *QueryArgs) toOP() Op {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(arg.Num)

	return Op{
		Op:"QUERY",
		ArgValue:w.Bytes(),
		ClientId:arg.ClientId,
		SerialNo:arg.SerialNo,
	}

}

func (arg *QueryArgs) fromOP(op *Op) {

	r := bytes.NewBuffer(op.ArgValue)
	d := gob.NewDecoder(r)

	var num int
	d.Decode(&num)

	arg.Num = num
	arg.SerialNo = op.SerialNo
	arg.ClientId = op.ClientId

}
