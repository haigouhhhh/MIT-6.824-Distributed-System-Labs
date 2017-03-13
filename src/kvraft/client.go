package raftkv

import "../labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
	"sync/atomic"
)

var (
	ID uint64
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leader   int64
	serialNo uint64
	id       uint64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	ck.id = atomic.AddUint64(&ID, uint64(1))
	ck.serialNo = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
		ClientId: ck.id,
		SerialNo: ck.incrementSN(),
	}
	for {
		i := ck.getLeaderOrRandom()
		reply := GetReply{}
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
		if ok && reply.Err == "" && !reply.WrongLeader {
			ck.setLeader(i)
			return reply.Value
		} else {
			ck.invalidateLeader(i)
		}

	}
	return ""
}

func (ck *Clerk) incrementSN() uint64 {
	return atomic.AddUint64(&ck.serialNo, uint64(1))
}

func (ck *Clerk)getLeaderOrRandom() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	i := ck.leader
	if i == -1 {
		i = nrand() % int64(len(ck.servers))
	} else {
		DPrintf("leader is %v", i)
	}
	return i
}

func (ck *Clerk) setLeader(leader int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leader = leader

}

func (ck *Clerk) invalidateLeader(leader int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.leader == leader {
		ck.leader = -1
		DPrintf("invalidate leader %v", ck.leader)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.id,
		SerialNo: ck.incrementSN(),
	}
	DPrintf(" here: %+v", args)
	for {
		i := ck.getLeaderOrRandom()
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
		if ok && reply.Err == "" && !reply.WrongLeader {
			ck.setLeader(i)
			return
		} else {
			ck.invalidateLeader(i)
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
