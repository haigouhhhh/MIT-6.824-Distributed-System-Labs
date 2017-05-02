package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
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
	// Your data here.
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
	ck.servers = servers
	// Your code here.
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.leader = -1
	ck.id = atomic.AddUint64(&ID, uint64(1))
	ck.serialNo = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:num,
		ClientId:ck.id,
		SerialNo:ck.incrementSN(),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		ClientId:ck.id,
		SerialNo:ck.incrementSN(),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		ClientId:ck.id,
		SerialNo:ck.incrementSN(),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:shard,
		GID:gid,
		ClientId:ck.id,
		SerialNo:ck.incrementSN(),
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk)getLeaderOrRandom() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	i := ck.leader
	if i == -1 {
		i = nrand() % int64(len(ck.servers))
	}
	return i
}

func (ck *Clerk) incrementSN() uint64 {
	return atomic.AddUint64(&ck.serialNo, uint64(1))
}
