package shardmaster

import "../raft"
import "../labrpc"
import "sync"
import "encoding/gob"
import (
	"github.com/serialx/hashring"
	"strconv"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}

type ShardMaster struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg

				// Your data here.
	clientTableMap map[uint64]*ClientTable
	waitMap        map[int]OpResult

	configs        []Config // indexed by config num
}

type OpResult struct {
	opPtr  *Op
	result chan interface{}
	err    chan Err
}

type ClientTable struct {
	mu           sync.Mutex
	lastSerialNo uint64
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	ArgValue []byte
	ClientId uint64
	SerialNo uint64
}

func (op *Op) equal(op2 *Op) bool {
	return op.ClientId == op2.ClientId && op.SerialNo == op2.SerialNo

}
func (op *Op) exec(shardMt *ShardMaster) (result interface{}) {
	shardMt.mu.Lock()
	defer shardMt.mu.Unlock()

	newNum := len(shardMt.configs)

	switch op.Op{

	case "JOIN":

		newGroups := make(map[int][]string)
		oldGroups := make(map[int][]string)

		var oldShards [NShards]int

		DPrintf("JOIN, rf %v, config size %v", shardMt.me, len(shardMt.configs))
		if len(shardMt.configs) > 0 {
			oldGroups = shardMt.configs[len(shardMt.configs) - 1].Groups
			oldShards = shardMt.configs[len(shardMt.configs) - 1].Shards
			for k, v := range oldGroups {
				newGroups[k] = v
			}
		}
		//TODO validate unique key?
		var servers map[int][]string

		arg := &JoinArgs{}
		arg.fromOP(op)
		servers = arg.Servers

		for k, v := range servers {
			newGroups[k] = v
		}

		var gids []int

		for k := range servers {
			if _, has := oldGroups[k]; !has {
				gids = append(gids, k)
			}
		}
		newShards := shardMt.addGroup(oldShards, gids)
		DPrintf(" rf %v, groups %v", shardMt.me, newGroups)

		shardMt.configs = append(shardMt.configs, Config{
			Num: newNum,
			Shards:newShards,
			Groups:newGroups,
		})


	case "LEAVE":
		newGroups := make(map[int][]string)
		var oldShards [NShards]int
		DPrintf("LEAVE, rf %v, config size %v", shardMt.me, len(shardMt.configs))
		if len(shardMt.configs) > 0 {
			oldGroups := shardMt.configs[len(shardMt.configs) - 1].Groups
			oldShards = shardMt.configs[len(shardMt.configs) - 1].Shards
			for k, v := range oldGroups {
				newGroups[k] = v
			}
		} else {
			panic("configs are empty")
		}

		//TODO
		var leaveGids []int
		arg := &LeaveArgs{}
		arg.fromOP(op)
		leaveGids = arg.GIDs

		for _, gid := range leaveGids {
			delete(newGroups, gid)
		}

		var gids []int

		for _, k := range leaveGids {
			gids = append(gids, k)
		}
		newShards := shardMt.removeGroup(oldShards, gids, newGroups)

		shardMt.configs = append(shardMt.configs, Config{
			Num: newNum,
			Shards:newShards,
			Groups:newGroups,
		})

	case "MOVE":
		var moveShard int
		var moveGID int

		arg := &MoveArgs{}
		arg.fromOP(op)
		moveShard = arg.Shard
		moveGID = arg.GID

		newGroups := make(map[int][]string)
		var newShards [NShards]int
		if len(shardMt.configs) > 0 {
			oldGroups := shardMt.configs[len(shardMt.configs) - 1].Groups
			for k, v := range oldGroups {
				newGroups[k] = v
			}

			oldShards := shardMt.configs[len(shardMt.configs) - 1].Shards
			for i, v := range oldShards {
				newShards[i] = v
			}

		} else {
			panic("configs are empty")
		}

		newShards[moveShard] = moveGID

		shardMt.configs = append(shardMt.configs, Config{
			Num: newNum,
			Shards:newShards,
			Groups:newGroups,
		})
	case "QUERY":
		var num int
		arg := &QueryArgs{}
		arg.fromOP(op)
		num = arg.Num
		return shardMt.getConfig(num)

	}
	return nil

}

func (sm *ShardMaster) getConfig(num int) Config {
	var config Config = Config{};
	if num == -1 || num >= len(sm.configs) {
		config = sm.configs[len(sm.configs) - 1]

	} else {
		config = sm.configs[num]

	}
	return config

}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	wrongLeader, err, _ := sm.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	wrongLeader, err, _ := sm.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	wrongLeader, err, _ := sm.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	wrongLeader, err, value := sm.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
	if config, ok := value.(Config); ok {
		reply.Config = config
	}

}

func (sm *ShardMaster) checkRaftState() (err Err) {
	_, isLeader := sm.rf.GetState()

	if !isLeader {
		err = "not leader"
	}
	return ""

}
func (sm *ShardMaster) waitForApply(op Op) (wrongLeader bool, returnErr Err, value interface{}) {

	if err := sm.checkRaftState(); err != "" {
		returnErr = err
		wrongLeader = true
		return
	}

	if val, hitCache := sm.getFromCache(&op); hitCache {
		value = val
		return

	}

	index, originalTerm, isLeader := sm.rf.Start(op)
	if !isLeader {
		wrongLeader = true
		return
	}

	sm.mu.Lock()
	if existing, has := sm.waitMap[index]; has {
		go func() {
			existing.err <- "newer command with same index"
		}()
	}
	cur := OpResult{
		opPtr: &op,
		err: make(chan Err),
		result:make(chan interface{}),
	}
	sm.waitMap[index] = cur
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if curOp, has := sm.waitMap[index]; has && cur.opPtr.equal(curOp.opPtr) {
			delete(sm.waitMap, index)
		}
	}()

	for {
		select {
		case err := <-cur.err:
			returnErr = err
			wrongLeader = true
			return
		case val := <-cur.result:
			wrongLeader = false
			value = val
			return

		case _ = <-time.After(time.Millisecond * 500):
			term, isLeader := sm.rf.GetState()
			if !isLeader || term != originalTerm {
				wrongLeader = true
				returnErr = "no longer leader"
				return
			}
		}
	}

}

func (sm *ShardMaster) getFromCache(op *Op) (cacheVal interface{}, hitCache bool) {
	sm.mu.Lock()
	clientTable, has := sm.clientTableMap[op.ClientId]
	sm.mu.Unlock()

	if has {
		clientTable.mu.Lock()
		hitCache = clientTable.lastSerialNo >= op.SerialNo
		clientTable.mu.Unlock()

	}

	if hitCache {
		cacheVal = op.getFromCache(sm)
	}
	return

}

func (op *Op) getFromCache(sm *ShardMaster) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch op.Op {
	case "QUERY":
		var num int
		return sm.getConfig(num)
	}
	return nil

}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) rebalance(groups []int) [NShards]int {
	var groups_str []string;
	for _, gid := range groups {
		groups_str = append(groups_str, "gid:" + strconv.Itoa(gid))
	}
	ring := hashring.New(groups_str)

	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shard_str := "shard:" + strconv.Itoa(i) + strconv.Itoa(i)
		server_str, _ := ring.GetNode(shard_str);
		gid, err := strconv.Atoi(server_str[len("gid:"):])
		if err != nil {
			panic(err)
		}
		shards[i] = gid

	}

	return shards
}

func (sm *ShardMaster) addGroup(oldShards [NShards]int, groups []int) [NShards]int {
	DPrintf(" rf%v, add groups: %v", sm.me, groups)

	if len(groups) == 0 {
		return oldShards
	}

	count := make(map[int]int)

	for _, gid := range oldShards {
		if gid != 0 {
			count[gid] = count[gid] + 1
		}
	}

	averageShardPerGroup := NShards / (len(count) + len(groups))

	var shardsPool[]int

	var newShards [NShards]int
	for shard, gid := range oldShards {
		if gid == 0 || ( count[gid] > averageShardPerGroup && len(shardsPool) < len(groups) * averageShardPerGroup) {
			shardsPool = append(shardsPool, shard)
			if _, has := count[gid]; has {
				count[gid] = count[gid] - 1
			}
		} else {
			newShards[shard] = gid
		}
	}

	if len(shardsPool) < len(groups) * averageShardPerGroup {
		panic("something wrong with shards pool")
	}

	for i, shard := range shardsPool {
		newShards[shard] = groups[i % len(groups)]
	}
	DPrintf("after addGroup, rf %v, new shards: %v", sm.me, newShards)

	return newShards
}

func (sm *ShardMaster) removeGroup(shards [NShards]int, removeGids []int, groups map[int][]string) [NShards]int {
	DPrintf("rf %v, remove groups: %v, original shards %v, new groups %v", sm.me, removeGids, shards, groups)

	delGroupMap := make(map[int]bool)

	for _, delG := range removeGids {
		delGroupMap[delG] = true
	}

	existingGid := make(map[int]bool)
	for shard, gid := range shards {
		if gid == 0 {
			panic("gid should not be 0 in removeGroup")
		}
		if delGroupMap[gid] {
			shards[shard] = 0
		} else {
			existingGid[gid] = true
		}
	}

	var candidateGids []int

	for gid := range existingGid {
		candidateGids = append(candidateGids, gid)
	}

	for gid := range groups {
		if len(candidateGids) < NShards {
			if !existingGid[gid] {
				candidateGids = append(candidateGids, gid)
			}
		}
	}

	// no groups left
	if len(candidateGids) == 0 {
		return shards
	}

	averageShardPerGroup := NShards / len(candidateGids)

	count := make(map[int]int)

	var shardsPool []int
	var carry int
	for shard, gid := range shards {
		if gid == 0 {
			shardsPool = append(shardsPool, shard)
			continue
		}
		if count[gid] < averageShardPerGroup {
			count[gid] = count[gid] + 1
		} else if count[gid] == averageShardPerGroup && ( NShards > averageShardPerGroup * len(candidateGids) + carry) {
			carry += 1
			count[gid] = count[gid] + 1
		} else {
			shardsPool = append(shardsPool, shard)
			shards[shard] = 0
		}
	}
	for _, shard := range shardsPool {
		for _, gid := range candidateGids {
			shardCount := count[gid]
			if shardCount < averageShardPerGroup {
				shards[shard] = gid
				count[gid] = count[gid] + 1
				break
			} else if shardCount == averageShardPerGroup && (NShards > averageShardPerGroup * len(candidateGids) + carry) {
				shards[shard] = gid
				count[gid] = count[gid] + 1
				carry += 1
				break
			}

		}
	}

	return shards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitMap = make(map[int]OpResult)
	sm.clientTableMap = make(map[uint64]*ClientTable)

	go sm.listenOnApplyCh(sm.applyCh)

	return sm
}

func (sm *ShardMaster) listenOnApplyCh(applyCh chan raft.ApplyMsg) {
	// Your code here.
	for applyMsg := range applyCh {
		op := applyMsg.Command.(Op)
		opPtr := &op

		var val interface{}
		if cacheVal, hitCache := sm.getFromCache(opPtr); hitCache {
			val = cacheVal

		} else {
			val = opPtr.exec(sm)
		}

		sm.mu.Lock()
		if opResult, has := sm.waitMap[applyMsg.Index]; has {
			sameOp := opResult.opPtr.equal(opPtr)
			go func() {
				if sameOp {
					opResult.result <- val
				} else {
					opResult.err <- "error, same index but different op"
				}
			}()
		}
		clientTable, has := sm.clientTableMap[opPtr.ClientId]
		sm.mu.Unlock()

		if has {
			clientTable.mu.Lock()
			if clientTable.lastSerialNo < opPtr.SerialNo {
				clientTable.lastSerialNo = opPtr.SerialNo
			}
			clientTable.mu.Unlock()

		} else {
			sm.mu.Lock()
			sm.clientTableMap[opPtr.ClientId] = &ClientTable{
				lastSerialNo: opPtr.SerialNo,
			}
			sm.mu.Unlock()
		}

	}
}
