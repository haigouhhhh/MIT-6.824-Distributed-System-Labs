package shardkv


// import "shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import (
	"encoding/gob"
	"fmt"
	"time"
	"strings"
	"bytes"
	"reflect"
)

type ShardKV struct {
	mu                sync.Mutex
	me                int
	rf                *raft.Raft
	applyCh           chan raft.ApplyMsg
	make_end          func(string) *labrpc.ClientEnd
	gid               int
	masters           []*labrpc.ClientEnd
	maxraftstate      int // snapshot if log grows this big

			      // Your definitions here.
	responsibleShards map[int]bool
	shardsConfigNum   map[int]int

	kvPair            map[string]string
	waitMap           map[int]OpResult

	clientTableMap    map[uint64]*ClientTable
}

func allSmall(vals[]int, target int) bool {
	for _, val := range vals {
		if val >= target {
			return false
		}

	}
	return true;

}

func anyLarge(vals[]int, target int) bool {
	for _, val := range vals {
		if val > target {
			return true
		}

	}
	return false;

}

func allEqual(vals[]int, target int) bool {
	for _, val := range vals {
		if val != target {
			return false
		}

	}
	return true;

}

func (kv *ShardKV) alreadySeeShardConfig(shards[]int, curConfigNum int, canRepeat bool) bool {
	var configNums []int

	for _, shard := range shards {
		configNums = append(configNums, kv.shardsConfigNum[shard])
	}

	if allSmall(configNums, curConfigNum) {
		return false
	}
	if anyLarge(configNums, curConfigNum) {
		return true
	}

	if allEqual(configNums, curConfigNum) {
		return !canRepeat
	}
	panic("werid")
}

func (kv *ShardKV) alreadySee(op *Op) bool {
	var shards []int
	var configNum int
	switch op.ConfigCmd {
	case "ADD_SHARDS":
		arg := &AddShardsArgs{}
		arg.fromOP(op)
		shards = arg.Shards
		configNum = arg.ConfigNum

	case "MOVE_SHARDS":
		arg := &MoveShardsArgs{}
		arg.fromOP(op)
		shards = arg.Shards
		configNum = arg.ConfigNum

	case "COPY_SHARDS":
		arg := &CopyShardsArgs{}
		arg.fromOP(op)
		shards = arg.Shards
		configNum = arg.ConfigNum

	case "REMOVE_SHARDS":
		arg := &RemoveShardsArgs{}
		arg.fromOP(op)
		shards = arg.Shards
		configNum = arg.ConfigNum

	default:
		panic("why here?")
	}
	return kv.alreadySeeShardConfig(shards, configNum, "REMOVE_SHARDS" == op.ConfigCmd)

}

func (kv *ShardKV) MoveShards(args *MoveShardsArgs, reply *MoveShardsReply) {
	// Your code here.
	wrongLeader, err, _ := kv.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (kv *ShardKV) CopyShards(args *CopyShardsArgs, reply *CopyShardsReply) {
	// Your code here.
	wrongLeader, err, _ := kv.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (kv *ShardKV) AddShards(args *AddShardsArgs, reply *AddShardsReply) {
	// Your code here.
	wrongLeader, err, _ := kv.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (kv *ShardKV) RemoveShards(args *RemoveShardsArgs, reply *RemoveShardsReply) {
	// Your code here.
	DPrint2("gid %v remove shards", kv.gid)
	wrongLeader, err, _ := kv.waitForApply(args.toOP())
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvPair = make(map[string]string)
	kv.waitMap = make(map[int]OpResult)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//if (kv.gid ==101 && Debug <0){
	//	kv.rf.Debug =1
	//}
	kv.clientTableMap = make(map[uint64]*ClientTable)

	kv.responsibleShards = make(map[int]bool)
	kv.shardsConfigNum = make(map[int]int)

	// You may need initialization code here.
	kv.restoreFromSnapShot(kv.rf.ReadSnapShot(persister.ReadSnapshot()))

	go kv.listenOnApplyCh(kv.applyCh)

	return kv
}



//
//func (watcher *ShardKV) watch() {
//
//	for true {
//		newConfig := watcher.sm.Query(-1)
//		if newConfig.Num > watcher.config.Num {
//			addGroups := make(map[int][]int)
//			removeGroups := make(map[int][]int)
//			moveGroups := make(map[int]map[int][]int)
//
//			for i, newGid := range newConfig.Shards {
//				oldGid := watcher.config.Shards[i]
//				if newGid != oldGid {
//					if newGid == 0 {
//						removeGroups[oldGid] = append(removeGroups[oldGid], i)
//					} else if oldGid == 0 {
//						addGroups[newGid] = append(addGroups[newGid], i)
//					} else {
//						if _, has := moveGroups[oldGid]; !has {
//							moveGroups[oldGid] = make(map[int][]int)
//						}
//						moveGroups[oldGid][newGid] = append(moveGroups[oldGid][newGid], i)
//					}
//
//				}
//
//			}
//
//			for gid, shards := range addGroups {
//				watcher.addShards(newConfig, shards, gid)
//			}
//
//			for gid, shards := range removeGroups {
//
//				watcher.removeShards(newConfig, shards, gid)
//			}
//			for gid, peers := range moveGroups {
//				for newGid, shards := range peers {
//					watcher.moveShards(newConfig, shards, gid, newGid)
//					watcher.removeShards(newConfig, shards, gid)
//				}
//			}
//
//		}
//		watcher.config = newConfig
//		time.Sleep(time.Millisecond * 50)
//	}
//}
//
//func (watcher *Watcher) addShards(config shardmaster.Config, shards []int, gid int) {
//
//	args := AddShardsArgs{
//		ConfigNum: config.Num,
//		Shards: shards,
//	}
//	if servers, ok := config.Groups[gid]; ok {
//		// try each server for the shard.
//		for si := 0; si < len(servers); si++ {
//			srv := watcher.make_end(servers[si])
//			var reply AddShardsReply
//			ok := srv.Call("ShardKV.AddShards", &args, &reply)
//			if ok && reply.WrongLeader == false && reply.Err == OK {
//				return
//			}
//			//if ok && (reply.Err == ErrWrongGroup) {
//			//	panic(fmt.Sprintf("gid %v cannot be found", gid))
//			//}
//		}
//	} else {
//		panic(fmt.Sprintf("gid %v cannot be found", gid))
//	}
//
//}
//
//func (watcher *Watcher) removeShards(config shardmaster.Config, shards []int, gid int) {
//	args := RemoveShardsArgs{
//		ConfigNum: config.Num,
//		Shards: shards,
//	}
//	if servers, ok := config.Groups[gid]; ok {
//		// try each server for the shard.
//		for si := 0; si < len(servers); si++ {
//			srv := watcher.make_end(servers[si])
//			var reply RemoveShardsReply
//			ok := srv.Call("ShardKV.RemoveShards", &args, &reply)
//			if ok && reply.WrongLeader == false && reply.Err == OK {
//				return
//			}
//			//if ok && (reply.Err == ErrWrongGroup) {
//			//	panic(fmt.Sprintf("gid %v cannot be found", gid))
//			//}
//		}
//	} else {
//		panic(fmt.Sprintf("gid %v cannot be found", gid))
//	}
//
//}
//
//func (watcher *Watcher) moveShards(config shardmaster.Config, shards []int, oldGid int, newGid int) {
//	args := MoveShardsArgs{
//		ConfigNum: config.Num,
//		Shards: shards,
//		NewGID:newGid,
//	}
//	if servers, ok := config.Groups[oldGid]; ok {
//		// try each server for the shard.
//		for si := 0; si < len(servers); si++ {
//			srv := watcher.make_end(servers[si])
//			var reply MoveShardsReply
//			ok := srv.Call("ShardKV.MoveShardsReply", &args, &reply)
//			if ok && reply.WrongLeader == false && reply.Err == OK {
//				return
//			}
//			//if ok && (reply.Err == ErrWrongGroup) {
//			//	panic(fmt.Sprintf("gid %v cannot be found", gid))
//			//}
//		}
//	} else {
//		panic(fmt.Sprintf("gid %v cannot be found", oldGid))
//	}
//
//}
//
////func (kv *ShardKV) onNewConfig(newConfig shardmaster.Config) {
////	kv.mu.Lock()
////	defer kv.mu.Unlock()
////
////	if newConfig.Num > kv.config.Num {
////		addShards := make([]int, 0)
////		removeShards := make([]int, 0)
////		moveShards := make(map[int][]int)
////
////		for shard, newGid := range newConfig.Shards {
////			oldGid := kv.config.Shards[shard]
////			if newGid != oldGid {
////				if newGid == 0 && oldGid == kv.gid {
////					removeShards = append(removeShards, shard)
////				} else if oldGid == 0 && newGid == kv.gid {
////					addShards = append(addShards, shard)
////				} else if oldGid == kv.gid {
////					moveShards[newGid] = append(moveShards[newGid], shard)
////				}
////
////			}
////
////		}
////
////		if len(addShards) && len(moveShards) > 0 {
////			panic("weird shards")
////		}
////		if len(addShards) && len(removeShards) > 0 {
////			panic("weird shards")
////		}
////		if len(removeShards) && len(moveShards) > 0 {
////			panic("weird shards")
////		}
////		if len(removeShards) > 0 {
////			panic("weird shards")
////		}
////
////		for newGid, shards := range moveShards {
////			watcher.moveShards(newConfig, shards, gid, newGid)
////			watcher.removeShards(newConfig, shards, gid)
////		}
////
////	} else {
////		panic(fmt.Sprintf("kv %v has a config %v >= new config %v", kv.me, kv.config.Num, newConfig.Num))
////	}
////	kv.config = newConfig
////}

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//fm.Printf(format, a...)
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}
func DPrint2(format string, a ...interface{}) (n int, err error) {
	if Debug < 0 {
		//fm.Printf(format, a...)
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op            string
	Key           string
	ArgValue      string
	ClientId      uint64
	SerialNo      uint64

	Type          string

	ConfigNum     int
	ConfigCmd     string
	ConfigCmdArgs []byte
}

type OpResult struct {
	opPtr  *Op
	result chan string
	err    chan Err
}

func (op *Op) exec(kv *ShardKV) (result string) {

	switch {
	case op.Type == KVCmd:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		switch op.Op {
		case "APPEND":
			if val, has := kv.kvPair[op.Key]; has {
				kv.kvPair[op.Key] = val + op.ArgValue
			} else {
				kv.kvPair[op.Key] = op.ArgValue
			}
		case "PUT":
			kv.kvPair[op.Key] = op.ArgValue
		case "GET":
			if val, has := kv.kvPair[op.Key]; has {
				result = val
			}
		}
	//DPrintf("kv %v: %+v", kv.me, kv.kvPair)
	case op.Type == ShardConfigCmd:
		op.execShardCmd(kv)
	}
	return

}

func (op *Op) execShardCmd(kv *ShardKV) {
	kv.mu.Lock()
	switch op.ConfigCmd {
	case "ADD_SHARDS":
		arg := &AddShardsArgs{}
		arg.fromOP(op)

		for _, shard := range arg.Shards {
			if kv.responsibleShards[shard] {
				panic("add shards twice")
			}

			kv.responsibleShards[shard] = true
			kv.shardsConfigNum[shard] = op.ConfigNum
		}
		kv.mu.Unlock()

	case "MOVE_SHARDS":
		arg := &MoveShardsArgs{}
		arg.fromOP(op)
		for _, shard := range arg.Shards {
			delete(kv.responsibleShards, shard)
			kv.shardsConfigNum[shard] = op.ConfigNum
		}

		servers := arg.GroupServers

		kvData := make(map[string]string)

		clientSNTable := make(map[uint64]uint64)

		for k, v := range kv.kvPair {
			for _, shard := range arg.Shards {
				if shard == key2shard(k) {
					kvData[k] = v
				}
			}
		}
		DPrint2("gid %v kv %v send data %v to gid %v, shards:%v, all kv data %v ", kv.gid, kv.me, kvData, arg.Gid, arg.Shards, kv.kvPair)

		for client, clientTable := range kv.clientTableMap {
			clientTable.mu.Lock()
			clientSNTable[client] = clientTable.lastSerialNo
			clientTable.mu.Unlock()
		}
		kv.mu.Unlock()

		copyArg := CopyShardsArgs{
			ConfigNum:arg.ConfigNum,
			Shards: arg.Shards,
			Gid: arg.Gid,
			KVData: kvData,
			ClientSerialNo: clientSNTable,


		}
		for {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				DPrint2("try servers %v, server%v, %+v", servers, si, srv)
				var reply CopyShardsReply
				ok := srv.Call("ShardKV.CopyShards", &copyArg, &reply)
				DPrint2("get response")
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == "") {
					DPrint2("reply %+v, return ", reply)
					return
				}
				if ok {
					DPrint2("reply %+v", reply)
				}else{
					DPrint2("not OK")
				}
			}
		}

	case "COPY_SHARDS":
		arg := &CopyShardsArgs{}
		arg.fromOP(op)

		DPrint2("kv %v, gid %v,  configCmd %v,  shards: %v, config num %v,responsible shards %v, shardConfigs %v", kv.me, kv.gid, op.ConfigCmd, arg.Shards, arg.ConfigNum, kv.responsibleShards, kv.shardsConfigNum)
		DPrint2("kv %v, copy data %v", kv.me, arg.KVData)
		for _, shard := range arg.Shards {
			if kv.responsibleShards[shard] {
				DPrint2("kv %v, gid %v,  configCmd %v,  shards: %v, responsible shards %v, shardConfigs %v", kv.me, kv.gid, op.ConfigCmd, arg.Shards, kv.responsibleShards, kv.shardsConfigNum)
				panic("add shards twice")
			}
			kv.responsibleShards[shard] = true
			kv.shardsConfigNum[shard] = op.ConfigNum
		}

		for k, v := range arg.KVData {
			if _, has := kv.kvPair[k]; has {
				panic("already has key")
			}
			kv.kvPair[k] = v
		}
		DPrint2("kv %v, after copy data, kv pair %v", kv.me, kv.kvPair)

		DPrint2("kv %v, client table arg %v", kv.me, arg.ClientSerialNo)
		for client, sn := range arg.ClientSerialNo {
			if _, has := kv.clientTableMap[client]; !has {
				kv.clientTableMap[client] = &ClientTable{
					lastSerialNo: sn,
				}
			} else {
				kv.clientTableMap[client].mu.Lock()
				if sn > kv.clientTableMap[client].lastSerialNo {
					kv.clientTableMap[client].lastSerialNo = sn
				}
				kv.clientTableMap[client].mu.Unlock()
			}
		}
		DPrint2("kv %v, after update client table arg %v", kv.me, arg.ClientSerialNo)
		kv.mu.Unlock()
		DPrint2("kv %v, release lock", kv.me)

	case "REMOVE_SHARDS":
		arg := &RemoveShardsArgs{}
		arg.fromOP(op)
		var key2delete []string

		for _, shard := range arg.Shards {
			if kv.responsibleShards[shard] {
				panic("remove responsible shard!")
			}
			kv.shardsConfigNum[shard] = op.ConfigNum
		}
		for key, _ := range kv.kvPair {
			for _, shard := range arg.Shards {
				if shard == key2shard(key) {
					key2delete = append(key2delete, key)
				}
			}
		}

		for _, key := range key2delete {
			delete(kv.kvPair, key)
		}
		DPrint2("kv %v, gid %v,  configCmd %v,  shards: %v, config num %v,responsible shards %v, shardConfigs %v, remove keys %v", kv.me, kv.gid, op.ConfigCmd, arg.Shards, arg.ConfigNum, kv.responsibleShards, kv.shardsConfigNum, key2delete)
		kv.mu.Unlock()

	default:
		panic("why here?")
	}
	return

}

func (op *Op) getFromCache(kv *ShardKV) (result string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Op {
	case "GET":
		if val, has := kv.kvPair[op.Key]; has {
			result = val
		}
	}
	return

}

func (op *Op) equal(op2 *Op) bool {
	if op.Type != op2.Type {
		return false
	}
	switch {
	case op.Type == KVCmd:
		return op.ClientId == op2.ClientId && op.SerialNo == op2.SerialNo
	case op.Type == ShardConfigCmd:
		return op.ConfigCmd == op2.ConfigCmd && op.ConfigNum == op2.ConfigNum && reflect.DeepEqual(op.ConfigCmdArgs, op2.ConfigCmdArgs)
	}
	DPrintf("%v", op.Type)
	panic("what the cmd is?")

}

type ClientTable struct {
	mu           sync.Mutex
	lastSerialNo uint64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Op: "GET",
		Key: args.Key,
		ClientId:args.ClientId,
		SerialNo:args.SerialNo,
		Type: KVCmd,
	}

	wrongLeader, returnErr, value := kv.waitForApply(op)

	reply.Value = value
	reply.WrongLeader = wrongLeader
	reply.Err = returnErr

}

func (kv *ShardKV) checkRaftState() (err Err) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		err = "not leader"
	}
	return ""

}

func (kv *ShardKV) waitForApply(op Op) (wrongLeader bool, returnErr Err, value string) {

	if err := kv.checkRaftState(); err != "" {
		returnErr = err
		wrongLeader = true
		return
	}

	//if op.Type == KVCmd {
	//	if !kv.containKeyShard(op.Key) {
	//		returnErr = ErrWrongGroup
	//		return
	//	}
	//}

	if val, hitCache := kv.getFromCache(&op); hitCache {
		value = val
		returnErr = OK
		return

	}

	index, originalTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		wrongLeader = true
		return
	}

	kv.mu.Lock()
	if existing, has := kv.waitMap[index]; has {
		go func() {
			existing.err <- "newer command with same index"
		}()
	}
	cur := OpResult{
		opPtr: &op,
		err: make(chan Err),
		result:make(chan string),
	}
	kv.waitMap[index] = cur
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		DPrintf("defer,index %v, op %+v, wrongleader %+v, error %+v", index, op, wrongLeader, returnErr)
		if curOp, has := kv.waitMap[index]; has && cur.opPtr.equal(curOp.opPtr) {
			delete(kv.waitMap, index)
		}
	}()

	for {
		select {
		case err := <-cur.err:
			returnErr = err
			DPrintf("return err: %+v", returnErr)
			wrongLeader = true
			DPrintf("index %v, wrongLeader %v", index, wrongLeader)
			return
		case val := <-cur.result:
			wrongLeader = false
			value = val
			returnErr = OK
			return

		case _ = <-time.After(time.Millisecond * 500):
			term, isLeader := kv.rf.GetState()
			if !isLeader || term != originalTerm {
				wrongLeader = true
				returnErr = "no longer leader"
				return
			}
		}
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	DPrintf("args %+v", args)
	op := Op{
		Op: strings.ToUpper(args.Op),
		Key: args.Key,
		ArgValue:args.Value,
		ClientId:args.ClientId,
		SerialNo:args.SerialNo,
		Type: KVCmd,
	}
	wrongLeader, returnErr, _ := kv.waitForApply(op)

	reply.WrongLeader = wrongLeader
	reply.Err = returnErr

	if !wrongLeader {
		DPrintf("reply: %+v, %p", reply, reply)
	}
	DPrintf("reply: %+v", reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) snapshot(index int) (doSnapshot bool) {
	if kv.maxraftstate == -1 {
		return
	}
	size := kv.rf.GetRaftStateSize()
	if size > kv.maxraftstate {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		DPrintf("kv %v, begin snapshot", kv.me)
		kv.rf.TruncateLog(index, kv.encodeToSnapShot())
		doSnapshot = true
		DPrintf("kv %v, end snapshot", kv.me)
	}
	return

}

func (kv *ShardKV) listenOnApplyCh(applyCh chan raft.ApplyMsg) {
	// Your code here.
	for applyMsg := range applyCh {
		if applyMsg.UseSnapshot {
			kv.mu.Lock()
			for _, v := range kv.waitMap {
				v := v
				go func() {
					v.err <- "error, no more leader since get InstallSnapShot"
				}()

			}

			kv.restoreFromSnapShot(applyMsg.Snapshot)
			kv.mu.Unlock()

			continue
		}
		op := applyMsg.Command.(Op)
		opPtr := &op

		var val string
		if cacheVal, hitCache := kv.getFromCache(opPtr); hitCache {
			val = cacheVal

		} else if op.Type == KVCmd && !kv.containKeyShard(op.Key) {
			kv.mu.Lock()
			if opResult, has := kv.waitMap[applyMsg.Index]; has {
				go func() {
					opResult.err <- ErrWrongGroup
				}()
			}
			kv.mu.Unlock()
			continue
		} else {
			val = opPtr.exec(kv)
			//DPrint2("after val exec")
		}

		kv.mu.Lock()
		if opResult, has := kv.waitMap[applyMsg.Index]; has {
			sameOp := opResult.opPtr.equal(opPtr)
			go func() {
				if sameOp {
					//DPrint2("sent to chanel")
					opResult.result <- val
					//DPrint2("after sent to chanel")
				} else {
					//DPrint2("different output")
					opResult.err <- "error, same index but different op"
				}
			}()
		} else {
			//DPrint2("not found index, op %v", op)
		}
		kv.mu.Unlock()
		//DPrint2("after unlock")

		if op.Type == KVCmd {
			kv.mu.Lock()
			clientTable, has := kv.clientTableMap[opPtr.ClientId]
			kv.mu.Unlock()

			if has {
				clientTable.mu.Lock()
				if clientTable.lastSerialNo < opPtr.SerialNo {
					clientTable.lastSerialNo = opPtr.SerialNo
				}
				clientTable.mu.Unlock()

			} else {
				kv.mu.Lock()
				kv.clientTableMap[opPtr.ClientId] = &ClientTable{
					lastSerialNo: opPtr.SerialNo,
				}
				kv.mu.Unlock()
			}
		}

		kv.snapshot(applyMsg.Index)
		//DPrint2("kv %v, applyMsg: %+v", kv.me, applyMsg)

	}
}

func (kv *ShardKV) restoreFromSnapShot(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	DPrintf("kv %v begin restoreFromSnapShot, data %v", kv.me, data)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var kvPair map[string]string
	err := d.Decode(&kvPair)
	if err != nil {
		panic(err)
	}
	DPrintf("kv pair: %v", kvPair)
	kv.kvPair = kvPair

	var clientTableMap map[uint64]uint64
	d.Decode(&clientTableMap)
	DPrintf("clientTable: %v", clientTableMap)
	kv.clientTableMap = make(map[uint64]*ClientTable)
	for client, serialNo := range clientTableMap {
		kv.clientTableMap[client] = &ClientTable{
			lastSerialNo: serialNo,
		}
	}
	var responsibleShards map[int]bool
	err = d.Decode(&responsibleShards)
	if err != nil {
		panic(err)
	}
	kv.responsibleShards = responsibleShards

	var shardsConfigNum   map[int]int
	err = d.Decode(&shardsConfigNum)
	if err != nil {
		panic(err)
	}
	kv.shardsConfigNum = shardsConfigNum
}

func (kv *ShardKV) encodeToSnapShot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	kvPair := make(map[string]string)

	for k, v := range kv.kvPair {
		kvPair[k] = v
	}

	clientTableMap := make(map[uint64]uint64)
	for k, v := range kv.clientTableMap {
		v.mu.Lock()
		clientTableMap[k] = v.lastSerialNo
		v.mu.Unlock()
	}

	responsibleShards := make(map[int]bool)
	for shard, val := range kv.responsibleShards {
		responsibleShards[shard] = val
	}

	shardsConfigNum := make(map[int]int)
	for shard, confNum := range kv.shardsConfigNum {
		shardsConfigNum[shard] = confNum
	}

	e.Encode(kvPair)
	e.Encode(clientTableMap)
	e.Encode(responsibleShards)
	e.Encode(shardsConfigNum)
	return w.Bytes()
}

func (kv *ShardKV) getFromCache(op *Op) (cacheVal string, hitCache bool) {
	switch {
	case op.Type == KVCmd:
		kv.mu.Lock()
		clientTable, has := kv.clientTableMap[op.ClientId]
		kv.mu.Unlock()

		if has {
			clientTable.mu.Lock()
			hitCache = clientTable.lastSerialNo >= op.SerialNo
			clientTable.mu.Unlock()
			DPrintf("hitCache %+v", hitCache)
			DPrintf("clientmap[%v]: %+v", op.ClientId, clientTable)

		}

		if hitCache {
			cacheVal = op.getFromCache(kv)
		}

	case op.Type == ShardConfigCmd:
		kv.mu.Lock()
		hitCache = kv.alreadySee(op)
		if hitCache {
			//DPrint2("gid %v, kv %v hit shard cache, kv responsible shards %v, shard config %v", kv.gid, kv.me, kv.responsibleShards, kv.shardsConfigNum)
		}
		kv.mu.Unlock()
	}
	return

}

func (kv *ShardKV) containKeyShard(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//return true
	return kv.responsibleShards[key2shard(key)]

}

