package raftkv

import (
	"encoding/gob"
	"../labrpc"
	"../raft"
	"sync"
	"time"
	"strings"
	"fmt"
	"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		//fm.Printf(format, a...)
		fmt.Println(time.Now().Format(time.StampMilli) + " " + fmt.Sprintf(format, a...))
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	Key      string
	ArgValue string
	ClientId uint64
	SerialNo uint64
}

type OpResult struct {
	opPtr  *Op
	result chan string
	err    chan Err
}

func (op *Op) exec(kv *RaftKV) (result string) {
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
	return

}

func (op *Op) getFromCache(kv *RaftKV) (result string) {
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
	return op.ClientId == op2.ClientId && op.SerialNo == op2.SerialNo

}

type RaftKV struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg

	maxraftstate   int // snapshot if log grows this big

	kvPair         map[string]string
	waitMap        map[int]OpResult

	clientTableMap map[uint64]*ClientTable

			   // Your definitions here.
}

type ClientTable struct {
	mu           sync.Mutex
	//result       map[uint64]string
	lastSerialNo uint64
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Op: "GET",
		Key: args.Key,
		ClientId:args.ClientId,
		SerialNo:args.SerialNo,
	}

	wrongLeader, returnErr, value := kv.waitForApply(op)

	reply.Value = value
	reply.WrongLeader = wrongLeader
	reply.Err = returnErr

}

func (kv *RaftKV) checkRaftState() (err Err) {
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		err = "not leader"
	}
	return ""

}

func (kv *RaftKV) waitForApply(op Op) (wrongLeader bool, returnErr Err, value string) {

	if err := kv.checkRaftState(); err != "" {
		returnErr = err
		wrongLeader = true
		return
	}

	//if op.Op != "GET" {
	//	DPrintf("try to get from cache")
	//		return
	//	}
	//
	//}
	if val, hitCache := kv.getFromCache(&op); hitCache {
		value = val
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

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	DPrintf("args %+v", args)
	op := Op{
		Op: strings.ToUpper(args.Op),
		Key: args.Key,
		ArgValue:args.Value,
		ClientId:args.ClientId,
		SerialNo:args.SerialNo,
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
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvPair = make(map[string]string)
	kv.waitMap = make(map[int]OpResult)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.clientTableMap = make(map[uint64]*ClientTable)

	// You may need initialization code here.
	kv.restoreFromSnapShot(kv.rf.ReadSnapShot(persister.ReadSnapshot()))

	go kv.listenOnApplyCh(kv.applyCh)

	return kv
}

func (kv *RaftKV) snapshot(index int) (doSnapshot bool) {
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

func (kv *RaftKV) listenOnApplyCh(applyCh chan raft.ApplyMsg) {
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

		} else {
			val = opPtr.exec(kv)
		}

		kv.mu.Lock()
		if opResult, has := kv.waitMap[applyMsg.Index]; has {
			sameOp := opResult.opPtr.equal(opPtr)
			go func() {
				if sameOp {
					opResult.result <- val
				} else {
					opResult.err <- "error, same index but different op"
				}
			}()
		}
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

		kv.snapshot(applyMsg.Index)
		DPrintf("kv %v, applyMsg: %+v", kv.me, applyMsg)

	}
}

func (kv *RaftKV) restoreFromSnapShot(data []byte) {
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
}

func (kv *RaftKV) encodeToSnapShot() []byte {
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

	e.Encode(kvPair)
	e.Encode(clientTableMap)
	return w.Bytes()
}

func (kv *RaftKV) getFromCache(op *Op) (cacheVal string, hitCache bool) {
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
	return

}
