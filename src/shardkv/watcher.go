package shardkv

import (
	"sync"
	"../labrpc"
	"../shardmaster"
	"time"
	"fmt"
	"sync/atomic"
)

var once sync.Once
var currentWatcher *Watcher

type Watcher struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	running  int32
}

func StartWatcher(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) {

	once.Do(func() {
		watcher := new(Watcher)
		watcher.sm = shardmaster.MakeClerk(masters)
		watcher.make_end = make_end
		go watcher.watch()
		currentWatcher = watcher

	})
	return
}

func stopCurrentWatcher() {
	atomic.StoreInt32(&currentWatcher.running, 1)
	once = sync.Once{}
}

func (watcher *Watcher) watch() {

	for atomic.LoadInt32(&watcher.running) == int32(0) {
		newConfig := watcher.sm.Query(-1)
		if newConfig.Num > watcher.config.Num {
			addGroups := make(map[int][]int)
			removeGroups := make(map[int][]int)
			moveGroups := make(map[int]map[int][]int)

			for i, newGid := range newConfig.Shards {
				oldGid := watcher.config.Shards[i]
				if newGid != oldGid {
					if newGid == 0 {
						removeGroups[oldGid] = append(removeGroups[oldGid], i)
					} else if oldGid == 0 {
						addGroups[newGid] = append(addGroups[newGid], i)
					} else {
						if _, has := moveGroups[oldGid]; !has {
							moveGroups[oldGid] = make(map[int][]int)
						}
						moveGroups[oldGid][newGid] = append(moveGroups[oldGid][newGid], i)
					}

				}

			}

			DPrint2(" watcher add %v, remove %v, move %v", addGroups, removeGroups, moveGroups)
			for gid, shards := range addGroups {
				watcher.addShards(newConfig, shards, gid)
			}

			for gid, shards := range removeGroups {

				watcher.removeShards(watcher.config, newConfig, shards, gid)
			}
			for gid, peers := range moveGroups {
				for newGid, shards := range peers {
					watcher.moveShards(watcher.config, newConfig, shards, gid, newGid)
					watcher.removeShards(watcher.config, newConfig, shards, gid)
				}
			}

			watcher.config = newConfig
		}
		time.Sleep(time.Millisecond * 50)
	}
	DPrint2("watcher die")
}

func (watcher *Watcher) addShards(config shardmaster.Config, shards []int, gid int) {

	args := AddShardsArgs{
		ConfigNum: config.Num,
		Shards: shards,
	}
	if servers, ok := config.Groups[gid]; ok {
		// try each server for the shard.
		for {
			for si := 0; si < len(servers); si++ {
				srv := watcher.make_end(servers[si])
				var reply AddShardsReply
				ok := srv.Call("ShardKV.AddShards", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == "") {
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		panic(fmt.Sprintf("gid %v cannot be found", gid))
	}
	panic("")

}

func (watcher *Watcher) removeShards(oldConfig shardmaster.Config, newConfig shardmaster.Config, shards []int, gid int) {
	args := RemoveShardsArgs{
		ConfigNum: newConfig.Num,
		Shards: shards,
	}
	DPrint2("watcher remove shard %v gid %v", args, gid)
	if servers, ok := oldConfig.Groups[gid]; ok {
		// try each server for the shard.
		for {
			for si := 0; si < len(servers); si++ {
				srv := watcher.make_end(servers[si])
				var reply RemoveShardsReply
				ok := srv.Call("ShardKV.RemoveShards", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == "") {
					DPrint2("watcher finish remove shard %v gid %v", args, gid)
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		panic(fmt.Sprintf("gid %v cannot be found", gid))
	}
	panic("")

}

func (watcher *Watcher) moveShards(oldConfig shardmaster.Config, newConfig shardmaster.Config, shards []int, oldGid int, newGid int) {
	args := MoveShardsArgs{
		ConfigNum: newConfig.Num,
		Shards: shards,
		Gid:newGid,
		GroupServers: newConfig.Groups[newGid],
	}
	DPrint2("watcher move shard %v from gid %v", args, oldGid)
	DPrint2("watcher move shard old config%v, new config %v", oldConfig, newConfig)
	if servers, ok := oldConfig.Groups[oldGid]; ok {
		// try each server for the shard.
		for {
			for si := 0; si < len(servers); si++ {
				srv := watcher.make_end(servers[si])
				var reply MoveShardsReply
				ok := srv.Call("ShardKV.MoveShards", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == "") {
					DPrint2("watcher finish move shard %v from gid %v", args, oldGid)
					return
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		panic(fmt.Sprintf("gid %v cannot be found", oldGid))
	}
	DPrint2("weird !!! gid %v, watcher old config %v, new config %v", oldGid, oldConfig, newConfig)
	panic("")

}
