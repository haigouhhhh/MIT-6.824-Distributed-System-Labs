package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"../labrpc"
	"time"
	"math/rand"
	"fmt"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                   sync.Mutex          // Lock to protect shared access to this peer's state
	peers                []*labrpc.ClientEnd // RPC end points of all peers
	persister            *Persister          // Object to hold this peer's persisted state
	me                   int                 // this peer's index into peers[]

						 // Your data here (2A, 2B, 2C).
						 // Look at the paper's Figure 2 for a description of what
						 // state a Raft server must maintain.

	state                State
	term                 int
	votedFor             int
	log                  []LogEntry
	electionTimeoutTimer *time.Timer

	commitIndex          int
	lastApplied          int

	nextIndex            []int
	matchIndex           []int

	applyChan            chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(rf.votedFor)
		e.Encode(rf.term)
		e.Encode(rf.log)
		data := w.Bytes()
		rf.persister.SaveRaftState(data)

		if len(rf.log) > 0 {
			DPrintf("rf %+v, persist, votedFor %v, term %v, log %+v", rf.me, rf.votedFor, rf.term, rf.log[len(rf.log) - 1 ])
		}

	}()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 {
		// bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.term)
	d.Decode(&rf.log)
	DPrintf("rf %+v, readPersist, votedFor %v, term %v, log %+v", rf.me, rf.votedFor, rf.term, rf.log)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int

	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.term

	DPrintf("rafter %+v with term %v, state %v, last log index %v, last log index term %v,  receive request from %+v", rf.me, rf.term, rf.state, rf.getLastIndex(), rf.getLastEntry().Term, args)

	switch  {
	case rf.term < args.Term:
		rf.becomeFollower(args.Term)
	case rf.term == args.Term:
	case rf.term > args.Term:
	}

	switch  {
	case rf.term > args.Term:
		reply.VoteGranted = false
	case rf.votedFor == -1 || rf.votedFor == args.CandidateId:
		selfLogMoreUpdate := rf.getLastIndex() > 0 && (rf.getEntryByIndex(rf.getLastIndex()).Term > args.LastLogTerm || (rf.getEntryByIndex(rf.getLastIndex()).Term == args.LastLogTerm && rf.getLastIndex() > args.LastLogIndex))
		if !selfLogMoreUpdate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}

	if reply.VoteGranted {
		rf.resetElectionTimeout()
	}
	DPrintf("result %+v", reply)
	DPrintf("rafter %+v with term %v, state %v, votedFor %v", rf.me, rf.term, rf.state, rf.votedFor)
	return

}

func updateTermIfNece(rf *Raft, otherTerm int) bool {
	if otherTerm > rf.term {
		rf.term = otherTerm
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.term
	reply.Success = true

	switch  {
	case rf.term < args.Term:
		rf.becomeFollower(args.Term)
	case rf.term == args.Term:
		switch rf.state {
		case Follower:
			DPrintf("%v reset election timeout because of AppendEntries from %v", rf.me, args.LeaderId)
			rf.resetElectionTimeout()
		case Candidate:
			rf.becomeFollower(args.Term)
		case Leader:
			fmt.Println("a leader receive a append entries request with same term, ignore")
		}
	case rf.term > args.Term:
		reply.Success = false
	}

	DPrintf("rf %v receive %+v", rf.me, args)

	if args.PrevLogIndex > 0 && (!rf.indexExist(args.PrevLogIndex) || rf.getEntryByIndex(args.PrevLogIndex).Term != args.PrevLogTerm) {
		reply.Success = false
	}
	DPrintf("rf %v return leader %v AppendEntries response %+v", rf.me, args.LeaderId, reply)
	if !reply.Success {
		return
	}
	DPrintf("rf %v log before %+v", rf.me, rf.log)

	for i, entry := range args.Entries {
		if rf.indexExist(args.PrevLogIndex + 1 + i) {
			if (rf.getEntryByIndex(args.PrevLogIndex + 1 + i).Term != entry.Term) {
				rf.log = rf.log[ : args.PrevLogIndex + i]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	DPrintf("rf %v log after %+v", rf.me, rf.log)

	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) > 0 && args.LeaderCommit > args.PrevLogIndex + len(args.Entries) {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	rf.applyEntries()
	rf.resetElectionTimeout()
}

func (rf *Raft) onElectionTimeout() {

	DPrintf("rf %+v election timeout", rf.me)
	if rf.state == Follower {
		DPrintf("rf %+v become candidate", rf.me)
		rf.becomeCandidate()
	} else if rf.state == Candidate {
		rf.startNewElection()
	} else {
		// ignore
		fmt.Println("Leader should not have election timeout timer")
	}

}

func (rf *Raft) onOtherTerm(otherTerm int) {
	if updateTermIfNece(rf, otherTerm) {
		rf.becomeFollower(otherTerm)
	}
}

//should be protected by rf.mu
func (rf *Raft) resetElectionTimeout() {
	DPrintf("rf %v reset timeout", rf.me)
	if rf.electionTimeoutTimer != nil {
		rf.electionTimeoutTimer.Stop()
	}

	timer := time.NewTimer(rf.getElectionTimeout());

	term := rf.term

	go func() {
		<-timer.C
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.term != term {
			return
		}
		rf.onElectionTimeout()

	}()
	rf.electionTimeoutTimer = timer

}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	boolChan := make(chan bool)
	go func() {
		boolChan <- rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}()
	select {
	case ok := <-boolChan:
		return ok
	case <-time.After(time.Millisecond * 200):
		return false
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	isLeader = rf.state == Leader
	term = rf.term

	if (isLeader) {
		index = rf.appendLog(command)
	}

	DPrintf("rf %v receive command %v, index is %v ", rf.me, command, index)
	return index, term, isLeader
}

// should be protected by rf.mu
func (rf *Raft) appendLog(command interface{}) (index int) {
	entry := LogEntry{
		Term: rf.term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	index = len(rf.log)
	return
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
	rf.term = 0
	rf.state = Follower

	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimeout()

	return rf
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.state = Follower
	rf.term = newTerm
	rf.votedFor = -1
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.startNewElection()

}

func (rf *Raft) initLeader() {
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}

	rf.matchIndex = make([]int, len(rf.peers))
	DPrintf("leader %v log is %+v, nextIndex is %+v", rf.me, rf.log, rf.nextIndex)

}

func (rf *Raft) getLastIndex() int {
	return len(rf.log)
}

func (rf *Raft) getEntryByIndex(index int) LogEntry {
	if rf.indexExist(index) {
		return rf.log[index - 1]
	}
	panic(fmt.Sprintf("index %v not exist in rf %v", index, rf.me))
}

func (rf *Raft) getLastEntry() LogEntry {
	if !rf.indexExist(rf.getLastIndex()) {
		return LogEntry{
			Command: -1,
			Term: -1,
		}
	}
	return rf.getEntryByIndex(rf.getLastIndex())
}

func (rf *Raft) indexExist(index int) bool {
	return index > 0 && (len(rf.log) >= index)
}

func (rf *Raft) becomeLeader() {
	DPrintf("%v become leader, term %v", rf.me, rf.term)
	rf.initLeader()

	if rf.electionTimeoutTimer != nil {
		rf.electionTimeoutTimer.Stop()
		rf.electionTimeoutTimer = nil
	}

	rf.sendHeartBeatToPeers()
}

func (rf *Raft) sendHeartBeatToPeers() {

	currentTerm := rf.term
	for peerIndex := range rf.peers {
		peerIndex := peerIndex
		if peerIndex == rf.me {
			continue
		}
		go func() {
			straightFailCount := 0
			for {
				rf.mu.Lock()

				if (rf.state != Leader || rf.term != currentTerm) {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[peerIndex] - 1
				prevLogTerm := -1

				var entries []LogEntry
				if rf.indexExist(prevLogIndex) {
					prevLogTerm = rf.getEntryByIndex(prevLogIndex).Term
					entries = rf.log[rf.nextIndex[peerIndex] - 1: ]
				} else {
					entries = rf.log[:]
				}

				args := AppendEntriesArgs{
					Term: rf.term,
					LeaderId: rf.me,

					PrevLogIndex: prevLogIndex,
					PrevLogTerm: prevLogTerm,
					Entries:entries,
					LeaderCommit: rf.commitIndex,
				}

				DPrintf("leader %v sent AppendEntries %+v, to %v", rf.me, args, peerIndex)
				leader := rf.me
				_ = leader
				rf.mu.Unlock()

				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(peerIndex, &args, &reply)
				DPrintf("leader %v end AE call to %v", rf.me, peerIndex)

				sleep := true
				if ok {

					rf.mu.Lock()
					rf.onOtherTerm(reply.Term)
					if rf.state != Leader {
						rf.persist()
						DPrintf("leader %v become follower after AppendEntries response", rf.me)
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] + len(args.Entries)
						rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
						rf.updateCommittedIndex()

					} else {
						if rf.nextIndex[peerIndex] > 1 {
							rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
						}
						straightFailCount ++

						if straightFailCount == 4 {
							straightFailCount = 0
							rf.nextIndex[peerIndex] = 1
						}
						sleep = false
					}
					rf.mu.Unlock()

				}

				//if !ok {
				//	DPrintf("leader %v sent AppendEntries to %v but no reply from ", leader, peerIndex)
				//}

				if sleep {
					constraint := 20
					time.Sleep(time.Second / time.Duration(constraint))
				}

			}

		}()
	}
}

func (rf *Raft) updateCommittedIndex() {
	if (rf.state != Leader) {
		panic(fmt.Sprintf("%v is not leader", rf.me))
	}

	majority := len(rf.peers) / 2;
	for n := rf.commitIndex + 1; n <= rf.getLastIndex(); n++ {
		count := 1
		for peerIndex, mI := range rf.matchIndex {
			if peerIndex == rf.me {
				continue
			}

			if mI >= n {
				count ++;
			}
		}
		if (rf.getEntryByIndex(n).Term == rf.term && count > majority) {
			rf.commitIndex = n;
		}

	}
	rf.applyEntries()

}

func (rf *Raft) applyEntries() {
	mediateChan := make(chan ApplyMsg, rf.commitIndex - rf.lastApplied)
	applyMsgChan := rf.applyChan
	go func() {
		for applyMsg := range mediateChan {
			applyMsgChan <- applyMsg
		}
	}()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		command := rf.getEntryByIndex(rf.lastApplied).Command
		i := rf.lastApplied
		mediateChan <- ApplyMsg{
			Index: i,
			Command: command,

		}
	}

	close(mediateChan)

}

func (rf *Raft) startNewElection() {

	rf.term += 1
	rf.resetElectionTimeout()
	rf.persist()

	votedChan := make(chan int, len(rf.peers))
	currentTerm := rf.term

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		lastLogTerm := -1
		if rf.indexExist(rf.getLastIndex()) {
			lastLogTerm = rf.getLastEntry().Term
		}
		args := RequestVoteArgs{
			Term: rf.term,
			CandidateId: rf.me,
			LastLogIndex:rf.getLastIndex(),
			LastLogTerm: lastLogTerm,
		}
		reply := RequestVoteReply{}
		go func(index int) {
			ok := rf.sendRequestVote(index, &args, &reply)

			//if !ok {
			//	DPrintf("rf %v send request vote to %v fail, %+v", rf.me, index, args)
			//}

			if ok && reply.VoteGranted {
				votedChan <- 1
			} else {
				rf.mu.Lock()
				rf.onOtherTerm(reply.Term)
				rf.persist()
				rf.mu.Unlock()

				votedChan <- 0
			}

		}(index)
	}

	go func() {
		votedCount := 1
		for voted := range votedChan {
			rf.mu.Lock()

			if rf.term != currentTerm {
				rf.mu.Unlock()
				return

			}
			votedCount += voted

			//DPrintf("%v votedCount, %v state", votedCount, rf.state)

			if (rf.state != Candidate) {
				rf.mu.Unlock()
				return
			}
			if votedCount > (len(rf.peers) / 2) {
				rf.becomeLeader()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

		}

	}()

}

func (rf *Raft) getElectionTimeout() time.Duration {
	d := time.Duration(rand.Intn(1000 - 550) + 550) * time.Millisecond
	return d

}
