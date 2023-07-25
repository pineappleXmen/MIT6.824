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

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Leader
	Candidate
	Dead
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state        State
	electionTime time.Time

	//persistent state
	currentTerm int
	votedFor    int
	log         Log

	lastIncludeIndex int
	lastIncludeTerm  int

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//Snapshot state
	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	waitingSnapshot []byte
	waitingIndex    int
	waitingTerm     int

	//volatile state on leaders
	nextindex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == State(1)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	//if err != nil {
	//	DPrintfAtRaft("[%d] failed", rf.me)
	//}
	data := w.Bytes()
	//Debug(dPersist, "[%d]Save persist data {term:[%v] voteFor:[%v] log:[%v]}", rf.me, rf.currentTerm, rf.votedFor, rf.log.log)
	rf.persister.Save(data, rf.snapshot)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var lastIncludeIndex int
	var lastIncludeTerm int
	var curTerm int
	var voteFor int
	var log []Entry

	errCurTerm := d.Decode(&curTerm)
	if errCurTerm != nil {
		Debug(dWarn, "[%d][Decode] CurTerm decode failed", rf.me)
		return
	}
	errVoteFor := d.Decode(&voteFor)
	if errVoteFor != nil {
		Debug(dWarn, "[%d][Decode] VoteFor decode failed", rf.me)
		return
	}
	errLog := d.Decode(&log)
	if errLog != nil {
		Debug(dWarn, "[%d][Decode] Log decode failed", rf.me)
		return
	}

	errIncludeindex := d.Decode(&lastIncludeIndex)
	if errIncludeindex != nil {
		Debug(dWarn, "[%d][Decode] IncludeIndex decode failed", rf.me)
		return
	}
	errIncludeTerm := d.Decode(&lastIncludeTerm)
	if errIncludeTerm != nil {
		Debug(dWarn, "[%d][Decode] Includeterm decode failed", rf.me)
		return
	}
	rf.currentTerm = curTerm
	rf.votedFor = voteFor
	rf.log.log = log
	rf.lastIncludeTerm = lastIncludeTerm
	rf.lastIncludeIndex = lastIncludeIndex
	Debug(dPersist, "[%d][Persister] read persistent date {term = [%d],votefor=[%d],log=[%v]}", rf.me, rf.currentTerm, rf.votedFor, rf.log.log)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastApplied = 0

	if rf.lastApplied+1 <= rf.log.start() {
		rf.lastApplied = rf.log.start()
	}

	for !rf.killed() {
		if rf.waitingSnapshot != nil {
			am := ApplyMsg{
				CommandValid:  false,
				Command:       nil,
				CommandIndex:  0,
				SnapshotValid: true,
				Snapshot:      rf.waitingSnapshot,
				SnapshotTerm:  rf.waitingTerm,
				SnapshotIndex: rf.waitingIndex,
			}
			rf.waitingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- am
			rf.mu.Lock()
		} else if rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 <= rf.log.lastindex() &&
			rf.lastApplied+1 > rf.log.start() {
			rf.lastApplied += 1
			am := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log.entry(rf.lastApplied).Command,
			}
			rf.mu.Unlock()
			Debug(dCommit, "[%v][ApplyMsg] Peer[%v] apply message:[%v]", rf.me, rf.me, am)
			rf.applyCh <- am
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) signalApplierL() {
	rf.applyCond.Broadcast()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	e := Entry{rf.currentTerm, command}
	index := rf.log.lastindex() + 1
	rf.log.append(e)
	Debug(dClient, "[%d] Client request for append new log:{index:[%d] term:[%v] command:[%v]}", rf.me, index, rf.currentTerm, command)
	rf.persist()
	//fmt.Println("a client send")
	rf.sendAppendsL(false)

	// Your code here (2B).
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) toCandidate() {
	//There is two status to change status to candidate
	//1.As a follower heartbeat timeout and change to candidate
	//2.As a candidate heartbeat timeout and start new candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	} else {
		rf.state = Candidate
		Debug(dInfo, "[%d] -> [Candidate]", rf.me)
		return
	}
}

func (rf *Raft) toCandidateL() {
	//There is two status to change status to candidate
	//1.As a follower heartbeat timeout and change to candidate
	//2.As a candidate heartbeat timeout and start new candidate
	if rf.state == Leader {
		return
	} else {
		rf.state = Candidate
		Debug(dInfo, "[%d] -> [Candidate]", rf.me)
		return
	}
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	} else {
		rf.state = Leader
		go rf.ticker()
		Debug(dInfo, "[%d] -> [Leader]", rf.me)
		return
	}
}

func (rf *Raft) toLeaderL() {
	if rf.state != Candidate {
		return
	} else {
		rf.state = Leader
		go rf.ticker()
		Debug(dInfo, "[%d] -> [Leader]", rf.me)
		return
	}
}

func (rf *Raft) toFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Follower {
		return
	} else {
		rf.state = Follower
		Debug(dInfo, "[%d] -> [follower]", rf.me)
		return
	}
}

func (rf *Raft) toFollowerL() {
	if rf.state == Follower {
		return
	} else {
		rf.state = Follower
		Debug(dInfo, "[%d] -> [follower]", rf.me)
		return
	}
}

func (rf *Raft) updateTermL(term int) {
	Debug(dWarn, "[%v] New Leader has been detected at term[%v],so [%v]->follower", rf.me, term, rf.me)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:               sync.Mutex{},
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		currentTerm:      0,
		votedFor:         -1,
		commitIndex:      0,
		lastApplied:      0,
		lastIncludeIndex: 0,
		lastIncludeTerm:  0,
		state:            Follower,
		snapshot:         nil,
		snapshotIndex:    0,
		snapshotTerm:     0,
		waitingSnapshot:  nil,
		waitingIndex:     0,
		waitingTerm:      0,
	}
	// Your initialization code here (2A, 2B, 2C).
	Debug(dClient, "[%v]Raft service node start", rf.me)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.setElectionTime()
	rf.log = mkEmptyLog()

	rf.nextindex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextindex[i] = 1
		rf.matchIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//read the persistent snapshot
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.log.entry(rf.log.start()).Term = rf.lastIncludeTerm
	rf.log.index0 = rf.lastIncludeIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
