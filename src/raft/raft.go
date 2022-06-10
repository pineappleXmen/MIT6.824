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
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type State int

const (
	Follower State = iota
	Leader
	Candidate
	Dead
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	CurrentTerm          int
	VoteFor              int
	Log                  []LogInfo
	CurState             State
	ElectionTimeoutTimer time.Time
	CommitIndex          int
	LastApplied          int

	//2B
	ApplyCh    chan ApplyMsg
	LeaderCond sync.Cond

	NextIndex  []int
	MatchIndex []int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogInfo struct {
	Command  interface{}
	LogIndex int
	LogTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.CurState == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurState != Leader {
		return index, term, false
	}
	appendLog := LogInfo{
		Command:  command,
		LogIndex: len(rf.Log),
		LogTerm:  rf.CurrentTerm,
	}
	rf.Log = append(rf.Log, appendLog)

	return len(rf.Log), rf.CurrentTerm, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []LogInfo
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) ToCandidate() {
	rf.CurState = Candidate
	rf.VoteFor = rf.me
	rf.CurrentTerm++
	rf.ElectionTimeoutTimer = time.Now()
}
func (rf *Raft) ToFollower() {
	rf.CurState = Follower
	rf.VoteFor = -1
}
func (rf *Raft) ToLeader() {
	rf.CurState = Leader
	rf.NextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = rf.NextIndex[rf.me]
	}
	//rf.CommitIndex = make([]int, len(rf.peers))
}

func (rf *Raft) AttempHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.CurState != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:     rf.CurrentTerm,
				LeaderId: rf.me,
				//PrevLogIndex: rf.NextIndex[index] - 1,
				//PreLogTerm:   rf.Log[rf.NextIndex[index]-1].LogTerm,
				//Entries:      make([]LogInfo, 0),
				//LeaderCommit: rf.CommitIndex,
				PrevLogIndex: 0,
				PreLogTerm:   0,
				Entries:      nil,
				LeaderCommit: 0,
			}
			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.mu.Unlock()
			go func(index int) {
				log.Printf("[%d] is sending heart beat to %d at term [%d]", rf.me, index, rf.CurrentTerm)
				ok := rf.sendAppendEntries(index, args, reply)
				if !ok {
					log.Printf("[%d] send appendentries to %d failed", rf.me, index)
				}
				if reply.Success {
					//rf.MatchIndex[index] = args.PrevLogIndex + len(args.Entries)
					//rf.NextIndex[index] = rf.MatchIndex[index] + 1
				} else {
					rf.mu.Lock()
					//rf.NextIndex[index] = args.PrevLogIndex
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.ToFollower()
					}
					rf.mu.Unlock()
				}
			}(index)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
	return ok
}
func (rf *Raft) AppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var ok = true
	rep := AppendEntriesReply{
		Term:    rf.CurrentTerm,
		Success: true,
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		log.Printf("[%d] knew that %d is the new leader at term %d to follower", rf.me, args.LeaderId, args.Term)
		rf.ToFollower()

	} else if args.Term < rf.CurrentTerm {
		rep.Success = false
		ok = false
		rep.Term = rf.CurrentTerm
	} else if args.Entries != nil && args.Entries[args.PrevLogIndex].LogTerm != rf.Log[args.PrevLogIndex].LogTerm {
		rep.Success = false
		ok = false
	}

	//for i, logEntry := range args.Entries {
	//	index := args.PrevLogIndex + i + 1
	//	if index > len(rf.Log)-1 {
	//		rf.Log = append(rf.Log, logEntry)
	//	} else {
	//		if rf.Log[index].LogTerm != logEntry.LogTerm {
	//			rf.Log = rf.Log[:index]
	//			rf.Log = append(rf.Log, logEntry)
	//		}
	//	}
	//}
	//
	//if args.LeaderCommit > rf.CommitIndex {
	//	rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	//}

	reply = &rep
	if ok {
		rf.ElectionTimeoutTimer = time.Now()
	}
	log.Printf("[%d] got the appendentries request from leader %d at term %d", rf.me, args.LeaderId, args.Term)
}

func min(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

// The ElectionTimeoutticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ElectionTimeoutticker() {
	for rf.killed() == false {
		if rf.CurState != Leader {
			if time.Since(rf.ElectionTimeoutTimer) > 300*time.Millisecond {
				rf.AttemptElection()
			}
		}
		time.Sleep(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		//} else {
		//	time.Sleep(150 * time.Millisecond)
		//}
		//Your code here to check if a leader election should
		//be started and to randomize sleeping time using

	}
}

func (rf *Raft) AttemptElection() bool {
	rf.mu.Lock()
	rf.ToCandidate()
	log.Printf("[%d] attempt to start election at term %d", rf.me, rf.CurrentTerm)
	votes := 1
	done := false
	term := rf.CurrentTerm
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.mu.Lock()
		log.Printf("[%d] sending request vote to %d", rf.me, index)
		args := RequestVoteArgs{
			Term:        term,
			CandidateId: rf.me,
		}
		var reply RequestVoteReply
		rf.mu.Unlock()
		go func(index int) {
			ok := rf.sendRequestVote(index, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			voteGranted := reply.VoteGranted
			if !ok {
				log.Printf("[%d] sending request vote to %d is failed", rf.me, index)
			}
			if !voteGranted {
				return
			}
			votes++
			log.Printf("[%d] got the vote from %d", rf.me, index)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.CurState != Candidate || rf.CurrentTerm != term {
				return
			}
			log.Printf("[%d] got enough vote become the leader of term [%d]", rf.me, rf.CurrentTerm)
			rf.ToLeader()
			go rf.AttempHeartBeat()
			return
		}(index)
	}
	return true
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
	CurrentTerm int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("[%d] receive the request vote from %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		return
	} else if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.CurState = Follower
		rf.VoteFor = -1
	}
	//log.Printf("Now the %d Votefor is %d", rf.me, rf.VoteFor)
	rep := RequestVoteReply{
		CurrentTerm: rf.CurrentTerm,
		VoteGranted: false,
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		//&& rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm
		rep.VoteGranted = true
		rf.VoteFor = args.CandidateId
	}
	*reply = rep
	log.Printf("[%d]  vote for %d is %t", rf.me, args.CandidateId, rep.VoteGranted)
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	log.Println(len(rf.Log))
	if len(rf.Log) < 1 {
		return true
	} else {
		return term > rf.Log[len(rf.Log)-1].LogTerm || ((term == rf.Log[len(rf.Log)-1].LogTerm) && (index >= rf.Log[len(rf.Log)-1].LogIndex))
	}
}

func (rf *Raft) ApplyLog() {
	for !rf.killed() && rf.CommitIndex > rf.LastApplied {
		rf.LastApplied++
		entry := rf.Log[rf.LastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.LogIndex,
		}
		rf.ApplyCh <- msg
	}
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
func (rf *Raft) StateMonitor() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.CurState = Dead
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:                   sync.Mutex{},
		peers:                peers,
		persister:            persister,
		me:                   me,
		dead:                 0,
		CurrentTerm:          0,
		VoteFor:              -1,
		Log:                  make([]LogInfo, 0),
		CurState:             Follower,
		ElectionTimeoutTimer: time.Now(),
		CommitIndex:          0,
		LastApplied:          0,
		ApplyCh:              applyCh,
		NextIndex:            nil,
		MatchIndex:           nil,
	}
	rf.LeaderCond = *sync.NewCond(&rf.mu)

	//rf.peers = peers
	//rf.persister = persister
	//rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	//_, isleader := rf.GetState()
	//if !isleader {
	go rf.ElectionTimeoutticker()
	//}
	go rf.StateMonitor()
	return rf
}
