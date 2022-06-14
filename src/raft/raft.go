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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"sort"
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
	CurrentTerm int
	VoteFor     int
	Log         []LogInfo

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	errCurTerm := e.Encode(rf.CurrentTerm)
	if errCurTerm != nil {
		log.Printf("[%d][Encode] CurrentTerm encode failed", rf.me)
		return
	}
	errVotefor := e.Encode(rf.VoteFor)
	if errVotefor != nil {
		log.Printf("[%d][Encode] VoteFor encode failed", rf.me)
		return
	}
	errLog := e.Encode(rf.Log)
	if errLog != nil {
		log.Printf("[%d][Encode] Log encode failed", rf.me)
		return
	}
	data := w.Bytes()
	log.Printf("[%d][WirteLogPersister] persister write info to disk [CurTerm] = [%d],[votefor]=[%d],[log]=%v", rf.me, rf.CurrentTerm, rf.VoteFor, rf.Log)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 2 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var currentTerm int
	var voteFor int
	var logs []LogInfo
	errCurTerm := d.Decode(&currentTerm)
	if errCurTerm != nil {
		log.Printf("[%d][Decode] CurTerm decode failed", rf.me)
		return
	}
	errVoteFor := d.Decode(&voteFor)
	if errVoteFor != nil {
		log.Printf("[%d][Decode] VoteFor decode failed", rf.me)
		return
	}
	errLog := d.Decode(&logs)
	if errLog != nil {
		log.Printf("[%d][Decode] Log decode failed", rf.me)
		return
	}
	rf.CurrentTerm = currentTerm
	rf.VoteFor = voteFor
	rf.Log = logs
	log.Printf("[%d][ReadLogPersister] persister got info from disk [CurTerm] = [%d],[votefor]=[%d],[log]=%v", rf.me, currentTerm, voteFor, logs)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 持久化快照信息
	rf.persist()
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
	isLeader := true
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader = rf.CurState == Leader
	if isLeader {
		appendLog := LogInfo{
			Command:  command,
			LogIndex: len(rf.Log),
			LogTerm:  rf.CurrentTerm,
		}
		log.Printf("[%d]<-[command %d] is added to Logindex %d at term %d", rf.me, command, len(rf.Log), rf.CurrentTerm)
		rf.Log = append(rf.Log, appendLog)
		rf.persist()
	}
	return index, term, isLeader
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
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) ToCandidate() {
	defer rf.persist()
	rf.CurState = Candidate
	rf.VoteFor = rf.me
	rf.CurrentTerm++
	rf.ElectionTimeoutTimer = time.Now()
}
func (rf *Raft) ToFollower() {
	defer rf.persist()
	rf.CurState = Follower
	rf.VoteFor = -1
}
func (rf *Raft) ToLeader() {
	defer rf.persist()
	rf.CurState = Leader
	rf.NextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.NextIndex[i] = len(rf.Log)
	}
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.MatchIndex[i] = 0
	}
}

func (rf *Raft) AttemptHeartBeat() {
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

			entries := make([]LogInfo, 0)
			entries = append(entries, rf.Log[rf.NextIndex[index]:]...)
			args := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.NextIndex[index] - 1,
				PreLogTerm:   rf.Log[rf.NextIndex[index]-1].LogTerm,
				Entries:      entries,
				LeaderCommit: rf.CommitIndex,
			}
			argsEmpty := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.NextIndex[index] - 1,
				PreLogTerm:   rf.Log[rf.NextIndex[index]-1].LogTerm,
				Entries:      nil,
				LeaderCommit: rf.CommitIndex,
			}
			reply := &AppendEntriesReply{
				Term:    0,
				Success: false,
			}
			rf.mu.Unlock()
			go func(index int) {
				var ok bool
				if rf.NextIndex[index] == len(rf.Log) {
					log.Printf("[%d]->[%d][HeartBeat] at term [%d]", rf.me, index, rf.CurrentTerm)
					ok = rf.sendAppendEntries(index, argsEmpty, reply)
				} else {
					log.Printf("[%d]->[%d][AppendEntries] at term [%d]", rf.me, index, rf.CurrentTerm)
					ok = rf.sendAppendEntries(index, args, reply)
				}
				if !ok {
					log.Printf("[%d]->[%d][HeartBeat] Failed to Connection", rf.me, index)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Success {
					log.Printf("[%d]->[%d][HeartBeatReply] Sync Success! current index is %d", index, rf.me, rf.NextIndex[index])
					rf.MatchIndex[index] = args.PrevLogIndex + len(args.Entries)
					rf.NextIndex[index] = rf.MatchIndex[index] + 1
					rf.applyMsg()
				} else if ok && !reply.Success {
					//if reply.ConflictTerm >= 1 {
					//	searchTerm := -1
					//	for i := reply.ConflictIndex; i > 1; i-- {
					//		if rf.Log[i].LogTerm == reply.ConflictTerm {
					//			searchTerm = i
					//			break
					//		}
					//	}
					//	if searchTerm == -1 {
					//		rf.NextIndex[index] = reply.ConflictIndex
					//	} else {
					//		rf.NextIndex[index] = searchTerm + 1
					//	}
					//} else {
					//	log.Printf("[%d] conflictindex [%d]prevlogindex", reply.ConflictIndex, args.PrevLogIndex)
					//	rf.NextIndex[index] = min(reply.ConflictIndex, args.PrevLogIndex)
					//	if rf.NextIndex[index] < 1 {
					//		rf.NextIndex[index] = 1
					//	}
					//--------------------------------
					if reply.ConflictTerm < 0 {
						rf.NextIndex[index] = reply.ConflictIndex
					} else {
						searchTerm := -1
						for i := reply.ConflictIndex; i >= 1; i-- {
							if rf.Log[i].LogTerm == reply.ConflictTerm {
								searchTerm = i
								break
							}
						}
						if searchTerm == -1 {
							rf.NextIndex[index] = reply.ConflictIndex
						} else {
							rf.NextIndex[index] = searchTerm + 1
						}
					}
					if rf.NextIndex[index] <= 0 {
						rf.NextIndex[index] = 1
					}
					log.Printf("[%d]->[%d][HeartBeatReply] just failed and nextIndex change to %d", index, rf.me, rf.NextIndex[index])
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						log.Printf("[%d]->[%d][HeartBeatReply] knew that new leader has been elected at term %d", index, rf.me, rf.CurrentTerm)
						rf.ToFollower()
						rf.ElectionTimeoutTimer = time.Now()
						return
					}
				}
			}(index)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) applyMsg() {
	findmiddleNum := make([]int, len(rf.MatchIndex))
	copy(findmiddleNum, rf.MatchIndex)
	findmiddleNum[rf.me] = len(rf.Log) - 1
	sort.Ints(findmiddleNum)
	mid := findmiddleNum[len(rf.peers)/2]
	log.Printf("[%d] mid[%d]", rf.me, mid)
	if rf.CurState == Leader && mid > rf.CommitIndex && rf.Log[mid].LogTerm == rf.CurrentTerm {
		rf.CommitIndex = mid
		rf.ApplyLog()
		log.Printf("[%d][ApplyLog] CommitIndex Increases to %d", rf.me, mid)
	}
}

func (rf *Raft) ApplyLog() {
	for !rf.killed() && rf.CommitIndex > rf.LastApplied {
		rf.LastApplied += 1
		entry := rf.Log[rf.LastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.LogIndex,
		}
		log.Printf("[%d][ApplyLog] Applied %d at index %d to client applymsg is %v", rf.me, msg.Command, msg.CommandIndex, msg)
		rf.ApplyCh <- msg
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
		Term:          rf.CurrentTerm,
		Success:       true,
		ConflictIndex: -1,
		ConflictTerm:  -1,
	}

	if len(args.Entries) != 0 {
		log.Printf("[%d]->[%d][AppendEntriesReply]got the AppendEntriesRPC at term %d", rf.me, args.LeaderId, args.Term)
	} else {
		log.Printf("[%d]->[%d][HeartBeatReply]got the HeartBeatRPC at term %d", rf.me, args.LeaderId, args.Term)
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		log.Printf("[%d]->[%d][HeartBeatReply] knew that new leader has been elected at term %d so convert to follower", rf.me, args.LeaderId, args.Term)
		rf.ToFollower()
		ok = true
		rep.Success = false
	}
	if args.Term < rf.CurrentTerm {
		rep.Success = false
		ok = false
		rep.Term = rf.CurrentTerm
		log.Printf("[%d]->[%d][HeartBeatReply] heartbeat term is too old so failed", rf.me, args.LeaderId)
	} else if len(rf.Log)-1 < args.PrevLogIndex {
		rep.Success = false
		ok = true
		log.Printf("[%d]->[%d][HeartBeatReply] logs doesnot contain log at prevlogindex ", rf.me, args.LeaderId)
		rep.ConflictIndex = len(rf.Log)
		rep.ConflictTerm = -1
	} else if args.PreLogTerm != rf.Log[args.PrevLogIndex].LogTerm {
		rep.Success = false
		ok = true
		rep.ConflictTerm = rf.Log[args.PrevLogIndex].LogTerm

		for i := 1; i <= args.PrevLogIndex; i++ {
			if rf.Log[i].LogTerm == rep.ConflictTerm {
				rep.ConflictIndex = i
				break
			}
		}
		log.Printf("[%d]->[%d][HeartBeatReply] prevIndexLog term dont match me:[%d] sender:[%d]", rf.me, args.LeaderId, rf.Log[args.PrevLogIndex].LogTerm, args.PreLogTerm)
		if rep.ConflictIndex == -1 {
			rep.ConflictIndex = args.PrevLogIndex
		}
		//rf.Log = rf.Log[:rep.ConflictIndex] //delete logs after prevlogindex
		//rf.persist()
		log.Printf("[%d]->[%d] has delete the conflict logs after prevlog %d which at term %d", rf.me, args.LeaderId, rep.ConflictIndex, rep.ConflictTerm)
	}

	if len(args.Entries) != 0 && rep.Success {
		for i, logEntry := range args.Entries {
			index := args.PrevLogIndex + i + 1
			if index > len(rf.Log)-1 {
				rf.Log = append(rf.Log, logEntry)
			} else {
				if rf.Log[index].LogTerm != logEntry.LogTerm {
					rf.Log = rf.Log[:index]
					rf.Log = append(rf.Log, logEntry)
				}
			}
		}
		rf.persist()
	}
	log.Printf("[%d][AppendLog] is update %d log at term %d", rf.me, len(args.Entries), rf.CurrentTerm)
	log.Printf("[%d][AppendLog] now Log entry has %d logs", rf.me, len(rf.Log))

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.Log)-1)
	}
	*reply = rep
	//reply = &rep
	if ok {
		rf.ElectionTimeoutTimer = time.Now()
	}
	if rep.Success {
		rf.ApplyLog()
	}
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
		//Your code here to check if a leader election should
		//be started and to randomize sleeping time using

	}
}

func (rf *Raft) AttemptElection() bool {
	rf.mu.Lock()
	rf.ToCandidate()
	log.Printf("[%d][StartElection] attempt to start election at term %d", rf.me, rf.CurrentTerm)
	votes := 1
	done := false
	term := rf.CurrentTerm
	rf.mu.Unlock()
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.mu.Lock()
		log.Printf("[%d]->[%d][StartElection] sending requestVoteRPC ", rf.me, index)
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.Log) - 1,
			LastLogTerm:  rf.Log[len(rf.Log)-1].LogTerm,
		}
		var reply RequestVoteReply
		rf.mu.Unlock()
		go func(index int) {
			ok := rf.sendRequestVote(index, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			voteGranted := reply.VoteGranted
			if !ok {
				log.Printf("[%d]->[%d][StartElection] sending requestVoteRPC failed", rf.me, index)
			}
			if !voteGranted {
				return
			}
			votes++
			log.Printf("[%d]->[%d][CountingVotes] got the YES vote", rf.me, index)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			done = true
			if rf.CurState != Candidate || rf.CurrentTerm != term {
				return
			}
			log.Printf("[%d][WinElection] got enough vote become the leader of term [%d]", rf.me, rf.CurrentTerm)
			rf.ToLeader()
			go rf.AttemptHeartBeat()
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
	log.Printf("[%d]->[%d][RequestVoteReply] receive the requestVoteRPC ", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rep := RequestVoteReply{
		CurrentTerm: rf.CurrentTerm,
		VoteGranted: false,
	}
	if args.Term < rf.CurrentTerm {
		*reply = rep
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.ToFollower()
	}
	//log.Printf("Now the %d Votefor is %d", rf.me, rf.VoteFor)

	if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rep.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.persist()
	}
	*reply = rep
	log.Printf("[%d]->[%d][ElectionVote] vote result is %t", rf.me, args.CandidateId, rep.VoteGranted)
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	return term > rf.Log[len(rf.Log)-1].LogTerm ||
		((term == rf.Log[len(rf.Log)-1].LogTerm) && (index >= rf.Log[len(rf.Log)-1].LogIndex))
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []LogInfo
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRequest", args, reply)
	return ok
}
func (rf *Raft) AttemptInstallSnapshot() {
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          0,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		Offset:            0,
		Data:              nil,
		Done:              false,
	}
	reply := InstallSnapshotReply{
		Term: 0,
	}
	for index, _ := range rf.peers {
		go func(index int) {
			ok := rf.sendInstallSnapshotRPC(index, &args, &reply)
			if ok != false {
				log.Printf("[%d][InstallSanpshot] Failed to Send Snapshot", rf.me)
			}
		}(index)
	}

}
func (rf *Raft) InstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rep := InstallSnapshotReply{
		Term: rf.CurrentTerm,
	}
	if args.Term < rf.CurrentTerm {

	} else if args.Offset == 0 {

	}

	*reply = rep
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
	initLog := LogInfo{
		Command:  nil,
		LogIndex: 0,
		LogTerm:  0,
	}
	rf.Log = append(rf.Log, initLog)
	rf.LeaderCond = *sync.NewCond(&rf.mu)
	//rf.persist()
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
