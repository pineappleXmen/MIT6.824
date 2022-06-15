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
	CommitCond *sync.Cond

	NextIndex  []int
	MatchIndex []int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2D
	LastIncludeIndex int
	LastIncludeTerm  int
	SnapshotData     []byte
}

type LogInfo struct {
	Command  interface{}
	LogIndex int
	LogTerm  int
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

//-------------------initialization-----------------
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
		LastIncludeIndex:     0,
		LastIncludeTerm:      0,
		SnapshotData:         nil,
	}
	initLog := LogInfo{
		Command:  nil,
		LogIndex: 0,
		LogTerm:  0,
	}
	rf.Log = append(rf.Log, initLog)
	rf.CommitCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	if rf.LastIncludeIndex > 0 {
		rf.LastApplied = rf.LastIncludeIndex
	}
	rf.CommitCond = sync.NewCond(&rf.mu)
	go rf.ApplyLog()

	//if rf.CurState != Leader {
	go rf.ElectionTimeoutticker()
	//}
	go rf.StateMonitor()
	return rf
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
	index := rf.getLastIndex() + 1
	term := rf.CurrentTerm
	isLeader = rf.CurState == Leader
	if isLeader {
		appendLog := LogInfo{
			Command:  command,
			LogIndex: rf.getLastIndex() + 1,
			LogTerm:  rf.CurrentTerm,
		}
		log.Printf("[%d]<-[command %d] is added to Logindex %d at term %d", rf.me, command, rf.getLastIndex()+1, rf.CurrentTerm)
		rf.Log = append(rf.Log, appendLog)
		rf.persist()
	}
	return index, term, isLeader
}

//----------HeartBeat RPC -----------

func (rf *Raft) AttemptHeartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.CurState != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.NextIndex[index]-1 < rf.LastIncludeIndex {
				go rf.AttemptInstallSnapshot(index)
				rf.mu.Unlock()
				continue
			}
			entries := make([]LogInfo, 0)
			if rf.getLastIndex() >= rf.NextIndex[index] {
				entries = append(entries, rf.Log[rf.getLogIndex(rf.NextIndex[index]):]...)
			}
			prevlogindex, prevlogterm := rf.getPrevLogInfo(index)
			args := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevlogindex,
				PreLogTerm:   prevlogterm,
				Entries:      entries,
				LeaderCommit: rf.CommitIndex,
			}
			argsEmpty := &AppendEntriesArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevlogindex,
				PreLogTerm:   prevlogterm,
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
				if rf.getLastIndex() >= rf.NextIndex[index] {
					log.Printf("[%d]->[%d][AppendEntries] with %d entries at term [%d]", rf.me, index, len(args.Entries), rf.CurrentTerm)
					ok = rf.sendAppendEntries(index, args, reply)
				} else {
					log.Printf("[%d]->[%d][HeartBeat] at term [%d]", rf.me, index, rf.CurrentTerm)
					ok = rf.sendAppendEntries(index, argsEmpty, reply)
				}
				if !ok {
					log.Printf("[%d]->[%d][HeartBeat] Failed to Connection", rf.me, index)
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Success {
					log.Printf("[%d]->[%d][HeartBeatReply] Sync Success! current index is %d", index, rf.me, rf.NextIndex[index]-1)
					rf.MatchIndex[index] = args.PrevLogIndex + len(args.Entries)
					rf.NextIndex[index] = rf.MatchIndex[index] + 1
					rf.applyMsg()
					rf.CommitCond.Broadcast()
				} else if ok && !reply.Success {
					if reply.ConflictTerm < 0 || args.PrevLogIndex < reply.ConflictIndex {
						rf.NextIndex[index] = reply.ConflictIndex
					} else {
						searchTerm := -1
						for i := rf.getLogIndex(reply.ConflictIndex); i >= 1; i-- {
							if rf.Log[i].LogTerm == reply.ConflictTerm {
								searchTerm = i
								break
							}
						}
						if searchTerm == -1 {
							rf.NextIndex[index] = reply.ConflictIndex
						} else {
							rf.NextIndex[index] = searchTerm + 1 + rf.LastIncludeIndex
						}
					}
					if rf.NextIndex[index] <= 0 {
						rf.NextIndex[index] = rf.LastIncludeIndex + 1
					}
					log.Printf("[%d]->[%d][HeartBeatReply] just failed and nextIndex change to %d", index, rf.me, rf.NextIndex[index])
					if reply.Term > rf.CurrentTerm || rf.NextIndex[index] > rf.getLastIndex() {
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
		log.Printf("[%d]->[%d][AppendEntriesReply]got the AppendEntriesRPC with %d entries at term %d", rf.me, args.LeaderId, len(args.Entries), args.Term)
	} else {
		log.Printf("[%d]->[%d][HeartBeatReply]got the HeartBeatRPC at term %d", rf.me, args.LeaderId, args.Term)
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		log.Printf("[%d]->[%d][HeartBeatReply] knew that new leader has been elected at term %d So [%d] convert to follower", rf.me, args.LeaderId, args.Term, rf.me)
		rf.ToFollower()
		ok = true
		rep.Success = false
	}
	if rf.LastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = rf.getLastIndex() + 1
		return
	}
	if args.Term < rf.CurrentTerm {
		rep.Success = false
		ok = false
		rep.Term = rf.CurrentTerm
		log.Printf("[%d]->[%d][HeartBeatReply] [%d] term is too old so failed", rf.me, args.LeaderId, args.LeaderId)
	} else if rf.getLastIndex() < args.PrevLogIndex {
		rep.Success = false
		ok = true
		log.Printf("[%d]->[%d][HeartBeatReply] logs doesnot contain log at prevlogindex ", rf.me, args.LeaderId)
		rep.ConflictIndex = rf.getLastIndex() + 1
		rep.ConflictTerm = -1
	} else if args.PreLogTerm != rf.getLogTerm(args.PrevLogIndex) {
		rep.Success = false
		ok = true
		rep.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		for i := rf.LastIncludeIndex; i <= args.PrevLogIndex; i++ {
			if rf.Log[rf.getLogIndex(i)].LogTerm == rep.ConflictTerm {
				rep.ConflictIndex = i
				break
			}
		}
		log.Printf("[%d]->[%d][HeartBeatReply] prevIndexLog term dont match me:[%d] sender:[%d]", rf.me, args.LeaderId, rf.getLogTerm(args.PrevLogIndex), args.PreLogTerm)
		if rep.ConflictIndex == -1 {
			rep.ConflictIndex = args.PrevLogIndex
		}
		//rf.Log = rf.Log[:rep.ConflictIndex] //delete logs after prevlogindex
		//rf.persist()
		//log.Printf("[%d]->[%d] has delete the conflict logs after prevlog %d which at term %d", rf.me, args.LeaderId, rep.ConflictIndex, rep.ConflictTerm)
	}

	if len(args.Entries) != 0 && rep.Success {
		for i, logEntry := range args.Entries {
			index := args.PrevLogIndex + i + 1
			if index > rf.getLastIndex() {
				rf.Log = append(rf.Log, logEntry)
			} else {
				if rf.Log[rf.getLogIndex(index)].LogTerm != logEntry.LogTerm {
					rf.Log = rf.Log[:rf.getLogIndex(index)]
					rf.Log = append(rf.Log, logEntry)
				}
			}
		}
		rf.persist()
	}

	log.Printf("[%d][AppendLog] now Log entry has %d logs which is %v", rf.me, rf.getLastIndex(), rf.Log)

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	*reply = rep
	//reply = &rep
	if ok {
		rf.ElectionTimeoutTimer = time.Now()
	}
	if rep.Success {
		rf.CommitCond.Broadcast()
	}
}

//--------RequestVote RPC---------------
func (rf *Raft) AttemptElection() bool {
	rf.mu.Lock()
	rf.ToCandidate()
	log.Printf("[%d][StartElection] attempts to start election at term %d", rf.me, rf.CurrentTerm)
	votes := 1
	done := false
	term := rf.CurrentTerm
	rf.mu.Unlock()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.mu.Lock()
		log.Printf("[%d]->[%d][StartElection] sending requestVoteRPC ", rf.me, index)
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastIndex(),
			LastLogTerm:  rf.getLastTerm(),
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("[%d]->[%d][RequestVoteReply] received the requestVoteRPC ", rf.me, args.CandidateId)
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
	log.Printf("[%d]->[%d][ElectionVote] votefor result is %t", rf.me, args.CandidateId, rep.VoteGranted)
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

//-------------------persister------------

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//errCurTerm := e.Encode(rf.CurrentTerm)
	//if errCurTerm != nil {
	//	log.Printf("[%d][Encode] CurrentTerm encode failed", rf.me)
	//	return
	//}
	//errVotefor := e.Encode(rf.VoteFor)
	//if errVotefor != nil {
	//	log.Printf("[%d][Encode] VoteFor encode failed", rf.me)
	//	return
	//}
	//errLog := e.Encode(rf.Log)
	//if errLog != nil {
	//	log.Printf("[%d][Encode] Log encode failed", rf.me)
	//	return
	//}
	//data := w.Bytes()
	//log.Printf("[%d][WirteLogPersister] persister write info to disk [CurTerm] = [%d],[votefor]=[%d],[log]=%v", rf.me, rf.CurrentTerm, rf.VoteFor, rf.Log)
	//rf.persister.SaveRaftState(data)
	data := rf.persistData()
	log.Printf("[%d][LogPersister] persister write info to disk [CurTerm] = [%d],[votefor]=[%d],[lastInclude]=[%d],lastIncludeTerm=[%d],[log]=[%v]", rf.me, rf.CurrentTerm, rf.VoteFor, rf.LastIncludeIndex, rf.LastIncludeTerm, rf.Log)
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
	var lastIncludeIndex int
	var lastIncludeTerm int
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
	errIncludeindex := d.Decode(&lastIncludeIndex)
	if errIncludeindex != nil {
		log.Printf("[%d][Decode] IncludeIndex decode failed", rf.me)
		return
	}
	errIncludeTerm := d.Decode(&lastIncludeTerm)
	if errIncludeTerm != nil {
		log.Printf("[%d][Decode] Includeterm decode failed", rf.me)
		return
	}
	rf.CurrentTerm = currentTerm
	rf.VoteFor = voteFor
	rf.Log = logs
	rf.LastIncludeTerm = lastIncludeTerm
	rf.LastIncludeIndex = lastIncludeIndex
	log.Printf("[%d][ReadLogPersister] persister got info from disk [CurTerm] = [%d],[votefor]=[%d],[lastInclude]=[%d],lastIncludeTerm=[%d],[log]=[%v]", rf.me, rf.CurrentTerm, rf.VoteFor, rf.LastIncludeIndex, rf.LastIncludeTerm, rf.Log)
}

//--------------SnapshotRPC--------------

func (rf *Raft) AttemptInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludeIndex,
		LastIncludedTerm:  rf.LastIncludeTerm,
		Offset:            0,
		Data:              rf.persister.ReadSnapshot(),
		Done:              false,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	go func(index int) {
		ok := rf.sendInstallSnapshotRPC(server, &args, &reply)
		if !ok {
			log.Printf("[%d][InstallSnapshot] Failed to Send Snapshot", rf.me)
		} else {
			rf.mu.Lock()
			log.Printf("[%d][InstallSnapshot] InstallSnapshot Success!", rf.me)
			if rf.CurState != Leader || rf.CurrentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.CurrentTerm {
				log.Printf("[%d][InstallSnapshot] knew that new leader has been elected to follower", rf.me)
				rf.ToFollower()
				rf.persist()
				rf.ElectionTimeoutTimer = time.Now()
				rf.mu.Unlock()
				return
			}
			rf.MatchIndex[server] = args.LastIncludedIndex
			rf.NextIndex[server] = args.LastIncludedIndex + 1
			log.Printf("[%d][InstallSnapshot] snapshot install success [%d]'s nextIndex change to [%d]", rf.me, server, args.LastIncludedIndex+1)
			rf.mu.Unlock()
			return
		}
	}(server)
}
func (rf *Raft) InstallSnapshotRequest(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	rf.CurrentTerm = args.Term
	reply.Term = args.Term
	rf.ToFollower()
	rf.persist()
	rf.ElectionTimeoutTimer = time.Now()

	if args.LastIncludedIndex < rf.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}
	index := args.LastIncludedIndex
	tempLog := make([]LogInfo, 0)
	tempLog = append(tempLog, LogInfo{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.getLog(i))
	}
	rf.LastIncludeIndex = args.LastIncludedIndex
	rf.LastIncludeTerm = args.LastIncludedTerm
	rf.Log = tempLog
	if index > rf.CommitIndex {
		rf.CommitIndex = index
		rf.CommitCond.Broadcast()
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
		log.Printf("[%d] lastapplied at installrequ %d", rf.me, rf.LastApplied)
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.LastIncludeTerm,
		SnapshotIndex: rf.LastIncludeIndex,
	}
	rf.mu.Unlock()
	log.Printf("[%d][InstallSnapshotApply] Success apply data and now log index is %d", rf.me, rf.LastIncludeIndex)
	log.Printf("[%d][InstallSnapshotApply] lastapplied is  %d", rf.me, rf.LastApplied)
	rf.ApplyCh <- msg

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
	if rf.killed() {
		return
	}
	log.Printf("[%d][SnapShot] SnapShot starts index is %d", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%d][Snapshot]Msg from server that index is %d", rf.me, index)
	if rf.LastIncludeIndex >= index || index > rf.CommitIndex {
		return
	}
	sLogs := make([]LogInfo, 0)
	sLogs = append(sLogs, LogInfo{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.getLog(i))
	}
	if index == rf.getLastIndex()+1 {
		rf.LastIncludeTerm = rf.getLastTerm()
	} else {
		rf.LastIncludeTerm = rf.getLogTerm(index)
	}

	rf.LastIncludeIndex = index
	rf.Log = sLogs
	log.Printf("[%d][Snapshot]Success to delete nodes in logs before snapshot current log is %v", rf.me, rf.Log)

	if index > rf.CommitIndex {
		rf.CommitIndex = index
		rf.CommitCond.Broadcast()
	}
	if index > rf.LastApplied {
		rf.LastApplied = index
	}
	log.Printf("[%d][SnapShot] commitindex change to %d", rf.me, rf.CommitIndex)
	log.Printf("[%d][SnapShot] lastapplied change to %d", rf.me, rf.LastApplied)
	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

//

//---------------Applymsg----------

func (rf *Raft) applyMsg() {
	findmiddleNum := make([]int, len(rf.MatchIndex))
	copy(findmiddleNum, rf.MatchIndex)
	findmiddleNum[rf.me] = rf.getLastIndex()
	sort.Ints(findmiddleNum)
	mid := findmiddleNum[len(rf.peers)/2]
	if rf.CurState == Leader && mid > rf.CommitIndex && rf.Log[rf.getLogIndex(mid)].LogTerm == rf.CurrentTerm {
		rf.CommitIndex = mid
		rf.CommitCond.Broadcast()
		log.Printf("[%d][CommitLog] CommitIndex Increases to %d and broadcast", rf.me, mid)
	}
}

func (rf *Raft) ApplyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.CommitCond.Wait()
		MessageQueue := make([]ApplyMsg, 0)
		for !rf.killed() && rf.CommitIndex > rf.LastApplied && rf.LastApplied < rf.getLastIndex() {
			rf.LastApplied += 1
			msg := ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       rf.getLog(rf.LastApplied).Command,
				CommandIndex:  rf.LastApplied,
			}
			MessageQueue = append(MessageQueue, msg)
		}
		rf.mu.Unlock()
		for _, msg := range MessageQueue {
			log.Printf("[%d][ApplyLog] Applied %d at index %d to client applymsg is %v", rf.me, msg.Command, msg.CommandIndex, msg)
			rf.ApplyCh <- msg
		}
	}
}

//----------------------Utils------------------------------

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
	ok := rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshotRPC(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRequest", args, reply)
	return ok
}

func (rf *Raft) getLog(index int) LogInfo {
	return rf.Log[index-rf.LastIncludeIndex]
}

func (rf *Raft) getLastIndex() int {
	return len(rf.Log) - 1 + rf.LastIncludeIndex
}

func (rf *Raft) getLogIndex(index int) int {
	return index - rf.LastIncludeIndex
}

func (rf *Raft) getLastTerm() int {
	if len(rf.Log)-1 == 0 {
		return rf.LastIncludeTerm
	} else {
		return rf.Log[len(rf.Log)-1].LogTerm
	}
}

func (rf *Raft) getLogTerm(index int) int {
	if index == rf.LastIncludeIndex {
		return rf.LastIncludeTerm
	}
	log.Printf("[%d]index : %d,lastincludeindex %d", rf.me, index, rf.LastIncludeIndex)
	return rf.Log[index-rf.LastIncludeIndex].LogTerm
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	newEntryBeginIndex := rf.NextIndex[server] - 1
	lastIndex := rf.getLastIndex()
	if newEntryBeginIndex == lastIndex+1 {
		newEntryBeginIndex = lastIndex
	}
	return newEntryBeginIndex, rf.getLogTerm(newEntryBeginIndex)
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	//if err != nil {
	//	log.Printf("[%d] failed", rf.me)
	//}
	data := w.Bytes()
	return data
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
		rf.NextIndex[i] = rf.getLastIndex() + 1
	}
	rf.MatchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.MatchIndex[i] = 0
	}
}

func (rf *Raft) isLogUpToDate(index int, term int) bool {
	return term > rf.Log[len(rf.Log)-1].LogTerm ||
		((term == rf.Log[len(rf.Log)-1].LogTerm) && (index >= rf.Log[len(rf.Log)-1].LogIndex))
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

func min(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}
