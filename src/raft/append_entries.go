package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendsL(heartbeat bool) {
	for i := range rf.peers {
		if i != rf.me {
			if rf.log.lastindex() > rf.nextindex[i] || heartbeat {
				rf.sendAppendL(i, heartbeat)
			}
		}
	}
}

func (rf *Raft) sendAppendL(peer int, heartbeat bool) {
	next := rf.nextindex[peer]
	if next <= rf.log.start() {
		next = rf.log.start() + 1
	}
	if next-1 > rf.log.lastindex() {
		next = rf.log.lastindex()
	}
	//should send snapshot
	if rf.nextindex[peer]-1 < rf.lastIncludeIndex {
		Debug(dSnap, "[%v]->[%v] update snapshot", rf.me, peer)
		rf.AttemptInstallSnapshotL(peer)
		return
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: next - 1,
		PreLogTerm:   rf.log.entry(next - 1).Term,
		Entries:      make([]Entry, rf.log.lastindex()-next+1),
		LeaderCommit: rf.commitIndex,
	}
	copy(args.Entries, rf.log.slice(next))

	go func() {
		var reply AppendEntriesReply
		if len(args.Entries) == 0 {
			Debug(dHeat, "[%v]->[%v][HeartBeat] Leader send HeartBeat to [%v]", rf.me, peer, peer)
		} else {
			Debug(dLog, "[%v]->[%v][AppendEntries1] Leader send %v entries log:{preLogTerm:[%v] preLogIndex:[%v]} nextIndex:[%v]", rf.me, peer, len(args.Entries), args.PreLogTerm, args.PrevLogIndex, next)
		}
		ok := rf.sendAppendEntries(peer, args, &reply)
		if ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.processAppendReplyL(peer, args, &reply)
		}
	}()
}

func (rf *Raft) processAppendReplyL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 {
		Debug(dHeat, "[%v]->[%v]HeartBeat Success!", rf.me, peer)
	} else {
		Debug(dLog, "[%v]<-[%v][AppendEntries3] reply is:{term:[%v] success:[%v] ConflictTerm:[%v] ConflictIndex:[%v]}", rf.me, peer, reply.Term, reply.Success, reply.ConflictTerm, reply.ConflictIndex)
	}
	if reply.Term > rf.currentTerm {
		rf.updateTermL(reply.Term)
	} else if rf.currentTerm == args.Term {
		rf.processAppendReplyTermL(peer, args, reply)
	}
}

func (rf *Raft) processAppendReplyTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newNextIndex > rf.nextindex[peer] {
			rf.nextindex[peer] = newNextIndex
		}
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
		}
		Debug(dLog, "[%v][AppendEntries3] Peer[%v] Success sync logs! nextIndex:[%v] matchIndex:[%v]", rf.me, peer, newNextIndex, newMatchIndex)
	} else {
		// 1. if term not exist but index exist
		//   (1) missing logs after index
		//   (2) need send snapshot after index
		if reply.ConflictTerm < 0 && reply.ConflictIndex > -1 {
			//need send snapshot
			rf.nextindex[peer] = reply.ConflictIndex
			Debug(dLog, "[%v][AppendEntries3] Peer[%v] missing logs after[%v]", rf.me, peer, reply.ConflictIndex)
		} else if reply.ConflictIndex > -1 && reply.ConflictTerm > -1 {
			// term exist and index exist
			Debug(dLog, "[%v][AppendEntries3] Peer[%v] logs already exist at ConflictIndex:[%v] ConflictTerm:[%v]", rf.me, peer, reply.ConflictIndex, reply.ConflictTerm)
			rf.processConflictTermL(peer, args, reply)
		}
	}
	rf.persist()
	rf.advanceCommitL()
}

func (rf *Raft) processConflictTermL(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Debug(dWarn, "[processConflictTerm] log needed to be handled [%v]", reply)
	//search the conflict term in entries
	index := rf.getLastIndexOfTerm(reply.ConflictTerm)
	if index > -1 {
		rf.nextindex[peer] = index
	} else {
		rf.nextindex[peer] = reply.ConflictIndex
	}
	rf.persist()
}

func (rf *Raft) advanceCommitL() {
	if rf.state != Leader {
		Debug(dError, "[Commit] Peer[%v] is not leader but it commit logs", rf.me)
	}
	start := rf.commitIndex + 1
	if start < rf.log.start() {
		start = rf.log.start()
	}
	for index := start; index <= rf.log.lastindex(); index++ {
		if rf.log.entry(index).Term != rf.currentTerm {
			continue
		}
		n := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me && rf.matchIndex[i] >= index {
				n += 1
			}
		}
		if n > len(rf.peers)/2 {
			Debug(dCommit, "[%v] Leader commit index before[%v]", rf.me, index)
			rf.commitIndex = index
		}
	}
	rf.signalApplierL()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	res := AppendEntriesReply{
		Term:          rf.currentTerm,
		Success:       true,
		ConflictIndex: -1,
		ConflictTerm:  -1,
	}
	//Reply false if args.term < CurrentTerm
	if args.Term < rf.currentTerm {
		res.Success = false
		res.Term = rf.currentTerm
		Debug(dLog, "[%d]->[%d][AppendEntries2] leader [%d] is out of date so failed", rf.me, args.LeaderId, args.LeaderId)
		*reply = res
		return
	}

	//check snapshot
	if rf.lastIncludeIndex > args.PrevLogIndex {
		res.Success = false
		res.ConflictIndex = rf.log.lastindex() + 1
		Debug(dSnap, "[%d] should send snapshot conflict:[%d]", rf.me, res.ConflictIndex)
		*reply = res
		return
	}

	//if new Leader set up become follower and reset timeout
	if args.Term > rf.currentTerm {
		Debug(dLog, "[%d][AppendEntries2] new leader has been elected at term [%d] So [%d] convert to follower", rf.me, args.Term, rf.me)
		rf.updateTermL(args.Term)
		rf.setElectionTime()
		res.Success = false
	}

	//for log
	if len(args.Entries) == 0 {
		Debug(dHeat, "[%d]<-[%d][HeartBeat] receive HeartBeat from leader ", rf.me, args.LeaderId)
	} else {
		Debug(dLog, "[%d]<-[%d][AppendEntries2] receive [%v] logs from leader begin with[%v]", rf.me, args.LeaderId, len(args.Entries), args.PrevLogIndex+1)
	}

	// handle append entries
	if len(args.Entries) > 0 {
		if args.PrevLogIndex == rf.log.lastindex() && args.PreLogTerm == rf.log.entry(rf.log.lastindex()).Term {
			l := rf.log.lastindex()
			rf.log.appendEntries(args.Entries)
			res.Success = true
			res.Term = rf.currentTerm
			Debug(dInfo, "[%d][AppendEntries2] Append [%v] new logs at[%v-%v]", rf.me, len(args.Entries), l+1, l+1+len(args.Entries))
		} else {
			isMatch := true
			nextIndex := args.PrevLogIndex + 1
			end := len(rf.log.log) - 1
			for i := 0; isMatch && i < len(args.Entries); i++ {
				if end < nextIndex+i {
					isMatch = false
				} else if rf.log.entry(nextIndex+i).Term != args.Entries[i].Term {
					isMatch = false
				}
			}
			if isMatch == false {
				rf.handleConflictLogL(args, &res)
			} else {
				Debug(dWarn, "[%v] Receive historic appendEntriesRPC so ignored", rf.me)
			}
		}
	} else {
		//heart Beat
		res.Success = true
		rf.setElectionTime()
	}
	//if leader commit > commitIndex set commitIndex = min(leaderIndex,last index of entries)
	if args.LeaderCommit > rf.commitIndex {
		if rf.log.lastindex() > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.log.lastindex()
		}
		Debug(dCommit, "[%v]Follower update commit Index to [%v]", rf.me, rf.commitIndex)
	}
	*reply = res
	rf.persist()
	if reply.Success {
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) handleConflictLogL(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//args.preLogIndex exist in peer log
	if rf.log.lastindex() >= args.PrevLogIndex {
		//index and term matches,but too many logs in entries so cut end and append
		if rf.log.entry(args.PrevLogIndex).Term == args.PreLogTerm {
			Debug(dWarn, "[%d][AppendEntries2] too many logs,cut end after [%v] and append [%v] new logs index0:[%v]", rf.me, args.PrevLogIndex+1, len(args.Entries), rf.log.index0)
			rf.log.cutend(args.PrevLogIndex + 1)
			rf.log.appendEntries(args.Entries)
		} else {
			//term conflict
			//find the first index on term of log [preLogIndex]
			preLogIndex := args.PrevLogIndex
			reply.ConflictIndex = rf.getFirstIndexOfTerm(preLogIndex)
			reply.ConflictTerm = rf.log.entry(preLogIndex).Term
			reply.Success = false
			Debug(dWarn, "[%d][AppendEntries2] Term Conflict term:[%v] index:[%v]", rf.me, reply.ConflictTerm, reply.ConflictIndex)
		}
	} else {
		//log at lastLogIndex did not exist
		reply.ConflictIndex = rf.log.lastindex()
		reply.Success = false
		Debug(dWarn, "[%d][AppendEntries2] missing log after index[%v]", rf.me, rf.log.lastindex())
	}
}

func (rf *Raft) getFirstIndexOfTerm(index int) int {
	if index == 0 {
		return 0
	}
	conflictIndex := index
	conflictTerm := rf.log.entry(index).Term
	for i := 0; i < len(rf.log.log) && conflictIndex > rf.log.index0; i++ {
		if rf.log.entry(conflictIndex).Term != conflictTerm {
			conflictIndex += 1
			break
		}
		conflictIndex -= 1
	}
	if rf.log.entry(conflictIndex).Term != conflictTerm {
		Debug(dError, "[%v]Term [%v] != [%v]", rf.me, conflictTerm, rf.log.entry(conflictIndex).Term)
	}
	return conflictIndex
}

func (rf *Raft) getLastIndexOfTerm(term int) int {
	res := -1
	if rf.log.entry(rf.log.lastindex()).Term < term {
		return res
	}
	for i := 0; i < len(rf.log.log); i++ {
		if rf.log.entry(rf.log.index0+i).Term == term {
			res = rf.log.index0 + i
		} else if rf.log.entry(rf.log.index0+i).Term > term {
			res = rf.log.index0 + i
			break
		}
	}
	return res
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
