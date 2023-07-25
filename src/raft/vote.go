package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) AttemptRequestVoteL() {
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.persist()
	Debug(dVote, "[%d] start election for term [%d]", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastindex(),
		LastLogTerm:  rf.log.entry(rf.log.lastindex()).Term,
	}
	votes := 1
	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {
			go rf.AttemptRequestVote(i, args, &votes)
		}
	}
}

func (rf *Raft) AttemptRequestVote(peer int, args *RequestVoteArgs, vote *int) {
	var reply RequestVoteReply
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	Debug(dVote, "[%v]->[%v][RequestVote1] send RequestVote at term[%v]", rf.me, peer, rf.currentTerm)
	rf.mu.Unlock()
	ok := rf.sendRequestVote(peer, args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//Debug(dVote, "[%v]<-[%v][RequestVote3] RequestVote reply receive at term[%v]", rf.me, peer, rf.CurrentTerm)
		if reply.Term > rf.currentTerm {
			rf.updateTermL(reply.Term)
		}
		if reply.VoteGranted {
			Debug(dVote, "[%v]<-[%v][RequestVote3] got votes at term[%d]", rf.me, peer, rf.currentTerm)
			*vote += 1
			if *vote > len(rf.peers)/2 {
				if rf.currentTerm == args.Term {
					Debug(dVote, "[%v]<-[%v][RequestVote3] got votes at term[%d]", rf.me, peer, rf.currentTerm)
					rf.toLeaderL()
					rf.sendAppendsL(true)
					return
				}
			}
		}
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Reply false if candidate's term is outdated
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Update current term and change state to follower
	Debug(dVote, "[%d]<-[%d][RequestVote2] ask for vote", rf.me, args.CandidateId)
	if args.Term > rf.currentTerm {
		Debug(dWarn, "[%d][RequestVote2] leader at term [%d] has already exist", rf.me, args.Term)
		rf.updateTermL(args.Term)
	}

	myIndex := rf.log.lastindex()
	myTerm := rf.log.entry(myIndex).Term
	uptodate := (args.LastLogTerm == myTerm && args.LastLogIndex >= myIndex) || args.LastLogTerm > myTerm
	if args.Term < rf.currentTerm {
		Debug(dWarn, "[%d][RequestVote2] Request term is out of date", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && uptodate {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.setElectionTime()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
