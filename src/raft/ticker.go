package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) tick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		rf.setElectionTime()
		rf.sendAppendsL(true)
	}
	if time.Now().After(rf.electionTime) {
		rf.setElectionTime()
		rf.AttemptRequestVoteL()
	}
}

func (rf *Raft) setElectionTime() {
	t := time.Now()
	t = t.Add(time.Duration(350) * time.Millisecond)
	ms := rand.Int63() % 250
	t = t.Add(time.Duration(ms) * time.Millisecond)
	rf.electionTime = t
}

func (rf *Raft) ticker() {
	// Your code here (2A)
	// Check if a leader election should be started.
	for rf.killed() == false {
		rf.tick()
		ms := 100 + (rand.Int63() % 20)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
