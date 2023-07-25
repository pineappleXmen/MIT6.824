package raft

import (
	"fmt"
	"strconv"
)

type Entry struct {
	Term    int
	Command interface{}
}

func (e Entry) String() string {
	return fmt.Sprintf("[%v:%v]", e.Term, e.Command)
}

func (l Log) String() string {
	reply := ""
	for i := 0; i < len(l.log); i++ {
		reply += "("
		reply += strconv.Itoa(i + l.index0)
		reply += ")"
		reply += l.log[i].String()
		reply += " "
	}
	return reply
}

type Log struct {
	log    []Entry
	index0 int
}

func (l *Log) append(e Entry) {
	l.log = append(l.log, e)
}

func (l *Log) appendEntries(e []Entry) {
	l.log = append(l.log, make([]Entry, len(e))...)
	copy(l.log[len(l.log)-len(e):], e)
}

// return the start index of Log
func (l *Log) start() int {
	return l.index0
}

// delete log entries after index(include index)
func (l *Log) cutend(index int) {
	l.log = l.log[0 : index-l.index0]
}

// delete log entries before index(exclude index)
func (l *Log) cutstart(index int) {
	l.index0 += index
	l.log = l.log[index:]
}

// return the entries after index(include index)
func (l *Log) slice(index int) []Entry {
	return l.log[index-l.index0:]
}

func (l *Log) lastindex() int {
	return l.index0 + len(l.log) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.log[index-l.index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}

func mkEmptyLog() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}
