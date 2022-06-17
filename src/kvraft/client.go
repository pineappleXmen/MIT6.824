package kvraft

import (
	"6.824/labrpc"
	"log"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	CurLeader int
	SeqID     int
	ClientID  int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientID = nrand()
	ck.CurLeader = mathrand.Intn(len(ck.servers))
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("[%d][GET][Client] is calling GET %s", ck.ClientID, key)
	ck.SeqID++
	args := GetArgs{
		Key:      key,
		ClientID: ck.ClientID,
		SeqID:    ck.SeqID,
	}
	curLeaderID := ck.CurLeader
	for {
		reply := GetReply{}
		ok := ck.servers[curLeaderID].Call("KVServer.Get", &args, &reply)
		if ok {
			log.Printf("[%d][GET][Client] Success msg from Server %d", ck.ClientID, ck.CurLeader)
			if reply.Err == OK {
				ck.CurLeader = curLeaderID
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.CurLeader = curLeaderID
				return ""
			} else if reply.Err == ErrWrongLeader {
				curLeaderID = (curLeaderID + 1) % len(ck.servers)
				continue
			}
		} else {
			log.Printf("[%d][GET][Client] Failed to connect %d", ck.ClientID, ck.CurLeader)
		}
		curLeaderID = (curLeaderID + 1) % len(ck.servers)
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("[%d][PutAppend][Client] is calling put %s %v", ck.ClientID, key, value)
	ck.SeqID++
	curLeaderID := ck.CurLeader
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.ClientID,
		SeqID:    ck.SeqID,
	}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[curLeaderID].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.CurLeader = curLeaderID
				break
			}
			if reply.Err == ErrWrongLeader {
				curLeaderID = (curLeaderID + 1) % len(ck.servers)
				continue
			}
		} else {
			log.Printf("[%d][PutAppend] Failed", curLeaderID)
		}
		curLeaderID = (curLeaderID + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")

}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
