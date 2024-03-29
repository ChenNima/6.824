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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"felix.chen/6.824/labrpc"
)

var heartBeatPeriod = 100
var heartBeatTimeout = 300
var heartBeatTimeoutRand = 150
var electionBeatTimeout = 800
var electionBeatTimeoutRand = 150

// import "bytes"
// import  "felix.chen/6.824/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftLog struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	// 0-follower 1-candidate 2-leader
	state         int
	lastHeartBeat time.Time
	votedFor      map[int]int

	logs []RaftLog

	applyCh        chan ApplyMsg
	committedIndex map[int]int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == 2
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

type AppendEntriesArgs struct {
	Term             int
	Logs             []RaftLog
	PreviousLogIndex int
	PreviousLogTerm  int
	Command          interface{}
	Index            int
	CommittedIndex   map[int]int
}

type AppendEntriesReply struct {
	Ok       bool
	AxTerm   int
	AxIndex  int
	AxLength int
}

type CommitEntriesReply struct {
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("[%d] received AppendEntries, term %d\n", rf.me, args.Term)
	if rf.currentTerm > args.Term {
		// ignore terms less then self
		fmt.Printf("[%d] ignore, has higher term %d\n", rf.me, rf.currentTerm)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// receives Append, change to follower
	rf.state = 0
	rf.currentTerm = args.Term
	currentTime := time.Now()
	if rf.lastHeartBeat.Before(currentTime) {
		rf.lastHeartBeat = currentTime
	}

	if args.Logs != nil {
		if len(rf.logs)-1 < args.PreviousLogIndex {
			fmt.Printf("[%d] reject append, no previous log at index %d\n", rf.me, args.PreviousLogIndex)
			reply.AxTerm = -1
			reply.AxIndex = -1
			reply.AxLength = len(rf.logs)
			reply.Ok = false
			fmt.Printf("[%d] %v\n", rf.me, reply)
			return
		}
		if args.PreviousLogIndex > -1 {
			lastLog := rf.logs[args.PreviousLogIndex]
			if lastLog.Term != args.PreviousLogTerm {
				reply.AxTerm = lastLog.Term
				for i, log := range rf.logs {
					if log.Term != lastLog.Term {
						continue
					} else {
						reply.AxIndex = i
						break
					}
				}
				reply.AxLength = len(rf.logs)
				fmt.Printf("[%d] reject append, my previous term %d, expected %d\n", rf.me, lastLog.Term, args.PreviousLogTerm)
				fmt.Printf("[%d] %v\n", rf.me, reply)
				reply.Ok = false
				return
			}
		}
		fmt.Printf("[%d] log index %d applied\n", rf.me, args.Index)
		reply.Ok = true
		rf.logs = append(rf.logs[:args.PreviousLogIndex+1], args.Logs...)
	}

	for i := 0; i < len(rf.logs); i++ {
		if args.CommittedIndex[i] != rf.logs[i].Term {
			break
		}
		if rf.committedIndex[i] != 0 {
			continue
		}
		fmt.Printf("[%d] committing for index %d, pre: %d\n", rf.me, i, args.PreviousLogIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.logs[i].Command,
		}
		rf.committedIndex[i] = rf.logs[i].Term
	}
}

func (rf *Raft) sendAppendEntriesToAll(lastIndex int, isHeartBeat bool, command interface{}, index int) {
	var appendSuccess uint64
	cond := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i != rf.me {
			go func(peer int, currentTerm int) {
				ok := false
				for !ok && lastIndex > -1 && rf.state == 2 {
					lastTerm := 0
					if lastIndex >= 0 {
						lastTerm = rf.logs[lastIndex].Term
					}
					logs := []RaftLog{}
					if isHeartBeat {
						logs = nil
					} else {
						for i := lastIndex + 1; i < len(rf.logs); i++ {
							logs = rf.logs[lastIndex+1:]
						}
					}
					if !isHeartBeat {
						fmt.Printf("[%d] sending log to %d length %v\n", rf.me, peer, len(logs))
					}
					arg := AppendEntriesArgs{
						Term:             currentTerm,
						Logs:             logs,
						PreviousLogIndex: lastIndex,
						PreviousLogTerm:  lastTerm,
						Index:            index,
						CommittedIndex:   rf.committedIndex,
					}
					reply := AppendEntriesReply{}
					// command := "AppendEntries"
					// if isHeartBeat {
					// 	command = "HeartBeat"
					// }
					// fmt.Printf("[%d] sending %s to %d\n", rf.me, command, peer)
					rpcSuccess := rf.peers[peer].Call("Raft.AppendEntries", &arg, &reply)
					if !rpcSuccess {
						continue
					}
					if isHeartBeat {
						ok = true
					} else {
						ok = reply.Ok
						if !ok {
							if reply.AxLength == 0 {
								fmt.Printf("[%d] detect higher term, stop append\n", rf.me)
								rf.state = 0
								break
							}
							fmt.Printf("[%d] false reply from %d: %v\n", rf.me, peer, reply)
							if reply.AxTerm == -1 {
								lastIndex = reply.AxLength - 1
							} else {
								hasTerm := false
								start := reply.AxLength - 1
								if len(rf.logs) < reply.AxLength {
									start = len(rf.logs) - 1
								}
								for i := start - 1; i >= 0; i-- {
									if rf.logs[i].Term == reply.AxTerm {
										lastIndex = i
										break
									}
									if rf.logs[i].Term < reply.AxTerm {
										break
									}
								}
								if !hasTerm {
									lastIndex = reply.AxIndex - 1
								}
							}
						}
					}
				}
				rf.mu.Lock()
				if ok {
					atomic.AddUint64(&appendSuccess, 1)
				}
				cond.Broadcast()
				rf.mu.Unlock()
			}(i, rf.currentTerm)
		}
	}
	if !isHeartBeat {
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			for int(appendSuccess) < (len(rf.peers)-1)/2 {
				cond.Wait()
			}
			if int(appendSuccess) >= (len(rf.peers)-1)/2 {
				fmt.Printf("[%d] start log committing, index %d, command %v\n", rf.me, index, command)
				rf.committedIndex[index] = rf.logs[index].Term
				for i := 0; i < len(rf.logs); i++ {
					if rf.committedIndex[i] == 0 {
						break
					}
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						CommandIndex: i,
						Command:      rf.logs[i].Command,
					}
				}
				// rf.applyCh <- ApplyMsg{
				// 	CommandValid: true,
				// 	CommandIndex: index,
				// 	Command:      command,
				// }
				// var wg sync.WaitGroup
				// for i := range rf.peers {
				// 	if i != rf.me {
				// 		wg.Add(1)
				// 		go func(peer int) {
				// 			defer wg.Done()
				// 			arg := CommitEntriesArgs{
				// 				Index: index,
				// 			}
				// 			reply := CommitEntriesReply{}
				// 			rf.sendCommitEntries(peer, &arg, &reply)
				// 		}(i)
				// 	}
				// }
				// wg.Wait()
				fmt.Printf("[%d] log committed, index %d, command %v\n", rf.me, index, command)
			}
		}()
	}
}

func (rf *Raft) startElection() {
	if rf.state == 2 {
		return
	}
	rf.mu.Lock()
	// make self to candidate
	rf.state = 1
	rf.currentTerm++
	rf.votedFor[rf.currentTerm] = rf.me
	fmt.Printf("[%d] startElection term %d \n", rf.me, rf.currentTerm)

	voteChan := make(chan bool, 10)
	defer close(voteChan)
	voted := 0
	votedForMe := 0
	timeout := false
	electionFinished := false
	electionTerm := rf.currentTerm
	for i := range rf.peers {
		if i != rf.me {
			go func(peer int, electionTerm int) {
				arg := RequestVoteArgs{
					Term:        electionTerm,
					Id:          rf.me,
					LogLength:   len(rf.logs),
					LastLogTerm: rf.logs[len(rf.logs)-1].Term,
				}
				reply := RequestVoteReply{
					Ok: false,
				}
				success := rf.sendRequestVote(peer, &arg, &reply)
				if !electionFinished {
					voteChan <- success && reply.Ok
				}
			}(i, electionTerm)
		}
	}
	rf.mu.Unlock()
	n := rand.Intn(electionBeatTimeoutRand)
	electionTimeout := time.Duration(electionBeatTimeout+n) * time.Millisecond
	for (voted != len(rf.peers)-1) &&
		(votedForMe < (len(rf.peers)-1)/2) &&
		!timeout {
		select {
		case res := <-voteChan:
			voted++
			if res {
				votedForMe++
			}
		case <-time.After(electionTimeout):
			timeout = true
		}
	}
	electionFinished = true
	fmt.Printf("[%d] electionFinished, votedForMe: %d voted: %d timeout: %v\n", rf.me, votedForMe, voted, timeout)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (votedForMe >= (len(rf.peers)-1)/2) && rf.state == 1 && rf.currentTerm == electionTerm {
		rf.state = 2
		go rf.startLeader()
	} else if rf.state == 1 {
		rf.state = 0
	}
}

func (rf *Raft) startLeader() {
	fmt.Printf("[%d] startLeader, term %d\n", rf.me, rf.currentTerm)
	for rf.state == 2 {
		rf.mu.Lock()
		if rf.state == 2 {
			rf.sendAppendEntriesToAll(0, true, nil, 0)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartBeatPeriod) * time.Millisecond)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	Id          int
	LastLogTerm int
	LogLength   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Ok bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("[%d] received RequestVote from %d, term %d\n", rf.me, args.Id, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	voted, pres := rf.votedFor[args.Term]
	if pres && voted != args.Id {
		reply.Ok = false
		return
	}
	// Your code here (2A, 2B).
	if rf.currentTerm > args.Term {
		reply.Ok = false
		return
	}
	rf.currentTerm = args.Term
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		reply.Ok = false
		return
	}
	if (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term) && args.LogLength < len(rf.logs) {
		reply.Ok = false
		return
	}
	reply.Ok = true
	rf.votedFor[args.Term] = args.Id
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
	index := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.GetState()
	if !isLeader {
		return index, term, isLeader
	}
	index = len(rf.logs)
	rf.logs = append(rf.logs, RaftLog{
		Term:    term,
		Command: command,
	})
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		_, isLeader := rf.GetState()
		if isLeader {
			rf.sendAppendEntriesToAll(index-1, false, command, index)
		}
	}()
	// Your code here (2B).

	return index, term, isLeader
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = make(map[int]int)
	rf.committedIndex = map[int]int{
		0: -1,
	}

	rf.currentTerm = 0
	rf.state = 0
	rf.logs = []RaftLog{
		{
			Command: nil,
			Term:    -1,
		},
	}
	rf.applyCh = applyCh

	go func() {
		for {
			rand.Seed(time.Now().UnixNano())
			n := rand.Intn(heartBeatTimeoutRand)
			heartBeatTimeoutDuration := time.Duration(n+heartBeatTimeout) * time.Millisecond
			time.Sleep(heartBeatTimeoutDuration)
			if rf.state == 0 && rf.lastHeartBeat.Add(heartBeatTimeoutDuration).Before(time.Now()) {
				rf.startElection()
			}
		}
	}()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
