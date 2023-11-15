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
	//	"bytes"

	"fmt"
	"math/rand"
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

const (
	Leader int = iota
	Candidate
	Follower
)

type LogEntry struct {
	Index   int
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logs []*LogEntry
	// termStartIndex map[int]int
	currentTerm int
	votedFor    int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	state      int
	done       chan bool
	notified   bool

	start sync.Mutex

	preCommand any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Index   int
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// reply.Index = -1
	if args.Term < rf.currentTerm || rf.killed() || args.LeaderCommit < rf.commitIndex {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term

		// //fmt.Printf("%d receive entries\n", rf.me)
	}
	if rf.me == args.LeaderId {
		rf.logs = append(rf.logs, args.Entries...)
		// rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		// rf.lastApplied = rf.commitIndex
		reply.Success = true
		// reply.Index = rf.commitIndex
		return
	}
	rf.state = Follower
	lastLogIndex := len(rf.logs) - 1
	if args.PrevLogIndex > rf.logs[lastLogIndex].Index || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// fmt.Printf("preindex:%d,%d,%d,%d,%d\n", args.PrevLogIndex, rf.me, rf.logs[lastLogIndex].Index, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Term = args.Term
		// reply.Success = true
		return
	}

	reply.Success = true
	go func() {
		if !rf.notified {
			rf.done <- true
			rf.notified = true
		}
	}()
	// fmt.Printf("%d %d %d %d\n", rf.me, rf.commitIndex, args.LeaderId, args.LeaderCommit)

	index := args.PrevLogIndex
	for i, entry := range args.Entries {
		index++
		// fmt.Println(GetGid(), ":", rf.me, "index", index, "len(rf.logs)", len(rf.logs))
		if index < len(rf.logs) {
			if rf.logs[index].Term == entry.Term {

				// fmt.Printf("%d skip %d\n", rf.me, index)
				continue
			}
			// fmt.Printf("%d long %d\n", rf.me, len(rf.logs))
			rf.logs = rf.logs[:index]
		}

		// if args.Entries != nil {
		rf.logs = append(rf.logs, args.Entries[i:]...)
		// }
		// fmt.Printf("%d append %d from %d\n", rf.me, len(args.Entries[i:]), args.LeaderId)
		break
	}
	if args.LeaderCommit > rf.commitIndex && len(rf.logs)-1 >= args.LeaderCommit {
		// fmt.Printf("%d apply %v\n", rf.me, rf.logs[args.LeaderCommit].Command)
		for i := rf.commitIndex + 1; i <= args.LeaderCommit; i++ {

			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command:      rf.logs[i].Command,
			}
			switch cmd := rf.logs[i].Command.(type) {
			case string:
				fmt.Printf("%d apply stringlength %d\n", rf.me, len(cmd))
			case int:
				fmt.Printf("%d:%d apply %d\n", GetGid(), rf.me, cmd)
			}
			// fmt.Printf("%d apply %v\n", rf.me, rf.logs[args.LeaderCommit].Command)
			rf.applyCh <- applyMsg
			rf.lastApplied++

		}
		rf.commitIndex = args.LeaderCommit
	}
	// if rf.commitIndex < args.LeaderCommit {
	// 	lastLogIndex = rf.logs[len(rf.logs)-1].Index
	// 	if args.LeaderCommit > lastLogIndex {
	// 		rf.commitIndex = lastLogIndex
	// 	} else {
	// 		rf.commitIndex = args.LeaderCommit
	// 	}
	// }
	go func() {
		if !rf.notified {
			rf.done <- true
			rf.notified = true
		}
	}()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	resultChan := make(chan bool)

	go func() {
		// 在新的 goroutine 中执行 RPC 调用
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		resultChan <- ok
	}()
	select {
	case <-time.After(time.Duration(50) * time.Millisecond):
		ok := false // 超时时返回nil
		return ok
	case ok := <-resultChan:
		return ok
	}
}

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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || rf.killed() || args.LastLogIndex < rf.commitIndex {
		return
	}

	// if args.Term == rf.currentTerm && args.LastLogIndex < rf.commitIndex {
	// 	return
	// }
	if args.CandidateId == rf.me {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		//fmt.Printf("%d votefor %d, it`s term is %d\n", rf.me, args.CandidateId, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.state = Follower
		go func() {

			if !rf.notified {
				rf.done <- true
				rf.notified = true
			}

		}()
		// ms := 100 + (rand.Int63() % 50)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

		//fmt.Printf("%d votefor %d, it`s term is %d\n", rf.me, args.CandidateId, args.Term)
	}
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
	//fmt.Printf("%d 发投票请求给 %d\n", rf.me, server)
	resultChan := make(chan bool)

	go func() {
		// 在新的 goroutine 中执行 RPC 调用
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		resultChan <- ok
	}()
	select {
	case <-time.After(time.Duration(10) * time.Millisecond):
		ok := false // 超时时返回nil
		return ok
	case ok := <-resultChan:
		return ok
	}
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
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	index := -1
	term := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).
	if isLeader {
		rf.start.Lock()
		defer rf.start.Unlock()
		// 处理命令
		// fmt.Printf("%d 处理命令 %v\n", rf.me, command)
		log := &LogEntry{
			Index:   rf.commitIndex + 1,
			Term:    term,
			Command: command,
		}

		if rf.preCommand == command {
			log = &LogEntry{
				Index:   rf.commitIndex,
				Term:    term,
				Command: command,
			}
		}

		// reply := &AppendEntriesReply{}
		// rf.AppendEntries(&AppendEntriesArgs{
		// 	Term:         term,
		// 	LeaderId:     rf.me,
		// 	PrevLogIndex: rf.commitIndex,
		// 	PrevLogTerm:  rf.logs[rf.commitIndex].Term,
		// 	Entries:      []*LogEntry{log},
		// 	LeaderCommit: rf.commitIndex}, reply)
		rf.logs = append(rf.logs[:log.Index], log)
		// rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		// rf.lastApplied = rf.commitIndex
		// reply.Success = true
		rf.preCommand = command
		term, isLeader = rf.GetState()
		if ok := rf.broadcastEntries(log); !ok {
			// switch command := command.(type) {
			// case string:
			// 	fmt.Printf("%d return stringlength %d\n", rf.me, len(command))
			// case int:
			// 	fmt.Printf("%d return %d\n", rf.me, command)
			// }
			return rf.commitIndex + 1, term, isLeader
		}
		// switch command := command.(type) {
		// case string:
		// 	fmt.Printf("%d return stringlength %d\n", rf.me, len(command))
		// case int:
		// 	fmt.Printf("%d return %d\n", rf.me, command)
		// }
		// 返回结果

		return rf.commitIndex, term, isLeader
	}

	return index, term, isLeader
}

func (rf *Raft) broadcastEntries(log *LogEntry) bool {

	SuccessNum := 1

	for i := 0; i < len(rf.peers); i++ {
		if rf.state != Leader {
			break
		}
		if rf.me == i {
			continue
		}
		Reply := &AppendEntriesReply{}
		rf.nextIndex[i] = len(rf.logs) - 1
		fmt.Println(rf.me, "send", i, "PrevLogIndex", log.Index-1, "cmd", log.Command)
		ok := rf.sendAppendEntries(i, &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: log.Index - 1,
			PrevLogTerm:  rf.logs[log.Index-1].Term,
			Entries:      []*LogEntry{log},
			LeaderCommit: rf.commitIndex}, Reply)
		if Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = Reply.Term
			break
		}
		fmt.Printf("%d:%d to %d %v %d\n", GetGid(), rf.me, i, Reply.Success, rf.logs[rf.nextIndex[i]-1].Index)
		Reply = rf.retryAppendEntries(i, Reply)

		if Reply.Success && ok {
			SuccessNum++
		}
		if SuccessNum > len(rf.peers)/2 {

			rf.commitIndex = len(rf.logs) - 1
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {

				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.commitIndex,
					Command:      rf.logs[i].Command,
				}
				// fmt.Printf("%d apply %d\n", rf.me, len(cmd.(string)))
				switch cmd := rf.logs[i].Command.(type) {
				case string:
					fmt.Printf("%d apply stringlength %d\n", rf.me, len(cmd))
				case int:
					fmt.Printf("%d apply %d\n", rf.me, cmd)
				}

				rf.applyCh <- applyMsg
				rf.lastApplied++
				fmt.Printf("%d commitIndex= %d\n", rf.me, rf.commitIndex)
			}
			return true
		}

	}
	return false

}

func (rf *Raft) retryAppendEntries(i int, Reply *AppendEntriesReply) *AppendEntriesReply {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// fmt.Printf("%d send to %d\n", rf.me, i)
	if !Reply.Success && Reply.Term == rf.currentTerm {
		term := rf.currentTerm
		Reply = &AppendEntriesReply{}
		for !Reply.Success {
			//回退一个term的Entries,直到
			for rf.logs[rf.nextIndex[i]-1].Term == term && rf.nextIndex[i]-1 != 0 && rf.nextIndex[i] > rf.matchIndex[i]+1 {
				rf.nextIndex[i]--
			}
			// fmt.Printf("term %d\n", term)
			Reply2 := &AppendEntriesReply{}
			rf.sendAppendEntries(i, &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.logs[rf.nextIndex[i]-1].Index,
				PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
				Entries:      nil,
				LeaderCommit: rf.commitIndex}, Reply2)
			term = rf.logs[rf.nextIndex[i]-1].Term
			Reply = Reply2
		}

		Reply = &AppendEntriesReply{}
		rf.sendAppendEntries(i, &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.logs[rf.nextIndex[i]-1].Index,
			PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
			Entries:      rf.logs[rf.nextIndex[i] : rf.commitIndex+1],
			LeaderCommit: rf.commitIndex}, Reply)
		fmt.Printf("%d:%d send %d logs to %d\n", GetGid(), rf.me, len(rf.logs[rf.nextIndex[i]:]), i)
	}
	if Reply.Success {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = rf.nextIndex[i] - 1
	}

	return Reply
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

func (rf *Raft) ticker() {

	for !rf.killed() {
		rf.notified = true
		if rf.state == Candidate {

			rf.elect()

		}
		// Your code here (2A)
		// Check if a leader election should be started.

		if rf.state == Leader {
			for rf.state == Leader {
				unReplyNum := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.state != Leader {
						break
					}
					if rf.me == i {
						continue
					}
					Reply := &AppendEntriesReply{}
					// fmt.Println(rf.me, rf.logs[len(rf.logs)-1].Index)
					ok := rf.sendAppendEntries(i, &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.logs[len(rf.logs)-1].Index,
						PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex}, Reply)
					if Reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = Reply.Term
						break
					}
					// fmt.Printf("ticker %d to %d %v\n", rf.me, i, Reply.Success)
					Reply = rf.retryAppendEntries(i, Reply)

					// println("Reply.Term", Reply.Term, Reply.Success, rf.me, i)
					if !Reply.Success || !ok {
						unReplyNum++
					}
					if unReplyNum > len(rf.peers)/2 {
						rf.state = Follower
						//fmt.Printf("%d become follower\n", rf.me)
						// rf.currentTerm = Reply.Term
						break
					}
					// 获取结束时间
				}

				ms := 100
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		if rf.state == Follower {
			rf.state = Candidate
		}
		rf.notified = false
		ms := 100 + (rand.Int63() % 350)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case <-rf.done:
			if rf.notified {
				ms := 100 + (rand.Int63() % 350)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				// //fmt.Printf("%d reset ticker\n", rf.me)
				rf.notified = false

			}

		default:
			continue
		}
	}
}

func (rf *Raft) elect() {
	voteforme := 0
	rf.currentTerm++
	Reply := &RequestVoteReply{}

	// startTime := time.Now()
	rf.sendRequestVote(rf.me, &RequestVoteArgs{rf.currentTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term}, Reply)
	// endTime := time.Now()
	// elapsedTime := endTime.Sub(startTime)
	//fmt.Printf("%d<-%d代码运行时间: %s %v\n", rf.me, rf.me, elapsedTime, Reply.VoteGranted)
	if Reply.VoteGranted {
		voteforme++
	}

	for i := 0; i < len(rf.peers); i++ {
		Reply = &RequestVoteReply{}
		if rf.me == i {
			continue
		}
		// startTime := time.Now()
		ok := rf.sendRequestVote(i, &RequestVoteArgs{rf.currentTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term}, Reply)
		// endTime := time.Now()
		// elapsedTime := endTime.Sub(startTime)
		//fmt.Printf("%d<-%d代码运行时间: %s %v\n", rf.me, i, elapsedTime, Reply.VoteGranted)
		if Reply.VoteGranted && ok {
			voteforme++
		}
		if rf.state == Follower {
			return
		}
		if Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = Reply.Term
			return
		}
		if voteforme > len(rf.peers)/2 {
			rf.state = Leader
			fmt.Printf("%d become leader, term is %d\n", rf.me, rf.currentTerm)
			// rf.broadcastHeartbeat()
			// rf.electionTimer.Stop()
			// rf.broadcastHeartbeat()
			return
		}

	}
	rf.state = Follower
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same ordser. persister is a place for this server to
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
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.matchIndex = make([]int, len(peers))
	rf.votedFor = -2
	rf.dead = 0
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.lastApplied = 0
	rf.state = Follower

	log := LogEntry{Term: 0, Command: nil, Index: 0}
	rf.logs =
		make([]*LogEntry, 0)
	rf.logs = append(rf.logs, &log)
	rf.done =
		make(chan bool)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
