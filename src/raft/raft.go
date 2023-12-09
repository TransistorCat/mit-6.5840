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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	cond       *sync.Cond
	// nilNum     int
	timer <-chan time.Time

	start sync.Mutex
	wait  sync.Mutex

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm int
		votedFor    int
		logs        []*LogEntry
		// commitIndex int
		// lastApplied int
	)

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		// d.Decode(&commitIndex) != nil ||
		// d.Decode(&lastApplied) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		// rf.commitIndex = commitIndex
		// rf.lastApplied = lastApplied
	}
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
	if args.Term < rf.currentTerm || rf.killed() {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.persist()

		// //DPrintf("%d receive entries\n", rf.me)
	}
	if rf.me != args.LeaderId {
		rf.state = Follower
	}
	rf.persist()
	lastLogIndex := len(rf.logs) - 1
	if args.PrevLogIndex > rf.logs[lastLogIndex].Index || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// DPrintf("preindex:%d,%d,%d,%d,%d\n", args.PrevLogIndex, rf.me, rf.logs[lastLogIndex].Index, rf.logs[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.Term = args.Term
		// reply.Success = true
		return
	}

	if args.Entries == nil {
		reply.Success = true
	}
	rf.setTimer()
	// DPrintf("%d %d %d %d\n", rf.me, rf.commitIndex, args.LeaderId, args.LeaderCommit)

	index := args.PrevLogIndex
	for i, entry := range args.Entries {
		index++

		// fmt.Println(GetGid(), ":", rf.me, "index", index, "len(rf.logs)", len(rf.logs))
		if index < len(rf.logs) {
			if rf.logs[index].Term == entry.Term {

				// DPrintf("%d skip %d\n", rf.me, index)
				continue
			}
			// DPrintf("%d long %d\n", rf.me, len(rf.logs))
			rf.logs = rf.logs[:index]
			rf.persist()
		}

		// if args.Entries != nil {
		rf.logs = append(rf.logs, args.Entries[i:]...)
		reply.Success = true
		DPrintf(dLog, "S%d <- S%d I:[%d:%d]", rf.me, args.LeaderId, args.Entries[i].Index,
			args.Entries[i].Index+len(args.Entries)-1)
		rf.persist()
		// }
		rf.commitIndex = args.LeaderCommit

		break
	}
	DPrintf(dClient, "S%d(LA:%d,LEN:%d,T:%d) S%d(CI:%d,T:%d) ", rf.me, rf.lastApplied, len(rf.logs)-1, rf.logs[len(rf.logs)-1].Term, args.LeaderId, args.LeaderCommit, args.Term)
	if args.LeaderCommit > rf.lastApplied &&
		len(rf.logs)-1 == args.LeaderCommit &&
		// rf.logs[len(rf.logs)-1].Term == args.Term &&
		args.PrevLogIndex == rf.logs[len(rf.logs)-1].Index {
		DPrintf(dClient, "S%d(LA:%d,LEN:%d) S%d(CI:%d) into if", rf.me, rf.lastApplied, len(rf.logs)-1, args.LeaderId, args.LeaderCommit)
		for i := rf.lastApplied + 1; i <= args.LeaderCommit; i++ {
			// if rf.logs[i-1].Command == rf.logs[i].Command {
			//
			// 	continue
			// }
			DPrintf(dClient, "S%d(LA:%d,LEN:%d) S%d(CI:%d) into for", rf.me, rf.lastApplied, len(rf.logs)-1, args.LeaderId, args.LeaderCommit)
			// if rf.logs[i].Command == nil {
			// 	rf.lastApplied++
			// 	rf.nilNum++
			// 	continue
			// }
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.logs[i].Index,
				Command:      rf.logs[i].Command,
			}
			switch cmd := rf.logs[i].Command.(type) {
			case string:
				DPrintf(dLog, "S%d apply stringlength %d\n", rf.me, len(cmd))
			case int:
				DPrintf(dLog, "S%d apply %d,I:%d", rf.me, cmd, applyMsg.CommandIndex)
			}
			// DPrintf("%d apply %v\n", rf.me, rf.logs[args.LeaderCommit].Command)
			rf.applyCh <- applyMsg
			rf.lastApplied = rf.logs[i].Index
			rf.persist()

		}

	}
	// if rf.commitIndex < args.LeaderCommit {
	// 	lastLogIndex = rf.logs[len(rf.logs)-1].Index
	// 	if args.LeaderCommit > lastLogIndex {
	// 		rf.commitIndex = lastLogIndex
	// 	} else {
	// 		rf.commitIndex = args.LeaderCommit
	// 	}
	// }
	rf.setTimer()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	resultChan := make(chan bool, 1)

	go func() {
		// 在新的 goroutine 中执行 RPC 调用
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		resultChan <- ok
		close(resultChan)
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
	CommitIndex  int

	IsPre bool
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
	rf.persist()

	if args.Term < rf.currentTerm || rf.killed() ||

		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		args.CommitIndex < rf.commitIndex {
		return
	}
	// if args.IsPre {
	// 	if args.Term > rf.currentTerm || ((args.LastLogIndex > rf.logs[len(rf.logs)-1].Index) &&
	// 		(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term)) {
	// 		reply.VoteGranted = true
	// 		rf.setTimer()
	// 		DPrintf(dPVote, "S%d grant S%d", rf.me, args.CandidateId)
	// 	}

	// 	return
	// }
	// if args.Term == rf.currentTerm && asrgs.LastLogIndex < rf.commitIndex {
	// 	return
	// }

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term ||
		((args.LastLogIndex < rf.logs[len(rf.logs)-1].Index) &&
			(args.LastLogTerm == rf.logs[len(rf.logs)-1].Term)) {
		reply.Term = rf.currentTerm
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	DPrintf(dVote, "S%d grant S%d LT:%d LI:%d", rf.me, args.CandidateId, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index)

	rf.persist()
	rf.setTimer()

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
	resultChan := make(chan bool, 1)
	go func() {
		// 在新的 goroutine 中执行 RPC 调用
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		resultChan <- ok
		close(resultChan)
	}()
	select {
	case <-time.After(time.Duration(50) * time.Millisecond):
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
	// index := -1
	term := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).
	// if rf.preCommand == command {
	// 	rf.commitIndex++
	// 	return rf.commitIndex, term, isLeader
	// }
	if isLeader {

		rf.start.Lock()
		defer rf.start.Unlock()

		// 处理命令
		term, _ = rf.GetState()
		log := &LogEntry{
			Index:   rf.logs[len(rf.logs)-1].Index + 1,
			Term:    term,
			Command: command,
		}
		DPrintf(dLeader, "S%d start %v I:%d", rf.me, command, log.Index)
		Index := log.Index
		// 	DPrintf(dLeader, "S%d %v repeated %v I:%d", rf.me, command, rf.preCommand, log.Index)
		if rf.logs[Index-1].Command == command {
			DPrintf(dLeader, "S%d return index:%d cmd:%v\n", rf.me, Index-1, log.Command)
			return Index - 1, term, isLeader
		}
		// 	rf.logs = rf.logs[:log.Index]
		rf.logs = append(rf.logs, log)

		// reply := &AppendEntriesReply{}
		// rf.AppendEntries(&AppendEntriesArgs{
		// 	Term:         term,
		// 	LeaderId:     rf.me,
		// 	PrevLogIndex: rf.commitIndex,
		// 	PrevLogTerm:  rf.logs[rf.commitIndex].Term,
		// 	Entries:      []*LogEntry{log},
		// 	LeaderCommit: rf.commitIndex}, reply)

		rf.persist()
		// rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		// rf.lastApplied = rf.commitIndex
		// reply.Success = true

		// if ok := rf.broadcastEntries(log); !ok {
		// rf.preCommand = nil
		// rf.broadcastEntries(log)
		// switch command := command.(type) {
		// case string:
		// 	DPrintf("%d return stringlength %d\n", rf.me, len(command))
		// case int:
		// 	DPrintf("%d return %d\n", rf.me, command)
		// }
		// rf.logs = rf.logs[:log.Index]
		// rf.persist()
		// 	return log.Index, term, isLeader
		// }
		// switch command := command.(type) {
		// case string:
		// 	DPrintf("%d return stringlength %d\n", rf.me, len(command))
		// case int:
		// 	DPrintf("%d return %d\n", rf.me, command)
		// }
		// 返回结果
		DPrintf(dLeader, "S%d return index:%d cmd:%v\n", rf.me, Index, log.Command)
		return Index, term, isLeader
	}

	return rf.logs[len(rf.logs)-1].Index, term, isLeader
}

func (rf *Raft) broadcastEntries(log *LogEntry) bool {

	SuccessNum := 1
	// startTime := time.Now()
	for i := 0; i < len(rf.peers); i++ {

		if rf.state != Leader {
			DPrintf(dInfo, "%d isn`t leader\n", rf.me)
			break
		}
		if rf.me == i {
			continue
		}
		Reply := &AppendEntriesReply{}
		rf.nextIndex[i] = len(rf.logs) - 1
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: log.Index - 1,
			PrevLogTerm:  rf.logs[log.Index-1].Term,
			Entries:      []*LogEntry{log},
			LeaderCommit: rf.commitIndex}
		if log.Command == nil {
			args.Entries = nil
		}
		// DPrintf(,"%d:%d send append entries %v to %d\n", GetGid(), rf.me, log.Command, i)
		DPrintf(dLog, "S%d -> S%d\n", rf.me, i)
		ok := rf.sendAppendEntries(i, args, Reply)
		if Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = Reply.Term
			DPrintf(dPersist, "S%d presist T:%d,VF:%d\n", rf.me, rf.currentTerm, rf.votedFor)
			rf.persist()
			break
		}

		Reply = rf.retryAppendEntries(i, Reply)
		// DPrintf("111\n")
		if Reply.Success && ok {
			SuccessNum++
			DPrintf(dLog, "S%d <- S%d %v I:%d", i, rf.me, log.Command, log.Index)
		}

		if SuccessNum > len(rf.peers)/2 {

			rf.commitIndex = len(rf.logs) - 1
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if rf.logs[i-1].Command == rf.logs[i].Command {
					continue
				}
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.logs[i].Index,
					Command:      rf.logs[i].Command,
				}
				// DPrintf("%d apply %d\n", rf.me, len(cmd.(string)))
				if rf.state != Leader {
					DPrintf(dInfo, "S%d isn`t leader", rf.me)
					return false
				}
				switch cmd := rf.logs[i].Command.(type) {
				case string:
					DPrintf(dLog, "S%d apply stringlength %d", rf.me, len(cmd))
				case int:
					DPrintf(dLog, "S%d apply %d", rf.me, cmd)
				}

				rf.applyCh <- applyMsg
				rf.lastApplied++

			}
			// endTime := time.Now()
			// elapsedTime := endTime.Sub(startTime)
			// DPrintf("%d apply %v 代码运行时间: %s true\n", rf.me, log.Command, elapsedTime)
			return true
		}

	}
	// endTime := time.Now()
	// elapsedTime := endTime.Sub(startTime)
	// DPrintf("%d apply %v 代码运行时间: %s false\n", rf.me, log.Command, elapsedTime)

	return false

}

func (rf *Raft) retryAppendEntries(i int, Reply *AppendEntriesReply) *AppendEntriesReply {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// DPrintf("%d send to %d\n", rf.me, i)

	if !Reply.Success && Reply.Term == rf.currentTerm && !rf.killed() {
		term := rf.currentTerm
		Reply = &AppendEntriesReply{}
		for !Reply.Success {
			//回退一个term的Entries,直到
			for rf.logs[rf.nextIndex[i]-1].Term == term && rf.nextIndex[i]-1 != 0 && rf.nextIndex[i] > rf.matchIndex[i]+1 {
				rf.nextIndex[i]--
				if rf.logs[rf.nextIndex[i]].Term == rf.currentTerm {
					break
				}
			}
			// DPrintf("rf.nextIndex[i] %d\n", rf.nextIndex[i])
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
			Entries:      rf.logs[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex}, Reply)
		// DPrintf("%d:%d send %d logs to %d %v\n", GetGid(), rf.me, len(rf.logs[rf.nextIndex[i]:]), i, Reply.Success)
	}
	if Reply.Success {
		rf.nextIndex[i] = len(rf.logs)
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
		if rf.state == Candidate {

			rf.elect()

		}
		// Your code here (2A)
		// Check if a leader election should be started.

		if rf.state == Leader {
			for rf.state == Leader {

				ReplyNum := 0
				rf.start.Lock()
				for i := 0; i < len(rf.peers); i++ {
					if rf.state != Leader {
						break
					}
					// if rf.me == i {
					// 	continue
					// }
					Reply := &AppendEntriesReply{}
					// fmt.Println(rf.me, rf.logs[len(rf.logs)-1].Index)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.logs[len(rf.logs)-1].Index,
						PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
						Entries:      nil,
						LeaderCommit: rf.commitIndex}
					ok := rf.sendAppendEntries(i, args, Reply)
					if Reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = Reply.Term
						rf.persist()
						break
					}

					Reply = rf.retryAppendEntries(i, Reply)
					DPrintf(dTimer, "S%d ticked S%d %v", rf.me, i, Reply.Success)
					// println("Reply.Term", Reply.Term, Reply.Success, rf.me, i)
					if Reply.Success && ok {
						ReplyNum++
					}
					if ReplyNum > len(rf.peers)/2 {
						rf.commitIndex = len(rf.logs) - 1
						rf.persist()
						DPrintf(dLeader, "S%d update CI:%d", rf.me, rf.commitIndex)
						// rf.currentTerm = Reply.Term
					}
					// 获取结束时间

				}

				if ReplyNum <= len(rf.peers)/2 {
					rf.state = Follower
					DPrintf(dLeader, "S%d convert follower", rf.me)
					rf.persist()
					DPrintf(dPersist, "S%d presist T:%d,VF:%d", rf.me, rf.currentTerm, rf.votedFor)
					rf.start.Unlock()
					break
				}
				rf.start.Unlock()
				ms := 50
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		if rf.state == Follower {
			rf.state = Candidate
			rf.persist()
		}
		rf.setTimer()
		<-rf.timer

	}
}
func (rf *Raft) setTimer() {
	ms := 100 + (rand.Int63() % 350)
	rf.timer = time.After(time.Duration(ms) * time.Millisecond)
}
func (rf *Raft) elect() {
	// if !rf.PreElect() {
	// 	return
	// }

	voteforme := 1
	rf.currentTerm++
	Reply := &RequestVoteReply{}
	DPrintf(dVote, "S%d start elect T:%d", rf.me, rf.currentTerm)

	// startTime := time.Now()
	DPrintf(dVote, "S%d -> S%d asked vote", rf.me, rf.me)
	rf.votedFor = rf.me

	// endTime := time.Now()
	// elapsedTime := endTime.Sub(startTime)
	// DPrintf("%d<-%d代码运行时间: %s %v\n", rf.me, rf.me, elapsedTime, Reply.VoteGranted)
	if Reply.VoteGranted {
		voteforme++
	}

	for i := 0; i < len(rf.peers); i++ {
		Reply = &RequestVoteReply{}
		if rf.me == i {
			continue
		}
		// startTime := time.Now()
		DPrintf(dVote, "S%d -> S%d asked vote LT:%d LI:%d", rf.me, i, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index)
		// endTime := time.Now()
		ok := rf.sendRequestVote(i, &RequestVoteArgs{rf.currentTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term, rf.commitIndex, false}, Reply)

		// elapsedTime := endTime.Sub(startTime)
		//DPrintf("%d<-%d代码运行时间: %s %v\n", rf.me, i, elapsedTime, Reply.VoteGranted)
		if Reply.VoteGranted && ok {
			DPrintf(dVote, "S%d <- S%d got vote", rf.me, i)
			voteforme++
		}
		if rf.state == Follower {
			return
		}
		if Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = Reply.Term
			rf.persist()
			return
		}
		if voteforme > len(rf.peers)/2 {
			rf.state = Leader
			rf.persist()
			// if rf.logs[len(rf.logs)-1].Index != 0 {
			// 	log :=
			// 		&LogEntry{Term: rf.currentTerm, Index: rf.logs[len(rf.logs)-1].Index + 1, Command: nil}
			// 	rf.logs = append(rf.logs, log)
			// }
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
			}

			DPrintf(dLeader, "S%d Achieved Majority for T1 (3), converting to Leader", rf.me)
			// rf.broadcastHeartbeat()
			// rf.broadcastEntries(&LogEntry{Term: rf.currentTerm, Index: rf.logs[len(rf.logs)-1].Index + 1, Command: nil})
			// rf.electionTimer.Stop()
			// rf.broadcastHeartbeat()
			return
		}

	}
	rf.state = Follower
}

func (rf *Raft) PreElect() bool {
	voteforme := 0
	PreTerm := rf.currentTerm + 1
	Reply := &RequestVoteReply{}
	DPrintf(dLeader, "S%d start pre-elect T:%d", rf.me, PreTerm)
	// startTime := time.Now()
	DPrintf(dPVote, "S%d -> S%d asked pre-vote", rf.me, rf.me)
	rf.sendRequestVote(rf.me, &RequestVoteArgs{PreTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term, rf.commitIndex, true}, Reply)

	// endTime := time.Now()
	// elapsedTime := endTime.Sub(startTime)
	// DPrintf("%d<-%d代码运行时间: %s %v\n", rf.me, rf.me, elapsedTime, Reply.VoteGranted)
	if Reply.VoteGranted {
		voteforme++
	}

	for i := 0; i < len(rf.peers); i++ {
		Reply = &RequestVoteReply{}
		if rf.me == i {
			continue
		}
		// startTime := time.Now()
		DPrintf(dPVote, "S%d -> S%d asked pre-vote LT:%d LI:%d", rf.me, i, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index)
		// endTime := time.Now()
		ok := rf.sendRequestVote(i, &RequestVoteArgs{PreTerm, rf.me, rf.logs[len(rf.logs)-1].Index, rf.logs[len(rf.logs)-1].Term, rf.commitIndex, true}, Reply)

		// elapsedTime := endTime.Sub(startTime)
		//DPrintf("%d<-%d代码运行时间: %s %v\n", rf.me, i, elapsedTime, Reply.VoteGranted)
		DPrintf(dPVote, "S%d <- S%d VG:%v,ok:%v", rf.me, i, Reply.VoteGranted, ok)
		if Reply.VoteGranted && ok {
			DPrintf(dPVote, "S%d <- S%d got pre-vote", rf.me, i)
			voteforme++
		}
		if rf.state == Follower {
			return false
		}
		if Reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.currentTerm = Reply.Term
			rf.persist()
			return false
		}
		if voteforme > len(rf.peers)/2 {
			DPrintf(dLeader, "S%d Alive, pre-elect succeed", rf.me)
			return true
		}

	}
	rf.state = Follower
	return false
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait()
		}
		rf.lastApplied++
		cmtidx := rf.lastApplied
		command := rf.logs[cmtidx].Command
		rf.mu.Unlock()
		// commit the log entry
		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: cmtidx,
		}
		// this line may be blocked
		rf.applyCh <- msg

	}
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
	rf.preCommand = nil
	rf.lastApplied = 0
	// rf.nilNum = 0
	rf.state = Follower
	rf.cond = sync.NewCond(&rf.mu)

	log := LogEntry{Term: 0, Command: nil, Index: 0}
	rf.logs =
		make([]*LogEntry, 0)
	rf.logs = append(rf.logs, &log)

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index + 1
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
