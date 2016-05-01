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

import "sync"
import (
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                                   State //  current state. leader,follower, candidate
	electionTimeoutLow, electionTimeoutHigh int   // range for init a election term, milliseconds
	term                                    int   // current term, Persistent
	heartBeatPeriod                         int   // milliseconds for sending heatBeat to all peers
	leaderId                                int   // leader's index into peers[]

	// candidate, leader
	stalenessChan chan bool // detect stale term -> back to follower

	// follower, candidate
	votedTerms    map[int]int // Persistent. term -> voted id. vote for at most one server in any given term. Equals to VotedFor in the original paper
	heartBeatChan chan bool

	// candidate
	votes             Votes
	voteChan          chan *Vote
	leaderElectedChan chan bool // others become leader -> back to follower

	// control
	stateTranChan chan State // chan for new state
	quit          chan bool
	retry         int // retry for RequestVote and AppendEntries

	// log replication
	ml           sync.Mutex
	log          LogStore // Persistent
	commitIndex  int
	lastApplied  int
	agents       []*Agent        // leader uses
	logRepCh     chan *logRep    // leader uses, notify leader's main daemon that a log has been successfully replicated
	replicated   map[int]*IntSet // result from peers for log replicated command
	applyCh      chan ApplyMsg
	applyEventCh chan bool
}

// a log has been successfully replicated
type logRep struct {
	peerId int
	logIdx int
}

// Agent is what a leader uses to communicate with followers
type Agent struct {
	nextIndex   int
	matchIndex  int
	entryChan   chan int // new entry to replicate, if it is greater than nextIndex then postpone it until log[nextIndex] is replicated
	successChan chan int // index of last successfully-replicated entry
	failureChan chan int
	stop        chan bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogStore interface {
	Append(*LogEntry) error
	AppendAll([]*LogEntry) error
	Write(idx int, e *LogEntry) error // write to idx
	WriteAll(idx int, es []*LogEntry) error
	Delete(idx int) error
	DeleteAll(idxFrom, idxTo int) error
	GetLastIndex() int // log index starts from 1
	Len() int
	Get(idx int) *LogEntry
	GetAll(idxFrom, idxTo int) []*LogEntry
	IsMatch(idx int, e *LogEntry) bool // if real entry matches the given entry
}

// for test purpose only
type InMemLogStore struct {
	entries []*LogEntry
}

func NewInMemLogStore() LogStore {
	return &InMemLogStore{make([]*LogEntry, 0)}
}

func (s *InMemLogStore) Append(e *LogEntry) error {
	s.entries = append(s.entries, e)
	return nil
}

func (s *InMemLogStore) AppendAll(es []*LogEntry) error {
	s.entries = append(s.entries, es...)
	return nil
}

func (s *InMemLogStore) Write(idx int, e *LogEntry) error {
	return s.WriteAll(idx, []*LogEntry{e})
}

func (s *InMemLogStore) WriteAll(idx int, es []*LogEntry) error {
	last := s.GetLastIndex()

	if idx <= 0 || idx > last+1 {
		return fmt.Errorf("log index out of bound")
	}

	if idx == last+1 {
		return s.AppendAll(es)
	}

	for i := 0; i < len(es); i++ {
		nextIdx := idx + i
		if nextIdx <= last {
			s.entries[nextIdx-1] = es[i]
		} else {
			s.Append(es[i])
		}
	}

	return nil
}

func (s *InMemLogStore) Delete(idx int) error {
	if idx <= 0 || idx > s.GetLastIndex() {
		return fmt.Errorf("index out of bound")
	}

	s.entries = append(s.entries[:idx-1], s.entries[idx:]...)
	return nil
}

func (s *InMemLogStore) DeleteAll(fromIdx, toIdx int) error {
	if fromIdx <= 0 {
		fromIdx = 1
	}

	if toIdx > s.GetLastIndex() {
		toIdx = s.GetLastIndex()
	}

	if fromIdx > toIdx {
		return nil
	}

	s.entries = append(s.entries[:fromIdx-1], s.entries[toIdx:]...)
	return nil
}

func (s *InMemLogStore) IsMatch(idx int, e *LogEntry) bool {
	r := s.Get(idx)
	if r == nil {
		return idx == 0 && e == nil
	} else {
		return r.Term == e.Term
	}
}

func (s *InMemLogStore) GetLastIndex() int {
	return len(s.entries)
}

func (s *InMemLogStore) Len() int {
	return len(s.entries)
}

func (s *InMemLogStore) Get(idx int) *LogEntry {
	if idx <= 0 || idx > s.GetLastIndex() {
		return nil
	}
	return s.entries[idx-1]
}

func (s *InMemLogStore) GetAll(from, to int) []*LogEntry {
	if from <= 0 {
		from = 1
	}

	if to > s.GetLastIndex() {
		to = s.GetLastIndex()
	}

	if from > to {
		return make([]*LogEntry, 0)
	}

	return s.entries[from-1 : to]
}

// TODO put this to leader's scope
func (rf *Raft) startAgents() {
	for i := 0; i < len(rf.agents); i++ {
		if i != rf.me {
			rf.agents[i] = &Agent{
				nextIndex:   rf.log.GetLastIndex() + 1,
				matchIndex:  0,
				entryChan:   make(chan int),
				successChan: make(chan int, 1),
				failureChan: make(chan int, 1),
				stop:        make(chan bool),
			}
			go rf.startAgent(i)
		}
	}
}

func (rf *Raft) startAgent(i int) {
	agent := rf.agents[i]

	replicate := func() {
		rf.logln(fmt.Sprintf("replicating log entry %v to server %v", agent.nextIndex, i))
		go rf.replicate(i, agent.nextIndex)
	}

	for {
		select {
		case idx := <-agent.entryChan:
			if idx == agent.nextIndex {
				replicate()
			} else {
				rf.logln("postpone replicating entry ", idx)
			}

		case lastSuccess := <-agent.successChan:
			rf.logln(fmt.Sprintf("logs pre to %v have been succesfully replicated on server %v", lastSuccess, i))
			agent.nextIndex = lastSuccess + 1
			agent.matchIndex = lastSuccess
			rf.logRepCh <- &logRep{i, agent.matchIndex}

			// continue to replicate postponed entries if any
			if agent.nextIndex <= rf.log.GetLastIndex() { // still fall behind
				replicate()
			}

		case firstFailure := <-agent.failureChan:
			rf.logln(fmt.Sprintf("log after %v failed to replicate on machine %v", firstFailure, i))
			agent.nextIndex--
			replicate()

		case <-agent.stop:
			rf.logln("stop agent ", i)
			return
		}
	}
}

// TODO can be optimized to send multiple entries each RPC call
func (rf *Raft) replicate(agentId, logIdx int) {
	for {
		ok, reply := rf.sendAppendEntries(agentId, logIdx, logIdx)
		if ok {
			if reply.Success {
				// current server may no longer be leader. the channel should be buffered to prevent deadlock
				rf.agents[agentId].successChan <- logIdx
			} else {
				rf.agents[agentId].failureChan <- logIdx
			}
			break
		} else {
			rf.logln(fmt.Sprintf("failed to make AppendEntries call to server %v, retrying", agentId))
		}
	}
}

// replicate log entries [from, to] to peer[peerId] at the same RPC call
func (rf *Raft) sendAppendEntries(peerId, from, to int) (bool, *AppendEntriesReply) {
	prevTerm := 0
	if prevLog := rf.log.Get(from - 1); prevLog != nil {
		prevTerm = prevLog.Term
	}

	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: from - 1,
		PrevLogTerm:  prevTerm,
		Entries:      rf.log.GetAll(from, to),
		LeaderCommit: rf.commitIndex,
	}

	var reply AppendEntriesReply

	if ok := rf.peers[peerId].Call("Raft.AppendEntries", *args, &reply); !ok {
		// no need to retry as it's already a periodic job
		return false, nil
	} else {
		return true, &reply
	}
}

func (rf *Raft) stopAgents() {
	for i := 0; i < len(rf.agents); i++ {
		if i != rf.me {
			rf.stopAgent(i)
		}
	}
}

func (rf *Raft) stopAgent(i int) {
	close(rf.agents[i].stop)
}

type Vote int
type Votes []*Vote // indexes of voted peers

type State int8

const (
	FOLLOWER  = State(0)
	CANDIDATE = State(1)
	LEADER    = State(2)
)

func (rf *Raft) logln(s ...interface{}) {
	header := fmt.Sprintf("server=%v, term=%v, state=%v : ", rf.me, rf.term, rf.getStateName(rf.state))
	for _, t := range s {
		header += fmt.Sprintf("%v ", t)
	}
	log.Println(header)
}

func (rf *Raft) getStateName(s State) string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	default:
		return "LEADER"
	}
}

// a daemon to perform the state transition
func (rf *Raft) elect() {
	for {
		select {
		case newState := <-rf.stateTranChan:
			switch newState {
			case FOLLOWER:
				rf.setState(FOLLOWER)
				rf.logln("became follower")
				go rf.runAsFollower()
			case CANDIDATE:
				rf.setState(CANDIDATE)
				rf.logln("became candidate")
				go rf.runAsCandidate()
			case LEADER:
				rf.setState(LEADER)
				rf.logln("became leader")
				go rf.runAsLeader()
			}

		case <-rf.quit:
			rf.logln("quit elect daemon")
			return
		}
	}
}

func (rf *Raft) setState(s State) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = s
}

func (rf *Raft) runAsFollower() {
	timeout := make(chan bool)
	clearTimeout := make(chan bool)
	stop := make(chan bool)
	defer close(stop)
	go rf.startElectionTimer(timeout, clearTimeout, stop)

	for {
		select {
		case <-timeout:
			rf.logln("follower times out, becomes candidate")
			rf.becomeCandidate()
			return

		case <-rf.heartBeatChan:
			// receive heartBeat from leader normally. clear term timeout
			clearTimeout <- true

		case <-rf.stalenessChan: // ignore
		case <-rf.voteChan: //vote for candidate, but candidate already becomes follower now. ignore
		case <-rf.quit:
			return
		}
	}
}

func (rf *Raft) startElectionTimer(timeout, clearTimeout, stop chan bool) {
	for {
		select {
		// TODO does this cause too many go routines (each time.After creates a go routine) ?
		case <-rf.electionTimeoutChan():
			timeout <- true
		case <-clearTimeout:
		case <-stop:
			return
		}
	}
}

func (rf *Raft) clearVotes() {
	rf.votes = make([]*Vote, 0)
}

func (rf *Raft) addVote(v *Vote) {
	rf.votes = append(rf.votes, v)
}

func (rf *Raft) runAsCandidate() {
	defer func() {
		rf.clearVotes()
	}()

	for {
		rf.incrementTerm()
		rf.voteSelf()

		select {
		case <-rf.electionTimeoutChan():
			rf.logln("candidate times out, start new election term")

		case <-rf.stalenessChan: // discover new term from peer
			rf.logln("current candidate is stale, back to follower state")
			rf.becomeFollower()
			return

		case v := <-rf.voteChan:
			rf.addVote(v)
			if rf.hasMajority() {
				rf.logln("received votes from majority, becoming leader")
				rf.becomeLeader()
				return
			}

		case <-rf.heartBeatChan: // discover current leader
			rf.logln(fmt.Sprintf("%v was elected as leader\n", rf.leaderId))
			rf.becomeFollower()
			return

		case <-rf.quit:
			return
		}
	}

}

func (rf *Raft) runAsLeader() {
	heartBeat := make(chan bool)
	stop := make(chan bool)
	rf.sendHeartBeats() // establish leadership immediately

	go rf.startHeartBeatTimer(heartBeat, stop)

	defer close(stop)

	for {
		select {
		case <-heartBeat:
			go rf.sendHeartBeats()

		case <-rf.stalenessChan:
			rf.logln("current leader is stale, back to follower")
			rf.becomeFollower()
			return

		case <-rf.voteChan: // vote for candidate, but candidate already becomes leader now, ignore.

		case rep := <-rf.logRepCh:
			rf.updateLogRep(rep)

		case <-rf.quit:
			return
		}
	}
}

func (rf *Raft) updateLogRep(r *logRep) {
	if set, ok := rf.replicated[r.logIdx]; ok {
		set.Add(r.peerId)
		if set.Size() >= rf.majority() {
			rf.logln(fmt.Sprintf("log %v has been replicated to majority peers, start to apply it to leader's local state machine"))

			if r.logIdx > rf.commitIndex {
				entry := rf.log.Get(r.logIdx)
				if entry != nil && entry.Term == rf.term {
					rf.commitIndex = r.logIdx
					rf.logln(fmt.Sprintf("Update commit index to %v", rf.commitIndex))
					rf.sendApplyEvent()
				}
			}

			delete(rf.replicated, r.logIdx)
		}
	} else {
		rf.logln(fmt.Sprintf("log %v has already been commited", r.logIdx))
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

type IntSet struct {
	values map[int]bool
}

func (s *IntSet) Containes(key int) bool {
	_, ok := s.values[key]
	return ok
}

func (s *IntSet) Add(key int) {
	s.values[key] = true
}

func (s *IntSet) Delete(key int) {
	delete(s.values, key)
}

func (s *IntSet) Size() int {
	return len(s.values)
}

func (rf *Raft) startHeartBeatTimer(heartBeat, stop chan bool) {
	for {
		select {
		case <-time.After(time.Duration(rf.heartBeatPeriod) * time.Millisecond):
			heartBeat <- true
		case <-stop:
			return
		}
	}
}

func (rf *Raft) incrementTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term++
}

func (rf *Raft) updateTerm(peerCurrent int) {
	rf.mu.Lock()
	rf.term = peerCurrent
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(id int) {
				ok, reply := rf.sendHeartBeat(id)
				if !ok {
					rf.logln(fmt.Sprintf("failed to send heart beat to peer %v", id))
				} else if !reply.Success {
					rf.logln(fmt.Sprintf("failed to get reply for heart beat from %v", id))
					if rf.term < reply.Term {
						rf.updateTerm(reply.Term)
						rf.stalenessChan <- true
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendHeartBeat(peerId int) (bool, *AppendEntriesReply) {
	args := &AppendEntriesArgs{}
	args.Term = rf.term
	args.LeaderId = rf.me

	var reply AppendEntriesReply

	if ok := rf.peers[peerId].Call("Raft.AppendEntries", *args, &reply); !ok {
		// no need to retry as it's already a periodic job
		return false, nil
	} else {
		return true, &reply
	}
}

func (rf *Raft) voteSelf() {
	rf.addVote(rf.newVote(rf.me))
	rf.votedTerms[rf.term] = rf.me // reject other vote request for the same term

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(id int) {
				ok, reply := rf.requestVote(id)
				if !ok {
					rf.logln(fmt.Sprintf("failed to request vote from %v", id))
				} else if rf.term < reply.Term {
					rf.logln(fmt.Sprintf("sender is stale, candidate term=%v, peer term=%v", rf.term, reply.Term))
					rf.updateTerm(reply.Term)
					rf.stalenessChan <- true
				} else if reply.VoteGranted { // also indicating rf.term == reply.Term
					rf.logln(fmt.Sprintf("vote granted %v -> %v\n", id, rf.me))

					// note at this point the sender may already become leader or follower. sending vote to non-candidate
					// may cause dead-lock if follower or leader doesn't listen voteChan
					rf.voteChan <- rf.newVote(id)
				}
			}(i)
		}
	}
}

// wrapper for RequestVote with retry logic
func (rf *Raft) requestVote(id int) (bool, *RequestVoteReply) {
	args := &RequestVoteArgs{}
	args.Term = rf.term
	args.CandidateId = rf.me

	for j := 0; j < rf.retry; j++ {
		var reply RequestVoteReply
		ok := rf.peers[id].Call("Raft.RequestVote", *args, &reply)
		if ok {
			return true, &reply
		}
	}
	return false, nil
}

func (rf *Raft) newVote(id int) *Vote {
	v := Vote(id)
	return &v
}

func (rf *Raft) retryF(f func() bool, cnt int) {
	for i := 0; i < cnt; i++ {
		if f() {
			break
		}
	}
}

// check if a candidate already owns votes from majority
func (rf *Raft) hasMajority() bool {
	return len(rf.votes) > len(rf.peers)/2
}

func (rf *Raft) becomeFollower() {
	rf.stateTranChan <- FOLLOWER
}

func (rf *Raft) becomeCandidate() {
	rf.stateTranChan <- CANDIDATE
}

func (rf *Raft) becomeLeader() {
	rf.stateTranChan <- LEADER
}

func (rf *Raft) electionTimeoutChan() <-chan time.Time {
	t := rand.Int()%(rf.electionTimeoutHigh-rf.electionTimeoutLow) + rf.electionTimeoutLow
	return time.After(time.Duration(t) * time.Millisecond)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.term, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// it is important to clear stale terms that won't be used to release mem
	for _, t := range rf.votedTerms {
		if t < rf.term {
			delete(rf.votedTerms, t)
		}
	}

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	// vote for candidate whose term >= rf.term, and update rf.term to candidate's term
	if args.Term > rf.term {
		rf.updateTerm(args.Term)
		rf.stalenessChan <- true // if self is candidate, go back to follower
	}

	if _, ok := rf.votedTerms[args.Term]; ok { // already voted for other candidate
		reply.Term = rf.term
		reply.VoteGranted = false
	} else {
		rf.votedTerms[args.Term] = args.CandidateId
		reply.Term = rf.term
		reply.VoteGranted = true
	}
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
	Term    int
	Success bool
}

// follower's behavior as the receiver of AppendEntries call, as described in Figure 2 in the original paper
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.isSenderStale(args.Term) {
		reply.Term = rf.term
		reply.Success = false
		return
	}

	if rf.isStale(args.Term) {
		rf.updateTerm(args.Term)
		rf.stalenessChan <- true
	}
	reply.Term = rf.term

	if rf.isHeartBeat(&args) {
		reply.Success = true
		rf.leaderId = args.LeaderId
		rf.heartBeatChan <- true
		return
	}

	// consistency check before appending entries
	e := &LogEntry{Term: args.PrevLogTerm}
	if args.PrevLogIndex == 0 {
		e = nil
	}

	if !rf.log.IsMatch(args.PrevLogIndex, e) {
		reply.Success = false

		// delete conflicting entry and all entries after it (as described in the original paper)
		rf.log.DeleteAll(args.PrevLogIndex, rf.log.GetLastIndex())
		return
	}

	// write new entries
	err := rf.log.WriteAll(args.PrevLogIndex+1, args.Entries)
	reply.Success = err == nil

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.min(args.LeaderCommit, rf.log.GetLastIndex())
	}

	// start to apply to local state machine
	if rf.lastApplied < rf.commitIndex {
		rf.sendApplyEvent()
	}
}

func (rf *Raft) isStale(senderTerm int) bool {
	return rf.term < senderTerm
}

func (rf *Raft) isSenderStale(senderTerm int) bool {
	return senderTerm < rf.term
}

func (rf *Raft) isHeartBeat(args *AppendEntriesArgs) bool {
	return len(args.Entries) == 0
}

func (rf *Raft) sendApplyEvent() {
	select {
	case rf.applyEventCh <- true: // channel must have non-zero buffer
	default:
	}
}

// apply log entries (lastApplied, commitIndex] to local state machine. running in separate go routine
func (rf *Raft) apply() {
	for {
		for rf.lastApplied < rf.commitIndex {
			entry := rf.log.Get(rf.lastApplied)
			msg := &ApplyMsg{
				Index:   rf.lastApplied,
				Command: entry.Command,
			}
			rf.applyCh <- *msg
			rf.lastApplied++
		}

		_, ok := <-rf.applyEventCh
		if !ok {
			break
		}
	}
}

func (rf *Raft) stopApply() {
	close(rf.applyEventCh)
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	if rf.state == LEADER {
		// apply to local
		entry := rf.makeEntry(command)
		rf.ml.Lock()
		rf.log.Append(entry)
		idx := rf.log.GetLastIndex()
		rf.ml.Unlock()

		// replicate to followers, return immediately
		rf.replicateEntry(idx)

		return idx, rf.term, true
	} else {
		return -1, -1, false
	}
}

func (rf *Raft) replicateEntry(idx int) {
	for _, agent := range rf.agents {
		agent.entryChan <- idx
	}
}

func (rf *Raft) makeEntry(command interface{}) *LogEntry {
	return &LogEntry{
		Term:    rf.term,
		Command: command,
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

	// quit all go routine
	rf.stopAgents()
	rf.stopApply()
	close(rf.quit)
}

func (rf *Raft) min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) max(a, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.electionTimeoutLow = 150
	rf.electionTimeoutHigh = 300
	rf.heartBeatPeriod = 50
	rf.term = 0
	rf.leaderId = -1
	rf.retry = 3
	rf.votedTerms = make(map[int]int)
	rf.heartBeatChan = make(chan bool)
	rf.leaderElectedChan = make(chan bool)
	rf.stalenessChan = make(chan bool)
	rf.stateTranChan = make(chan State)
	rf.voteChan = make(chan *Vote)
	rf.applyEventCh = make(chan bool, 1)
	rf.log = NewInMemLogStore()
	rf.agents = make([]*Agent, len(peers))
	rf.replicated = make(map[int]*IntSet)
	rf.quit = make(chan bool)

	go rf.elect()
	rf.stateTranChan <- FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0
	// must start agent after read from persist store, because nextIndex is initialized to last log index + 1
	go rf.startAgents()
	go rf.apply()

	return rf
}
