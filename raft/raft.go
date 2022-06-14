// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers  ->
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
	logger                *log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := newLog(c.Storage)

	raft := &Raft{
		id:               c.ID,
		Lead:             None,
		Prs:              make(map[uint64]*Progress),
		RaftLog:          raftLog,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           log.New(),
	}

	if !IsEmptyHardState(hardState) {
		if hardState.Commit < raft.RaftLog.committed || hardState.Commit > raft.RaftLog.LastIndex() {
			log.Panicf("%x state.commit %d is out of range [%d, %d]", raft.id, hardState.Commit, raft.RaftLog.committed, raft.RaftLog.LastIndex())
		}
		raft.Term = hardState.Term
		raft.Vote = hardState.Vote
		raft.RaftLog.committed = hardState.Commit
	}

	for _, peer := range c.peers { //这里有点问题，restart时peer是空的，应该从config中拿
		raft.Prs[peer] = &Progress{Next: 1, Match: 0}
	}

	if c.Applied > 0 {
		raft.RaftLog.appliedTo(c.Applied)
	}
	raft.becomeFollower(raft.Term, None)

	return raft
}

// sendAppend sends an truncateAndAppend RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	m := pb.Message{}
	nextIndex := r.Prs[to].Next
	m.To = to
	term, errt := r.RaftLog.Term(nextIndex - 1)

	if errt != nil {
		//Snapshot

	} else {
		//AppendEntries操作
		m.MsgType = pb.MessageType_MsgAppend
		m.Term = r.Term
		m.LogTerm = term
		m.Index = nextIndex - 1 //prevLogIndex=nextIndex-1

		var entries []*pb.Entry
		for i := nextIndex; i <= r.RaftLog.LastIndex(); i++ {
			entries = append(entries, &r.RaftLog.entries[i-r.RaftLog.firstIndex()])
		}
		m.Entries = entries
		m.Commit = r.RaftLog.committed
	}
	r.send(m)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()

	case StateLeader:
		r.tickHeartBeat()

	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	//投给自己
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	// NOTE: Leader should propose a noop entry on its term
	//r.logger.Infof("%x become leader at term %+v", r.id, r.Term)

	r.appendEntry(&pb.Entry{Data: nil})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//r.logger.Infof("%x step msg %+v", r.id, m)

	//检查term
	switch {
	case m.Term == 0:
		//本地消息
	case m.Term > r.Term:
		//收到具有更大Term的消息
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgAppend {
			//已经有新Leader了，发送来心跳或者AE
			r.becomeFollower(m.Term, m.From)
		} else {
			//有新的Candidate来请求投票了
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		//收到更小Term的消息,回复
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse})
		}
		return nil
	}

	//处理HUP、Vote信息，即follwer、candidate、leader都可以进行该判断
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		//收到tickElection超时后触发的Hup消息，开始一轮选举
		if r.State != StateLeader {
			//r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			r.startElection()
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MessageType_MsgRequestVote:
		//在m.Term > r.Term中已经处理了，不会再出现该情况，在这里只会两者相等
		canVote := (r.Vote == None || r.Vote == m.From) &&
			(r.RaftLog.LastTerm() < m.LogTerm || (r.RaftLog.LastTerm() == m.LogTerm && r.RaftLog.LastIndex() <= m.Index))
		if canVote {
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}

	default:
		//其他消息，根据状态进行处理
		switch r.State {
		case StateFollower:
			r.stepFollower(m)
		case StateCandidate:
			r.stepCandidate(m)
		case StateLeader:
			r.stepLeader(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term}
	lastNew := uint64(0) //返回append操作后最新的index给leader更新
	if r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		lastNew = m.Index + uint64(len(m.Entries)) //prevLogIndex+len(entries)
		//搜索矛盾点
		var conflictIndex uint64
		for _, entry := range m.Entries {
			if !r.RaftLog.matchTerm(entry.Index, entry.Term) {
				conflictIndex = entry.Index
				break
			}
		}
		if conflictIndex == 0 {
			//没有冲突，啥也不做
		} else if conflictIndex <= r.RaftLog.committed {
			//传入的数据出错，how?
			r.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", conflictIndex, r.RaftLog.committed)
		} else {
			//r.appendEntry(m.Entries[conflictIndex-m.Index-1:]...) //这是Leader才自主进行的append方法
			r.RaftLog.truncateAndAppend(m.Entries)
		}

		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		r.RaftLog.commitTo(min(m.Commit, lastNew))
		response.Index = lastNew
	} else {
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
		response.Index = m.Index
		response.Reject = true
	}
	r.send(response)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//首先判断处理m中的commit信息，然后回复
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, Index: r.RaftLog.LastIndex(), MsgType: pb.MessageType_MsgHeartbeatResponse, Commit: r.RaftLog.committed})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) reset(term uint64) {
	//重新设置term、重新设置超时时间、重置Lead、Vote等
	if r.Term != term {
		//新的term，说明遇到了具有更大term的msg,重新设置voteFor,会在step中确认投票时改成对应的Vote
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTimeout()

	//重置votes、progress等信息
	r.votes = make(map[uint64]bool)
	for peerId := range r.Prs {
		//只有Leader节点用得到这个数据
		r.Prs[peerId] = &Progress{Next: r.RaftLog.LastIndex() + 1, Match: 0}
		if peerId == r.id {
			r.Prs[peerId].Match = r.RaftLog.LastIndex()
		}
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) send(message pb.Message) {
	message.From = r.id
	//r.logger.Infof("%x send msg %+v", r.id, message)
	r.msgs = append(r.msgs, message)
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		//follower收到了heartbeat消息
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		//收到prop请求，转发给Leader
		if r.Lead != None {
			m.To = r.Lead
			r.send(m)
		}
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	}

}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgRequestVoteResponse:
		_, reject, res := r.pollAndCountVotes(m.From, !m.Reject)
		if res {
			r.becomeLeader()
			r.broadCastAppend() //变成Leader之后添加了noop，broadcast一下
		} else if reject >= int(len(r.Prs)/2)+1 {
			//选举失败，超过一半的节点拒绝了
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgAppend:
		//candidate收到了term>=自身的心跳，说明有新的Leader已经产生了
		//在此时，只有m.Term == r.Term，因为若m.Term>r.term,
		//会在step中直接变为follower
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		//candidate收到了heartbeat消息:
		//如果说是更大的term到来，则会在前面的step中自动转为follower，不会来到这里
		//所以只能是以相同term竞选的candidate成功当选成为Leader后发送心跳到来，candidate要转为follower
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		//收到本地的prop请求，然而candidate并不知道Leader，丢弃请求
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadCastHeartBeat()
	case pb.MessageType_MsgPropose:
		//收到prop请求，触发Leader对log的操作
		if len(m.Entries) == 0 { //检查entries数组是否没有数据，这是一个保护性检查。
			r.logger.Panicf("%x stepped empty MsgPropose", r.id)
		}
		r.appendEntry(m.Entries...) //在appendEntry里面会更新match、commit等值
		r.broadCastAppend()
	case pb.MessageType_MsgHeartbeatResponse:
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}

}

func (r *Raft) startElection() {
	//首先，变为Candidate
	r.becomeCandidate()
	//修改votes状态信息，为自己投一票
	_, _, res := r.pollAndCountVotes(r.id, true)
	if res {
		//只有一个节点，直接成为Leader
		r.becomeLeader()
		return
	}
	//给其他peer发送投票请求
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		r.send(pb.Message{Term: r.Term, To: id, MsgType: pb.MessageType_MsgRequestVote, Index: lastIndex, LogTerm: lastTerm})
	}
}

func (r *Raft) pollAndCountVotes(id uint64, voteResult bool) (int, int, bool) {
	//修改votes
	r.votes[id] = voteResult
	granted, rejected := 0, 0
	for peerId := range r.Prs {
		v, ok := r.votes[peerId]
		if !ok {
			//peerId的票还没有结果
			continue
		}
		if v {
			//peerId同意投票
			granted++
		} else {
			//peerId拒绝投票
			rejected++
		}
	}
	return granted, rejected, granted >= int(len(r.Prs)/2)+1
}

func (r *Raft) broadCastHeartBeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

func (r *Raft) appendEntry(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIndex + uint64(i) + 1
	}
	r.RaftLog.truncateAndAppend(entries)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.tryCommitAndBroadCast()
}

func (r *Raft) broadCastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		//append失败了，尝试回退nextIndex后重新发送
		r.Prs[m.From].Next = max(1, r.Prs[m.From].Next-1)
		r.sendAppend(m.From)
	} else {
		//更新该节点的next、match
		if r.Prs[m.From].Match < m.Index {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1
			//更新match之后检测更新这个commited
			r.tryCommitAndBroadCast()
		}
	}
}

func (r *Raft) tryCommitAndBroadCast() {
	//获取所有的match值排序后取中位数
	temp := make(uint64Slice, 0, len(r.Prs))
	for id := range r.Prs {
		temp = append(temp, r.Prs[id].Match)
	}
	sort.Sort(temp)
	if r.RaftLog.tryCommit(temp[int(len(r.Prs)-1)/2], r.Term) {
		//r.logger.Infof("%x commit to %x", r.id, r.RaftLog.committed)
		//更新了commit，要broadcast告诉大家
		r.broadCastAppend()
	}
}
