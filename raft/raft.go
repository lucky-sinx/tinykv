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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"sort"
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

	// log replication progress of each peers
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
	electionTimeout       int
	randomElectionTimeout int
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
}

// 读取持久化数据
func (r *Raft) loadHardState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		panic(fmt.Sprintf("[%v]--state.commit[%d]-- is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex()))
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// 读取持久化数据
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	raftLog := newLog(c.Storage)
	r := &Raft{
		id:      c.ID,
		Term:    0,
		Vote:    None,
		RaftLog: raftLog,
		Prs:     make(map[uint64]*Progress, 0),

		msgs: make([]pb.Message, 0),

		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}
	r.becomeFollower(r.Term, None)
	if !IsEmptyHardState(hardState) {
		r.loadHardState(hardState)
	}

	// 可能上层会直接告诉你已经apply到哪个位置了
	if c.Applied <= raftLog.committed && c.Applied >= raftLog.applied {
		raftLog.applied = c.Applied
	} else if c.Applied != 0 {
		panic(fmt.Sprintf("[%v]--c.applied[%d]-- is out of range [prevApplied(%d), committed(%d)]", r.id, c.Applied, raftLog.applied, raftLog.committed))
	}

	DPrintf("[%v]--init--:Server--Term-%v-lastApplied-%v-commitIndex-%v\n\n", r.id, r.Term, r.RaftLog.applied, r.RaftLog.committed)
	return r
}

// 将消息放入队列
func (r *Raft) send(m pb.Message) {
	r.msgs = append(r.msgs, m)
}

func (r *Raft) dealReplicationL(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	pr := r.Prs[to]

	//需要发送的所有entry
	lastIndex := r.RaftLog.LastIndex()
	if lastIndex >= pr.Next {
		tmpEntries, _ := r.RaftLog.sliceInAll(pr.Next, r.RaftLog.LastIndex())
		for i, _ := range tmpEntries {
			// 记住取不同位置的指针要用tmpEntries[i]，而不是_,entry = ...，否则实际上是同一个地址
			m.Entries = append(m.Entries, &tmpEntries[i])
		}
	}
	// 就算没新数据，preLogIndex应该也是最后一个Log的Index
	m.Index = pr.Next - 1
	m.LogTerm, _ = r.RaftLog.Term(m.Index)
	r.send(m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	p := r.Prs[to]
	// 无entries发送
	//if r.RaftLog.LastIndex() < p.Next {
	//	return false
	//}
	if p.Next < r.RaftLog.FirstIndex() {
		//已经snapshot了
	} else {
		r.dealReplicationL(to)
	}
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		// 通过心跳同步commit时只能匹配到match对应位置
		Commit: min(r.Prs[to].Match, r.RaftLog.committed),
	}
	r.send(m)
}

// 广播append消息
func (r *Raft) broadcastAppend() {
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// 广播append消息
func (r *Raft) broadcastHeartBeat() {
	for to := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

// 广播append消息
func (r *Raft) broadcastVote() {
	r.becomeCandidate()
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
	}
	m.LogTerm, _ = r.RaftLog.Term(m.Index)

	for to := range r.Prs {
		if to != r.id {
			m.To = to
			r.send(m)
		}
	}

	// 单节点直接选举成功
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.broadcastHeartBeat()
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		// Leader超时处理
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			})
			DPrintf("[%v]--doHeartBeat--:begin to send HeartBeat-%v", r.id, r.Term)
		}
	} else {
		r.electionElapsed++
		// Follow,Candidate超时处理
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
			})
		}
	}
}

func (r *Raft) resetRandomElectionTime() {
	//DPrintf("\n%v\n", r.electionTimeout)
	r.randomElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}
func (r *Raft) resetTerm(term uint64) {
	r.Term = term
	r.Vote = None
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomElectionTime()
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetTerm(term)
	r.State = StateFollower
	r.Lead = lead

	DPrintf("[%v]--init--:Follower--[Term-%v,Lead-%v]", r.id, r.Term, r.Lead)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetTerm(r.Term + 1)
	r.State = StateCandidate
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	DPrintf("[%v]--init--:Candidate--[Term-%v,Lead-%v]", r.id, r.Term, r.Lead)

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.resetTerm(r.Term)
	r.State = StateLeader
	r.Lead = r.id

	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = 0
	}
	DPrintf("[%v]--init--:Leader--[Term-%v,Lead-%v]\n\n", r.id, r.Term, r.Lead)

	// append一条空消息
	r.appendEntry(&pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	r.broadcastAppend()
}

// append新的日志条目
func (r *Raft) appendEntry(entries ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entries[i])
	}
	r.Prs[r.id].Match = lastIndex + uint64(len(entries))
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if len(r.Prs) == 1 {
		r.updateCommitIndexL()
	}
	DPrintf("[%v]--AcceptCommand--:new entry at Index-[%v,%v] Term-%v", r.id, lastIndex+1, lastIndex+uint64(len(entries)), r.Term)
}

// 处理Leader消息
func (r *Raft) stepLeader(m *pb.Message) error {
	switch m.MsgType {
	// 收到新entry
	case pb.MessageType_MsgPropose:
		if m.Entries != nil && len(m.Entries) != 0 {
			r.appendEntry(m.Entries...)
		}
		r.broadcastAppend()
	// 发送心跳
	case pb.MessageType_MsgBeat:
		r.broadcastHeartBeat()

	// 处理心跳response
	case pb.MessageType_MsgHeartbeatResponse:
		if r.Term >= m.Term {
			pr := r.Prs[m.From]
			if r.RaftLog.LastIndex() >= pr.Next {
				r.sendAppend(m.From)
			}
		}

	// 处理AppendEntry response
	case pb.MessageType_MsgAppendResponse:
		pr := r.Prs[m.From]

		if m.Reject {
			DPrintf("[%v]--AE_Response--ConflictAndNewNext--:To [%v],myTerm-%v,oldNext-%v,newNext-%v", r.id, m.From, r.Term, pr.Next, m.Index)
			// 更新nextIndex寻找最大共识
			pr.Next = m.Index
			// 消息重发
			r.sendAppend(m.From)
			return nil
		}

		//更新matchIndex
		nxtMatchIndex := m.Index
		if nxtMatchIndex == pr.Match {
			DPrintf("[%v]--AE_Response--Ignore-:success append to [%v]", r.id, m.From)
		} else if nxtMatchIndex < pr.Match {
			DPrintf("[%v]--AE_Response--Ignore-:old response to [%v]", r.id, m.From)
		} else {
			pr.Match = nxtMatchIndex
			pr.Next = nxtMatchIndex + 1
			DPrintf("[%v]--AE_Response--UpdateMatch-%v--:success append to [%v],myTerm_%v", r.id, nxtMatchIndex, m.From, r.Term)
			r.updateCommitIndexL()
		}
	}

	return nil
}

// 处理Leader消息
func (r *Raft) stepFollow(m *pb.Message) error {
	switch m.MsgType {
	// 发送Vote
	case pb.MessageType_MsgHup:
		r.broadcastVote()
	// 收到新entry，转发给Leader
	case pb.MessageType_MsgPropose:
		{
			if r.Lead == None {
				DPrintf("[%v]--no leader at term-%v,Follow", r.id, r.Term)
				return ErrProposalDropped
			}
			m.To = r.Lead
			r.send(*m)
		}
	}

	return nil
}

// 处理Candidate消息
func (r *Raft) stepCandidate(m *pb.Message) error {
	switch m.MsgType {
	// 收到新entry，转发给Leader
	case pb.MessageType_MsgPropose:
		DPrintf("[%v]--no leader at term-%v,candidate", r.id, r.Term)
		return ErrProposalDropped
	// 发送Vote
	case pb.MessageType_MsgHup:
		r.broadcastVote()
	// 处理Vote Response
	case pb.MessageType_MsgRequestVoteResponse:
		// 老Term的投票结果无用，超时时新Term会重置votes
		if m.Term == r.Term {
			r.votes[m.From] = !m.Reject
			if !m.Reject {
				DPrintf("[%v]--Vote_Response--:getVote from [%v]", r.id, m.From)
			} else {
				DPrintf("[%v]--Vote_Response--:rejectVote from [%v]", r.id, m.From)
			}
			voteCnt := 0
			finishCnt := len(r.votes)
			for _, res := range r.votes {
				if res {
					voteCnt++
				}
			}

			if voteCnt >= len(r.Prs)/2+1 {
				// 选举成功
				r.becomeLeader()
				//r.broadcastHeartBeat()
			} else if finishCnt >= len(r.Prs) {
				// 选举失败
				DPrintf("[%v]--RoleChange--:Vote Failed", r.id)
				r.becomeFollower(r.Term, None)
			}
		}
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 来了term大的，转follow
	if m.Term > r.Term {
		DPrintf("[%v]--RoleChange--:get Message more Term from Peer-%v--", r.id, m.From)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
		// 投票需要Term一样
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			return nil
		}
	} else if m.Term == r.Term && r.State == StateCandidate {
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
			DPrintf("[%v]--RoleChange--:get Message more Term from Peer-%v--", r.id, m.From)
		}
	}
	// 所有State都需要处理的消息
	switch m.MsgType {
	// 处理Vote Request
	case pb.MessageType_MsgRequestVote:
		r.handleVote(&m)
	// 处理心跳request
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(&m)
	// 处理Append request
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}

	// 节点状态改变
	switch r.State {
	case StateFollower:
		return r.stepFollow(&m)
	case StateCandidate:
		return r.stepCandidate(&m)
	case StateLeader:
		return r.stepLeader(&m)
	}
	return nil
}

// 当Leader的marchIndex更新时，尝试更新commitIndex
func (r *Raft) updateCommitIndexL() {
	//Leader更新commitIndex
	var tmp = make([]int, len(r.Prs))
	for i, progress := range r.Prs {
		tmp[i-1] = int(progress.Match)
	}
	tmp[r.id-1] = int(r.RaftLog.LastIndex())
	sort.Ints(tmp)
	nxtCommitMax := uint64(tmp[(len(r.Prs)-1)/2])

	//DPrintf("[%v]--matchIndex-%v,nxtCommitMax-%v,Index0-%v\n\n", rf.me, rf.matchIndex, nxtCommitMax, rf.log.getIndexIndexL(0))
	//一定要先判断commitIndex是不是小些,因为它初始化比现有的snapshotIndex大,否则可能数组越界
	if nxtCommitMax >= r.RaftLog.committed && r.RaftLog.getTerm(nxtCommitMax) == r.Term {
		//更新commmit
		if r.RaftLog.committed == nxtCommitMax {
			//DPrintf("[%v]--AE_True--UpdateCommit--Same:commitIndex-%v,nxtCommitMax-%v", r.id, r.RaftLog.committed, nxtCommitMax)
		} else {
			DPrintf("[%v]--AE_True--UpdateCommit--Success:commitIndex-%v,nxtCommitMax-%v", r.id, r.RaftLog.committed, nxtCommitMax)
			r.RaftLog.committed = nxtCommitMax
			r.broadcastAppend()
		}
	} else {
		if nxtCommitMax < r.RaftLog.committed {
			//DPrintf("[%v]--AE_True--UpdateCommit--CommitLittle--:commitIndex-%v,nxtCommitMax-%v", r.id, r.RaftLog.committed, nxtCommitMax)
		} else {
			//DPrintf("[%v]--AE_True--UpdateCommit--TermLittle--:commitIndex-%v,nxtCommitMax-%v,nxtCommitTerm-%v,cureentTerm-%v", r.id, r.RaftLog.committed, nxtCommitMax, r.RaftLog.getTerm(nxtCommitMax), r.Term)
		}
	}
}

// 处理Vote Request
func (r *Raft) handleVote(m *pb.Message) {
	// 获得最后log
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	mRes := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	//判断是否需要投票
	if m.Term >= r.Term && (r.Vote == None || r.Vote == m.From) &&
		(m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastLogIndex)) {
		r.Term = m.Term
		r.Vote = m.From
		r.Lead = None
		r.electionElapsed = 0
		mRes.Reject = false
	} else {
		mRes.Reject = true
	}
	r.send(mRes)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m *pb.Message) {
	// Your Code Here (2A).
	mRes := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term >= r.Term {
		if m.Commit > r.RaftLog.committed {
			//nxtCommitMax := min(min(m.Commit, m.Index+uint64(len(m.Entries))), r.RaftLog.LastIndex())
			nxtCommit := min(m.Commit, r.RaftLog.LastIndex())
			DPrintf("[%v]--HB_Request--UpdateCommit--:commitIndex-%v,nxtCommitMax-%v,Term-%v", r.id, r.RaftLog.committed, nxtCommit, r.Term)
			r.RaftLog.committed = nxtCommit
		} else {
			DPrintf("[%v]--HB_Request--:Term-%v", r.id, r.Term)
		}
		r.electionElapsed = 0
		r.Lead = m.From
	}
	r.send(mRes)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	mRes := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   0,
		LogTerm: None,
		Reject:  false,
	}
	if m.Term < r.Term {
		r.send(mRes)
		return
	}
	r.electionElapsed = 0
	r.Lead = m.From

	// 过时消息
	if m.Index < r.RaftLog.committed {
		mRes.Index = r.RaftLog.committed
		mRes.LogTerm = 1
		r.send(mRes)
		return
	}

	// 更新Log
	lastLogIndex := r.RaftLog.LastIndex()
	firstLogIndex := r.RaftLog.FirstIndex()

	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm

	if lastLogIndex < m.Index {
		// preLogIndex超出长度
		mRes.Index = lastLogIndex + 1
		mRes.Reject = true
		DPrintf("[%v]--AE_Request--conflict--LackEntry--:To [%v],myTerm-%v,LeaderTerm-%v,LastIndex-%v,PrevLogIndex-%v", m.From, r.id, r.Term, m.Term, lastLogIndex, m.Index)

		r.send(mRes)
		return
	}
	myPreTerm, _ := r.RaftLog.Term(m.Index)
	if myPreTerm != m.LogTerm {
		// preLogIndex处Term冲突
		mRes.Index = m.Index
		mRes.Reject = true
		// 既然myPreTerm和LeaderPreTerm不同，可以直接往前匹配整个myPreTerm算作冲突，减少通信次数
		// 注意此时mRes是Uint64的，0-1会死循环
		for mRes.Index != 0 && mRes.Index-1 >= firstLogIndex {
			nxtTerm, _ := r.RaftLog.Term(mRes.Index - 1)
			if nxtTerm == myPreTerm {
				mRes.Index--
			} else {
				break
			}
		}
		DPrintf("[%v]--AE_Request--conflict--ConflictEntry--:To [%v],myTerm-%v,LeaderTerm-%v,preLogIndex-%v,myPreLogTerm-%v,leaderPreLogTerm-%v,nextIndex-%v", m.From, r.id, r.Term, m.Index, m.Term, myPreTerm, m.LogTerm, mRes.Index)

		r.send(mRes)
		return
	}

	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	//Append any new entries not already in the log
	// 有发空的AppendEntries
	if m.Entries == nil || len(m.Entries) == 0 {
		mRes.Index = m.Index
	} else {
		mRes.Index = m.Entries[len(m.Entries)-1].Index
	}
	//寻找冲突点
	i := m.Index + 1
	j := 0
	for m.Entries != nil && i <= lastLogIndex && j < len(m.Entries) {
		tmpTerm, _ := r.RaftLog.Term(i)
		if tmpTerm == m.Entries[j].Term {
			i++
			j++
		} else {
			break
		}
	}

	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	if j >= len(m.Entries) {
		// 发过来的entries完全匹配
		DPrintf("[%v]--AE_Request--Ignore--:To [%v],LastIndex-%v,LastTerm-%v", m.From, r.id, lastLogIndex, lastLogTerm)
	} else {
		// 需要将m.Entries[j:]连接到r.log.Entries[0,i-1]后面
		//if i-1 != m.Entries[j-1].Index {
		//	panic(fmt.Sprintf("check the function,i-%v,m.Entries[j].Index-%v", i, m.Entries[j].Index))
		//}
		r.RaftLog.truncateAndAppend(getEntries(m.Entries[j:]))
		DPrintf("[%v]--AE_Request--Success--:To [%v],myTerm-%v,LeaderTerm-%v,afterIndex-%v,LastIndex-%v", m.From, r.id, r.Term, m.Term, i-1, r.RaftLog.LastIndex())
	}

	//更新commit
	if m.Commit > r.RaftLog.committed {
		//If leaderCommit > commitIndex, set commitIndex =
		//min(leaderCommit, index of last new entry)
		//更新commmit
		//nxtCommitMax := min(m.Commit, r.RaftLog.LastIndex())
		//Leader commit了并且共享来的数据可以commit
		nxtCommitMax := min(min(m.Commit, m.Index+uint64(len(m.Entries))), r.RaftLog.LastIndex())
		DPrintf("[%v]--AE_Request--UpdateCommit--Success--:oldCommitIndex-%v,nxtCommitMax-%v", r.id, r.RaftLog.committed, nxtCommitMax)
		r.RaftLog.committed = nxtCommitMax
	} else {
		//DPrintf("[%v]--AE_Request--UpdateCommit--No--:commitIndex-%v,LeaderCommit-%v", r.id, r.RaftLog.committed, m.Commit)
	}
	r.send(mRes)
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
