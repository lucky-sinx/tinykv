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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
	dummyTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//return nil
	if storage == nil {
		log.Panic("storage is nil")
	}
	newLog := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	newLog.entries = entries
	newLog.applied = firstIndex - 1
	newLog.committed = firstIndex - 1
	newLog.stabled = lastIndex //log entries with index <= stabled are persisted to storage.
	newLog.dummyIndex = firstIndex - 1
	return newLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).

	// 检查applyState,在内存中删除[firstIndex...truncateIndex]的entries,更新dummyIndex、dummyTerm
	firstIndex := l.firstIndex()
	trueNextIndex, _ := l.storage.FirstIndex()
	//log.Infof("RaftLog compact from %d to %d", firstIndex, trueNextIndex)

	if trueNextIndex > firstIndex {
		log.Infof("RaftLog compact from %d to %d", firstIndex, trueNextIndex)
		l.entries = l.entries[trueNextIndex-l.dummyIndex-1:]
		l.dummyIndex = trueNextIndex - 1
		term, err := l.storage.Term(l.dummyIndex)
		if err != nil {

		}
		l.dummyTerm = term
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//return nil
	return l.entries[int(l.stabled-l.dummyIndex):]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//return nil
	return l.entries[int(l.applied-l.dummyIndex):int(l.committed-l.dummyIndex)]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//return 0
	if len(l.entries) == 0 {
		return l.dummyIndex
	}

	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	//return 0
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panic(err)
	}
	return term
}

func (l *RaftLog) firstIndex() uint64 {
	if len(l.entries) == 0 {
		return l.dummyIndex + 1
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//有效term范围[l.dummyIndex,l.lastIndex]
	if i < l.dummyIndex {
		return 0, ErrCompacted
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if i == l.dummyIndex {
		return l.dummyTerm, nil
	}
	term := l.entries[i-l.firstIndex()].Term
	return term, nil
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		log.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

func (l *RaftLog) truncateAndAppend(entries []*pb.Entry) {
	//第一条数据索引
	index := entries[0].Index
	switch {
	case index == l.LastIndex()+1:
		//entries[0].Index==lastLogIndex+1,说明是完全新增的数据,直接append
		for i := range entries {
			l.entries = append(l.entries, *entries[i])
		}
	case index <= l.LastIndex():
		//需要删除index和之后的日志，再append，并修改storage还有stabled的值
		l.entries = l.entries[:index-l.dummyIndex-1]
		for i := range entries {
			l.entries = append(l.entries, *entries[i])
		}
		if index <= l.stabled {
			//index..stabled...
			//把[index,stabled]的日志也删掉了，需要更新stabled值
			l.stabled = index - 1
		}
	}

}

func (l *RaftLog) matchTerm(index uint64, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) findConflictIndex(entries []*pb.Entry) {

}

func (l *RaftLog) tryCommit(commit uint64, nowTerm uint64) bool {
	// 获取commit对应地term值
	term, err := l.Term(commit)
	if err != nil {
		return false
	}
	if commit > l.committed && term == nowTerm {
		l.commitTo(commit)
		return true
	}
	return false
}

func (l *RaftLog) commitTo(commit uint64) {
	if l.committed < commit {
		l.committed = min(l.LastIndex(), commit)
	}
}

func (l *RaftLog) commitedEntries() []pb.Entry {
	//返回[l.applied+1:l.commited+1)的日志
	return l.entries[l.applied-l.dummyIndex : l.committed-l.dummyIndex]
}
