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
	"fmt"
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	log := &RaftLog{
		storage:         storage,
		pendingSnapshot: nil,
	}
	// storage中的是持久化了的数据
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	log.stabled = lastIndex

	// 将所有storage中的数据读出
	log.entries, err = storage.Entries(firstIndex, lastIndex+1)
	if log.entries == nil || err != nil {
		log.entries = make([]pb.Entry, 0)
	}
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	// 从firstIndex开始
	firstIndex, _ := l.storage.FirstIndex()
	if len(l.entries) != 0 && firstIndex > l.entries[0].Index {
		l.entries = l.entries[firstIndex-l.entries[0].Index:]
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.stabled+1-l.entries[0].Index:]
}

// check l.firstIndex <= left <= right <= l.LastIndex
// 在所有log中找
func (l *RaftLog) checkOutOfBoundsInAll(left, right uint64) error {
	fi := l.FirstIndex()
	if left < l.FirstIndex() {
		return ErrCompacted
	}
	li := l.LastIndex()
	if right > li {
		panic(fmt.Sprintf("All.slice[%d,%d] out of bound [%d,%d]", left, right, fi, li))
	}
	return nil
}

// 在该RaftLog中找
func (l *RaftLog) checkOutOfBoundsInMemory(left, right uint64) error {
	if len := len(l.entries); len == 0 || left < l.entries[0].Index || right > l.entries[len-1].Index {
		panic(fmt.Sprintf("Memory.slice[%d,%d] out of bound", left, right))
	}
	return nil
}

// 获得所有存储上[left,right]之间的所有log
func (l *RaftLog) sliceInAll(left, right uint64) ([]pb.Entry, error) {
	if left > right {
		return nil, nil
	}
	err := l.checkOutOfBoundsInAll(left, right)
	if err != nil {
		return nil, err
	}

	var ents []pb.Entry
	// 先在内存中取数据
	flag := false
	if len(l.entries) != 0 && right >= l.entries[0].Index {
		flag = true
		getFromL := max(left, l.entries[0].Index)
		err := l.checkOutOfBoundsInMemory(getFromL, right)
		if err != nil {
			panic(err)
		}
		unstable := l.entries[getFromL-l.entries[0].Index : right-l.entries[0].Index+1]
		ents = unstable
	}

	// 在storage中取数据
	var tmpR uint64
	if !flag {
		tmpR = right
	} else {
		tmpR = l.entries[0].Index - 1
	}

	// 比如entries为空，所有log都compact了left刚好等于truncIndex+1，此时不能去拿数据
	// 实际上后来改成了所有没compact的Log都在内存中，这里没有用到
	if len(l.entries) == 0 || left < l.entries[0].Index {
		storedEnts, err := l.storage.Entries(left, tmpR+1)
		if err == ErrCompacted {
			log.Warn(err)
			return nil, nil
		} else if err == ErrUnavailable {
			//panic(fmt.Sprintf("entries[%d:%d] is unavailable from storage", left, min(right, l.stabled)+1))
			log.Warn(err)
			return nil, nil
		} else if err != nil {
			//panic(err)
			log.Warn(err)
			return nil, nil
		}

		if len(ents) > 0 {
			combined := make([]pb.Entry, len(storedEnts)+len(ents))
			n := copy(combined, storedEnts)
			copy(combined[n:], ents)
			ents = combined
		} else {
			ents = storedEnts
		}
	}

	return ents, nil
}

// 获得自身entries上[left,right]之间的所有log
func (l *RaftLog) sliceInMemory(left, right uint64) []pb.Entry {
	return l.entries[left-l.entries[0].Index : right-l.entries[0].Index+1]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := max(l.applied+1, l.FirstIndex())
	if l.committed >= off {
		ents, err := l.sliceInAll(off, l.committed)
		if err != nil {
			panic(fmt.Sprintf("unexpected error when getting unapplied entries (%v)", err))
		}
		return ents
	}
	return nil
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

// LastIndex return the last index of the log entries
//
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// 有未持久化的数据
	if len := len(l.entries); len != 0 {
		//错误
		//stable表示持久化到了哪里，stable+len便是最后一个数据对应的index
		//之后注意下snapshot的逻辑，貌似snapshot后stable也更新了
		//return l.stabled + uint64(len)

		////原方法错误，测试中持久化后不调用advance，也就是l.entries和storage中的同时存在
		return l.entries[len-1].Index
	}
	// 有可能所有entries的数据都snapshot了
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i < l.FirstIndex()-1 || i > l.LastIndex() {
		return 0, nil
	}
	// i不在内存entries中
	if len(l.entries) == 0 || i < l.entries[0].Index {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		} else {
			t, err := l.storage.Term(i)
			if err == nil {
				return t, nil
			}
			if err == ErrCompacted || err == ErrUnavailable {
				return 0, err
			}
			panic(err)
		}
	}
	// 在内存中找数据
	if len := len(l.entries); len != 0 {
		return l.entries[i-l.entries[0].Index].Term, nil
	}
	panic("please check the function")
}
func (l *RaftLog) getTerm(i uint64) uint64 {
	res, _ := l.Term(i)
	return res
}

// 是否有数据要apply
func (l *RaftLog) hasNextEnts() bool {
	// entry has snapshot don't apply
	off := max(l.applied+1, l.FirstIndex())
	return l.committed >= off
}

//将ents append到log之后，如果ents[0].index位置有冲突，
//应当将自身的entries从该位置截断，完整append ents
func (l *RaftLog) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	if len(l.entries) == 0 || after < l.entries[0].Index {
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		l.stabled = after - 1
		l.entries = ents
	} else if after == l.entries[len(l.entries)-1].Index+1 {
		// after is the next index in the u.entries
		// directly append
		l.entries = append(l.entries, ents...)
	} else {
		// truncate to after and copy to u.entries
		// then append
		//DPrintf("truncate the unstable entries before index %d", after)
		l.entries = append([]pb.Entry{}, l.sliceInMemory(l.entries[0].Index, after-1)...)
		l.stabled = min(l.stabled, after-1)
		l.entries = append(l.entries, ents...)
	}
}

func (l *RaftLog) hasPendingSnapshot() bool {
	// 暂时未使用
	return !IsEmptySnap(l.pendingSnapshot)
	//return false
	//return l.unstable.snapshot != nil && !IsEmptySnap(*l.unstable.snapshot)
}
