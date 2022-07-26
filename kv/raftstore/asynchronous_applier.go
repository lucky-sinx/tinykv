package raftstore

import (
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type AsyncApplyTask struct {
	D                *peerMsgHandler
	CommittedEntries []eraftpb.Entry
	Notifier         chan<- bool
}

type asyncApplyTaskHandler struct {
	d *peerMsgHandler
}

func NewAsyncApplyTaskHandler() *asyncApplyTaskHandler {
	return &asyncApplyTaskHandler{}
}

func (a *asyncApplyTaskHandler) Handle(t worker.Task) {
	task := t.(*AsyncApplyTask)
	kvWB := &engine_util.WriteBatch{}

	for _, entry := range task.CommittedEntries {
		task.D.applyEntry(&entry, kvWB)
		if task.D.stopped {
			// 可能apply了一条remove自己的命令，调用了destory()会删除该region的元信息,直接返回，不用继续执行下面了
			// 否则报错：
			// panic: [region 1] 2 unexpected raft log index: lastIndex 0 < appliedIndex 8
			task.Notifier <- true
			return
		}
	}
	task.D.peerStorage.applyState.AppliedIndex = task.CommittedEntries[len(task.CommittedEntries)-1].Index
	kvWB.SetMeta(meta.ApplyStateKey(task.D.regionId), task.D.peerStorage.applyState)
	kvWB.WriteToDB(task.D.peerStorage.Engines.Kv)
	task.Notifier <- true
}
