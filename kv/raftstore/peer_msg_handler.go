package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer // 继承peer
	ctx   *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()
		//log.Debugf("%v hasReady:%+v", d.Tag, ready)
		//log.Debugf("%v before handle Ready applyState=%+v,softState=%+v", d.Tag, d.peerStorage.applyState, d.peerStorage.raftState)

		// 持久化log、元数据到磁盘
		applyResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			panic(err)
		}
		if applyResult != nil {
			log.Infof("%v apply snapshot success!!,applyResult=%v", d.Tag, applyResult)
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.setRegion(applyResult.Region, d.peer)
			if applyResult.PrevRegion.RegionEpoch.GetVersion() != 0 {
				d.ctx.storeMeta.regionRanges.Delete(&regionItem{applyResult.PrevRegion})
			}
			//d.ctx.storeMeta.Lock()
			//d.ctx.storeMeta.setRegion(applyResult.Region, d.peer)
			//d.ctx.storeMeta.regionRanges.Delete(&regionItem{applyResult.PrevRegion})
			d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{applyResult.Region})
			d.ctx.storeMeta.Unlock()
		}
		// 发送msg
		d.Send(d.ctx.trans, ready.Messages)

		// 处理commited entries
		//for _, entry := range ready.CommittedEntries {
		//	d.applyEntry(&entry)
		//}

		if len(ready.CommittedEntries) > 0 {
			t := time.Now()
			//kvWB := &engine_util.WriteBatch{}
			//for _, entry := range ready.CommittedEntries {
			//	d.applyEntry(&entry, kvWB)
			//	if d.stopped {
			//		// 可能apply了一条remove自己的命令，调用了destory()会删除该region的元信息,直接返回，不用继续执行下面了
			//		// 否则报错：
			//		// panic: [region 1] 2 unexpected raft log index: lastIndex 0 < appliedIndex 8
			//		return
			//	}
			//}
			//d.peerStorage.applyState.AppliedIndex = ready.CommittedEntries[len(ready.CommittedEntries)-1].Index
			//kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			//kvWB.WriteToDB(d.peerStorage.Engines.Kv)
			ch := make(chan bool, 1)
			applyTask := &AsyncApplyTask{
				D:                d,
				CommittedEntries: ready.CommittedEntries,
				Notifier:         ch,
			}
			d.ctx.applyTaskSender <- applyTask
			//<-ch

			duration := time.Since(t)
			log.Infof("%v apply %d entries %v in time %v", d.Tag, ready.CommittedEntries, len(ready.CommittedEntries), duration)
		}
		// 调用advance
		d.RaftGroup.Advance(ready)
		//log.Debugf("%v after handle Ready applyState=%+v,softState=%+v", d.Tag, d.peerStorage.applyState, d.peerStorage.raftState)
	} else {
		//log.Debugf("%d-%v has not ready", d.RaftGroup.Raft.GetId(), d.RaftGroup.Raft.State)

	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	//log.Debugf("1111")
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:

		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest == nil {
		// 在cluster.NewBaseRequest中可以看到每一个request都会自动包括一个header，不要在这里设置header。
		//msg.Header = &raft_cmdpb.RaftRequestHeader{RegionId: d.regionId, Peer: &metapb.Peer{Id: d.PeerId(), StoreId: d.storeID()}, RegionEpoch: d.Region().RegionEpoch}
		d.proposeRequest(msg, cb)
	} else {
		d.proposeAdminRequest(msg, cb)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	// 收到了来自已经删除peer的msg，通知该peer删除自身
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		//panic(d.Tag + " meta corruption detected")
		panic(d.Tag + " meta corruption detected:" + d.Region().String())
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		// 目前存储的日志大于RaftLogGcCountLimit就进行快照
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func (d *peerMsgHandler) applyEntry(e *eraftpb.Entry, writeBatch *engine_util.WriteBatch) {
	//writeBatch := &engine_util.WriteBatch{}
	var msg = &raft_cmdpb.RaftCmdRequest{}
	var header = &raft_cmdpb.RaftResponseHeader{}
	var responses = make([]*raft_cmdpb.Response, 0) //d.commitNormalRequests(msg)
	msg.Unmarshal(e.Data)
	switch e.EntryType {
	case eraftpb.EntryType_EntryNormal:
		// apply前检查regionEpoch
		if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
			//log.Debugf("%v response err %v for msg %v", d.Tag, err, msg)
			//log.Infof("%v response err %v for msg %v", d.Tag, err, msg)
			//d.handleResponse(e, ErrResp(err))
			d.handleResponse(e, ErrResp(&util.ErrEpochNotMatch{}))
			return
		}
		if len(msg.Requests) > 0 {
			// 先判断所有请求的key是否都在region里面
			region := d.Region()
			for _, request := range msg.Requests {
				var key []byte
				switch request.CmdType {
				case raft_cmdpb.CmdType_Get:
					key = request.Get.Key
				case raft_cmdpb.CmdType_Put:
					key = request.Put.Key
				case raft_cmdpb.CmdType_Delete:
					key = request.Delete.Key
				case raft_cmdpb.CmdType_Snap:
					// 没有key，无需进行下面的检查，否则scan命令总是返回错误导致request timeout
					continue
				}
				if err := util.CheckKeyInRegion(key, region); err != nil {
					d.handleResponse(e, ErrResp(err))
					return
				}
			}

			for _, request := range msg.Requests {
				switch request.CmdType {
				case raft_cmdpb.CmdType_Get:
					d.peerStorage.applyState.AppliedIndex = e.Index
					writeBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					writeBatch.WriteToDB(d.peerStorage.Engines.Kv)
					writeBatch.Reset()
					value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, request.Get.Cf, request.Get.Key)
					if err != nil {
						// key not exist
						value = nil
					}
					responses = append(responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Get,
						Get:     &raft_cmdpb.GetResponse{Value: value},
					})
				case raft_cmdpb.CmdType_Delete:
					writeBatch.DeleteCF(request.Delete.Cf, request.Delete.Key)
					responses = append(responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Delete,
						Delete:  &raft_cmdpb.DeleteResponse{},
					})

				case raft_cmdpb.CmdType_Put:
					//log.Infof("%v put key=%v,value=%v", d.Tag, string(request.Put.Key), string(request.Put.Value))
					writeBatch.SetCF(request.Put.Cf, request.Put.Key, request.Put.Value)
					responses = append(responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Put,
						Put:     &raft_cmdpb.PutResponse{},
					})

				case raft_cmdpb.CmdType_Snap:
					//log.Infof("%v snap key=%v,value=%v", d.Tag, string(request.Put.Key), string(request.Put.Value))
					d.peerStorage.applyState.AppliedIndex = e.Index
					writeBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					writeBatch.WriteToDB(d.peerStorage.Engines.Kv)
					writeBatch.Reset()

					region := &metapb.Region{
						Id:       d.Region().Id,
						StartKey: d.Region().StartKey,
						EndKey:   d.Region().EndKey,
						RegionEpoch: &metapb.RegionEpoch{
							ConfVer: d.Region().RegionEpoch.GetConfVer(),
							Version: d.Region().RegionEpoch.GetVersion(),
						},
						Peers: d.Region().Peers,
					}
					responses = append(responses, &raft_cmdpb.Response{
						CmdType: raft_cmdpb.CmdType_Snap,
						Snap: &raft_cmdpb.SnapResponse{
							Region: region,
						},
					})

				}
			}
		}
		if msg.AdminRequest != nil {
			switch msg.AdminRequest.CmdType {
			case raft_cmdpb.AdminCmdType_CompactLog:
				index, term := msg.AdminRequest.CompactLog.CompactIndex, msg.AdminRequest.CompactLog.CompactTerm
				if index >= d.peerStorage.applyState.TruncatedState.Index {
					d.peerStorage.applyState.TruncatedState.Index = index
					d.peerStorage.applyState.TruncatedState.Term = term
					writeBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
					d.ScheduleCompactLog(index) //启动一个异步任务进行磁盘上的日志删除工作
				}
			case raft_cmdpb.AdminCmdType_Split:
				//1.先比较splitKey与当前region的endKey
				region := new(metapb.Region)
				util.CloneMsg(d.Region(), region)

				exceedEndKey := engine_util.ExceedEndKey(msg.AdminRequest.Split.SplitKey, region.EndKey)
				if exceedEndKey {
					// splitKey不在当前region中

				} else {
					// 将Region 分裂成两个 Region 时，其中一个 Region 将继承分裂前的元数据，
					//1.修改其 Range 和 RegionEpoch，而另一个将创建相关的元信息。
					region.RegionEpoch.Version++
					log.Infof("%v want to apply the split %v, region=%v", d.Tag, msg.AdminRequest.Split, d.Region())

					newPeers := make([]*metapb.Peer, 0)
					//构建storeId->到newPeerIDs的映射,主要是按照当前storeID在旧region中的次序来分配新的peerId
					for i := range region.Peers {
						newPeers = append(newPeers, &metapb.Peer{Id: msg.AdminRequest.Split.NewPeerIds[i], StoreId: region.Peers[i].StoreId})
					}
					newRegion := &metapb.Region{
						Id:       msg.AdminRequest.Split.NewRegionId,
						StartKey: msg.AdminRequest.Split.SplitKey,
						EndKey:   region.EndKey,
						RegionEpoch: &metapb.RegionEpoch{
							ConfVer: region.RegionEpoch.GetConfVer(),
							Version: region.RegionEpoch.GetVersion(),
						},
						Peers: newPeers,
					}
					region.EndKey = msg.AdminRequest.Split.SplitKey

					//2.创建新的region：新创建的 Region 的对应 Peer 应该由 createPeer() 创建，并注册到 router.regions。
					p, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.peerStorage.Engines, newRegion)
					if err != nil {

					}
					d.ctx.router.register(p)
					d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})

					//3.先做region的持久化
					meta.WriteRegionState(writeBatch, region, rspb.PeerState_Normal)
					meta.WriteRegionState(writeBatch, newRegion, rspb.PeerState_Normal)

					//4.修改regionRanges，region 的信息应该插入 ctx.StoreMeta 中的regionRanges 中
					d.ctx.storeMeta.Lock()
					d.ctx.storeMeta.regionRanges.Delete(&regionItem{d.Region()})
					d.ctx.storeMeta.setRegion(region, d.peer)
					//d.ctx.storeMeta.regions[region.GetId()] = region
					d.ctx.storeMeta.regions[newRegion.GetId()] = newRegion
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
					d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
					d.ctx.storeMeta.Unlock()
					log.Infof("%v split new left region=%v,right region=%v", d.Tag, region, newRegion)
					//log.Debugf("%v split new left region=%v,right region=%v", d.Tag, region, newRegion)

					if d.IsLeader() { // try to fix find no region
						d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
						p.HeartbeatScheduler(d.ctx.schedulerTaskSender)
					}
				}
			}

		}
	case eraftpb.EntryType_EntryConfChange:
		// conf change
		cc := &eraftpb.ConfChange{}
		region := new(metapb.Region)
		util.CloneMsg(d.Region(), region)
		msg := &raft_cmdpb.RaftCmdRequest{}

		cc.Unmarshal(e.Data)
		msg.Unmarshal(cc.Context)
		//检查regionEpoch信息
		if err := util.CheckRegionEpoch(msg, region, true); err != nil {
			d.handleResponse(e, ErrResp(err))
			return
		}
		log.Infof("%v apply the config change %v", d.Tag, cc)

		// 1、修改storage中的region信息并作持久化
		switch cc.ChangeType {
		case eraftpb.ConfChangeType_AddNode:
			for _, p := range region.Peers {
				if p.Id == cc.NodeId {
					// 已经有了要添加的peer
					return
				}
			}
			// 添加新的peer
			region.Peers = append(region.Peers, msg.AdminRequest.ChangePeer.Peer)
			d.insertPeerCache(msg.AdminRequest.ChangePeer.Peer) // peerCache用户获取该peer的storeId
		case eraftpb.ConfChangeType_RemoveNode:
			if cc.NodeId == d.peer.PeerId() {
				// 销毁自己
				d.destroyPeer()
				return
			}
			index := -1
			for i, p := range region.Peers {
				if p.Id == cc.NodeId {
					index = i
					region.Peers = append(region.Peers[:index], region.Peers[index+1:]...)
					break
				}
			}
			if index == -1 {
				// 找不到要删除的peer
				return
			}
			d.removePeerCache(cc.NodeId)
		}
		region.RegionEpoch.ConfVer++
		meta.WriteRegionState(writeBatch, region, rspb.PeerState_Normal)
		//writeBatch.WriteToDB(d.peerStorage.Engines.Kv)
		//writeBatch.Reset()
		// 2.修改元数据，让storeWorker在后续通过心跳机制创建peer
		d.ctx.storeMeta.Lock()
		//d.SetRegion(region)
		d.ctx.storeMeta.setRegion(region, d.peer)
		//d.ctx.storeMeta.regions[region.Id] = region
		d.ctx.storeMeta.Unlock()

		// 3、修改raft中的Prs信息
		d.RaftGroup.ApplyConfChange(*cc)
	}

	//writeBatch.WriteToDB(d.peerStorage.Engines.Kv)
	//writeBatch.Reset()
	d.handleResponse(e, &raft_cmdpb.RaftCmdResponse{Header: header, Responses: responses})
}

func (d *peerMsgHandler) handleResponse(e *eraftpb.Entry, r *raft_cmdpb.RaftCmdResponse) {
	// find the proposal
	eindex, eterm := e.Index, e.Term
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		pterm, pindex := p.term, p.index
		if pterm < eterm {
			//stale proposal
			log.Debugf("%v response proposal %v with response %v", d.Tag, p, ErrRespStaleCommand(pterm))
			//log.Infof("%v response proposal %v with response %v", d.Tag, p, ErrRespStaleCommand(pterm))
			p.cb.Done(ErrRespStaleCommand(pterm))
			d.proposals = d.proposals[1:]
		} else if pterm == eterm {
			if pindex < eindex {
				//stale proposal
				log.Debugf("%v response proposal %v with response %v", d.Tag, p, ErrRespStaleCommand(pterm))
				//log.Infof("%v response proposal %v with response %v", d.Tag, p, ErrRespStaleCommand(pterm))
				p.cb.Done(ErrRespStaleCommand(pterm))
				d.proposals = d.proposals[1:]
			} else if pindex == eindex {
				// find the proposal, done and return
				if len(r.Responses) > 0 && r.Responses[0].CmdType == raft_cmdpb.CmdType_Snap {
					p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				}
				log.Debugf("%v response proposal %v with response %v", d.Tag, p, r)
				//log.Infof("%v response proposal %v with response %v", d.Tag, p, r)
				p.cb.Done(r)
				d.proposals = d.proposals[1:]
				return
			} else if pindex > eindex {
				return
			}
		} else if pterm > eterm {
			return
		}
	}
}

func (d *peerMsgHandler) proposeRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	region := d.Region()
	for _, request := range msg.Requests {
		var key []byte
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = request.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = request.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = request.Delete.Key
		case raft_cmdpb.CmdType_Snap:
			// 没有key，无需进行下面的检查，否则scan命令总是返回错误导致request timeout
			continue
		}
		if err := util.CheckKeyInRegion(key, region); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	proposal := &proposal{
		term:  d.Term(),
		index: d.nextProposalIndex(),
		cb:    cb,
	}
	//添加新的proposal，每个proposal包含了callback
	d.proposals = append(d.proposals, proposal)
	//编码msg，调用rawNode.Propose写入raft
	log.Debugf("%v propose a Request: %v, proposal=%v", d.Tag, msg.Requests, proposal)
	//log.Infof("%v propose a Request: %v, proposal=%v", d.Tag, msg, proposal)
	//log.Infof("%v propose a Request: %v, proposal=%v", d.Tag, msg, proposal)
	//if msg.Requests[0].CmdType == raft_cmdpb.CmdType_Snap {
	//	log.Debugf("%v propose a Request: %v, proposal=%v,snapARgs=%v", d.Tag, msg, proposal, msg.Requests[0].Snap.XXX_unrecognized)
	//}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	d.RaftGroup.Propose(data)
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	region := d.Region()
	switch msg.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		//log.Infof("%v propose a Request: %v", d.Tag, msg)
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// no need to replicate to other peers, just need to call the TransferLeader()
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
		cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			},
		})
	case raft_cmdpb.AdminCmdType_ChangePeer:
		// 通过 ProposeConfChange 提出 conf change admin
		if msg.AdminRequest.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode &&
			len(d.Region().Peers) == 2 && d.IsLeader() && msg.AdminRequest.ChangePeer.Peer.Id == d.PeerId() {
			//corner case：只剩两个节点的时候删除自身
			//log.Infof("Corner case: will transfer:%v want propose a change peer Request: %v, peers=%v", d.Tag, msg.AdminRequest.ChangePeer, d.Region().Peers)
			if d.Region().Peers[0].Id == d.PeerId() {
				d.RaftGroup.TransferLeader(d.Region().Peers[1].Id)
			} else {
				d.RaftGroup.TransferLeader(d.Region().Peers[0].Id)
			}
			return
		}
		data, _ := msg.Marshal()
		proposal := &proposal{
			term:  d.Term(),
			index: d.nextProposalIndex(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, proposal)
		d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    data,
		})
		//log.Debugf("%v propose a change peer Request: %v, proposal=%v", d.Tag, msg.AdminRequest.ChangePeer, proposal)
		log.Infof("%v propose a change peer Request: %v, proposal=%v, peers=%v", d.Tag, msg.AdminRequest.ChangePeer, proposal, d.Region().Peers)

	case raft_cmdpb.AdminCmdType_Split:
		// 1.判断splitKey和regionEpoch是否正确
		if err := util.CheckKeyInRegion(msg.AdminRequest.Split.SplitKey, region); err != nil {
			log.Infof("%v will not propose the split Request: %v, err=%v", d.Tag, msg.AdminRequest.Split, err)
			cb.Done(ErrResp(err))
		}
		if err := util.CheckRegionEpoch(msg, region, true); err != nil {
			log.Infof("%v will not propose the split Request: %v, err=%v", d.Tag, msg.AdminRequest.Split, err)
			cb.Done(ErrResp(err))
		}
		if len(d.Region().Peers) != len(msg.AdminRequest.Split.NewPeerIds) {
			log.Infof("%v will not propose the split Request: %v, err=peers长度不匹配,len(peers)=%d", d.Tag, msg.AdminRequest.Split, len(d.Region().Peers))
			cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
		}
		data, _ := msg.Marshal()
		proposal := &proposal{
			term:  d.Term(),
			index: d.nextProposalIndex(),
			cb:    cb,
		}
		d.proposals = append(d.proposals, proposal)
		d.RaftGroup.Propose(data)
		//log.Debugf("%v propose a split Request: %v, proposal=%v", d.Tag, msg.AdminRequest.Split, proposal)
		log.Infof("%v propose a split Request: %v, proposal=%v", d.Tag, msg.AdminRequest.Split, proposal)
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
