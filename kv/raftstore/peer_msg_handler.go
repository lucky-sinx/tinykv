package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger"
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
	// 继承自Peer
	*peer
	ctx *GlobalContext
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
	//engine_util.DPrintf("%v-HandleRaftReady-Router-%p", d.Meta.StoreId, d.ctx.router)
	// Your Code Here (2B).
	//1. 首先判断当前是否有Ready，有则更新信息，否则继续。
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		//2. 通过PeerStorage中的SaveReadyState()函数来保存ready中的信息，
		//包括Append()添加日志项和应用快照等。
		applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		if applySnapResult != nil {
			// 修改storeMeta需要加锁，如maybeCreatePeer()中也修改了
			d.ctx.storeMeta.Lock()
			d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
			d.ctx.storeMeta.Unlock()
		}
		//3. 发送ready中的消息。然后将已经提交的日志项应用到状态机。
		//	首先从entries中取出日志项，从Proposals中按顺序寻找对应的callback。这里需要注意，如果存在之前index的proposal,可以进行回收处理。
		//	依次执行Requests请求中的多个请求，构造Response统一放在RaftCmdResponse中。
		//	更新ApplyState,写入KVDB。
		if len(rd.Messages) != 0 {
			// 通过ServerTransport将Ready中的Msg发送给其他节点
			d.Send(d.ctx.trans, rd.Messages)
		}
		if len(rd.CommittedEntries) != 0 {
			engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- -%v", d.Meta.StoreId, d.PeerId(), d.regionId, rd.CommittedEntries)
			// 成功apply的entry
			//successCommitEntries := make([]eraftpb.Entry, 0)
			for _, entry := range rd.CommittedEntries {
				kvWB := new(engine_util.WriteBatch)
				if entry.EntryType == eraftpb.EntryType_EntryConfChange {
					// confChange
					d.handleConfigChange(&entry, kvWB)
					//if configChangeState != 1 {
					//	// 两节点，是Leader并且是RemoveNode自身的Log，则不提交该Log及之后的Log
					//	// Transfer Leader
					//	if d.IsLeader() && configChangeState == 2 {
					//		var toId uint64 = d.PeerId()
					//		for _, peer := range d.Region().Peers {
					//			if peer.Id != d.PeerId() {
					//				toId = peer.Id
					//				break
					//			}
					//		}
					//		if toId == d.PeerId() {
					//			panic("check the two peers transfer Leader")
					//		}
					//		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- TransferBeforeDestroy to-%v", d.Meta.StoreId, d.PeerId(), d.regionId, toId)
					//
					//		d.RaftGroup.TransferLeader(toId)
					//
					//	}
					//	break
					//}
					//else if configChangeState == 3 {
					//	// 两节点，是Follow并且是RemoveNode Leader的Log，则不提交该Log及之后的Log
					//	// 等待Leader Transfer成功，自身成为Leader
					//	break
					//}
				} else {
					d.process(&entry, kvWB)
				}

				//successCommitEntries = append(successCommitEntries, rd.CommittedEntries[i])

				// Bug：removeNode后applyState已经清空，重新赋值会出问题
				if d.stopped {
					return
				}
				d.peerStorage.applyState.AppliedIndex = entry.Index
				err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
				if err != nil {
					panic(err)
				}
				err = d.peerStorage.Engines.WriteKV(kvWB)
				if err != nil {
					panic(err)
				}
			}
			//rd.CommittedEntries = successCommitEntries
			//commitIndex := len(rd.CommittedEntries) - 1
			//if commitIndex >= 0 {
			//	engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- ApplyIndex-%v,lastApplyEntry-%v", d.Meta.StoreId, d.PeerId(), d.regionId, rd.CommittedEntries[commitIndex].Index, rd.CommittedEntries[commitIndex])
			//}
		}

		//4. 最后通过Advance()函数来强制更新raft层的状态。
		d.RaftGroup.Advance(rd)
	}
}

// 将entry apply
func (d *peerMsgHandler) process(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) {
	msg := new(raft_cmdpb.RaftCmdRequest)
	err := msg.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	if msg.AdminRequest != nil {
		// no callback
		if d.checkEpochNotMatch(nil, msg, false) {
			return
		}

		request := msg.AdminRequest
		if request.CmdType == raft_cmdpb.AdminCmdType_CompactLog {
			d.handleCompactLog(request)
		} else if request.CmdType == raft_cmdpb.AdminCmdType_Split {
			d.handleSplit(request, kvWB)
		}
		return
	}
	if len(msg.Requests) != 0 {
		if d.checkEpochNotMatch(entry, msg, true) {
			return
		}

		// 仅Write相关操作是batch的
		// 和824不同，貌似没有记录重复消息的地方
		request := msg.Requests[0]

		// 因为可能存在split，当请求错误的region时应该返回错误
		if err = d.checkKeyNotInRegion(request); err != nil {
			d.callbackPropose(entry, ErrResp(err), nil)
			return
		}
		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Apply[Normal]Command(index,term,request) -- entry-[%v,%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, entry.Index, entry.Term, msg.Requests)

		// apply所有外部请求
		for _, request := range msg.Requests {
			switch request.CmdType {
			case raft_cmdpb.CmdType_Get:
			case raft_cmdpb.CmdType_Put:
				put := request.Put
				kvWB.SetCF(put.Cf, put.Key, put.Value)
			case raft_cmdpb.CmdType_Delete:
				del := request.Delete
				kvWB.DeleteCF(del.Cf, del.Key)
			case raft_cmdpb.CmdType_Snap:
			}
		}

		// 返回结果
		if len(d.proposals) != 0 {
			header := &raft_cmdpb.RaftResponseHeader{}
			response := &raft_cmdpb.RaftCmdResponse{
				Header: header,
			}

			switch request.CmdType {
			case raft_cmdpb.CmdType_Get:
				get := request.Get
				val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, get.Cf, get.Key)
				response.Responses = []*raft_cmdpb.Response{{
					CmdType: raft_cmdpb.CmdType_Get,
					Get:     &raft_cmdpb.GetResponse{Value: val},
				}}

			case raft_cmdpb.CmdType_Put:
				response.Responses = []*raft_cmdpb.Response{{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				}}

			case raft_cmdpb.CmdType_Delete:
				response.Responses = []*raft_cmdpb.Response{{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				}}
			case raft_cmdpb.CmdType_Snap:
				// 后面好像region可能会发生改变
				//d.Region是一个指针，后面可能修改它的值，所以应该重新new一个Region复制过去

				responseRegion := new(metapb.Region)
				data, err := d.Region().Marshal()
				if err != nil {
					panic(err)
				}
				err = responseRegion.Unmarshal(data)
				if err != nil {
					panic(err)
				}
				//engine_util.DPrintf("SNAP,region-%v", responseRegion)
				response.Responses = []*raft_cmdpb.Response{{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: responseRegion},
				}}
				d.callbackPropose(entry, response, d.peerStorage.Engines.Kv.NewTransaction(false))
				return
			}
			d.callbackPropose(entry, response, nil)
		}
	}
}

// 判断peerId是否已经在Region中存在
func (d *peerMsgHandler) checkInPeers(peerId uint64) bool {
	for _, peer := range d.Region().GetPeers() {
		if peer.Id == peerId {
			return true
		}
	}
	return false
}

// 修改config，addNode,removeNode
// （旧的想法）三种返回值，成功configChange返回1，两节点Leader删除自身失败需要transfer返回2，两节点Follow删除Leader失败需要等到Leader Transfer成功返回3
func (d *peerMsgHandler) handleConfigChange(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) int {
	cc := new(eraftpb.ConfChange)
	err := cc.Unmarshal(entry.Data)
	if err != nil {
		panic(err)
	}
	cmpRequest := new(raft_cmdpb.RaftCmdRequest)
	err = cmpRequest.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	peerContext := cmpRequest.AdminRequest.ChangePeer.Peer
	if d.checkEpochNotMatch(nil, cmpRequest, false) {
		//engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- ConfigChange-EpochNotMatch(stoneId,peerID)--epoch-%v", d.Meta.StoreId, d.PeerId(), d.regionId, peerContext.StoreId, peerContext.Id, d.Region().RegionEpoch)
		return 1
	}

	if cc.ChangeType == eraftpb.ConfChangeType_AddNode {
		// 新增节点
		if d.checkInPeers(peerContext.Id) {
			return 1
		}
		// 修改该节点region中的peers
		newPeer := &metapb.Peer{
			Id:      peerContext.Id,
			StoreId: peerContext.StoreId,
		}
		d.Region().Peers = append(d.Region().Peers, newPeer)
		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- AddNode(stoneId,peerID)--[%v,%v],confver-%v", d.Meta.StoreId, d.PeerId(), d.regionId, peerContext.StoreId, peerContext.Id, d.Region().RegionEpoch.ConfVer)

		d.insertPeerCache(newPeer)
	} else {
		// 删除节点
		if !d.checkInPeers(peerContext.Id) {
			return 1
		}
		// 无论是否是删除自身，在两节点的情况下都需要延时删除，保证两节点都删了其中一个
		// 还是不行，改成propose前必须transfer了
		//if len(d.Region().Peers) == 2 {
		//	d.WantDeleteLeaderTimes++
		//	if d.WantDeleteLeaderTimes <= 100 {
		//		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v]--WantDeleteLeaderTimes-%v", d.Meta.StoreId, d.PeerId(), d.regionId, d.WantDeleteLeaderTimes)
		//		//删除自身
		//		if peerContext.GetId() == d.PeerId() {
		//			return 2
		//		} else {
		//			return 3
		//		}
		//	}
		//}
		//d.WantDeleteLeaderTimes = 0

		//删除自身
		if peerContext.GetId() == d.PeerId() {
			// 两个节点并且当前是Leader的时候，不能Apply remove自身，否则可能commitIndex没有同步出去
			// 另一个节点超时选举时并不知道removeNode Log已经commit了，会向该节点请求Vote，
			// 而当前节点不存在会导致一直请求失败，Leader选举不出来，集群不可用
			//bug想法1:只增加这一个判断是不够的，因为当Follow Region正常删除Leader时，Leader还在Transfer没能删除自己
			//这时Transfer相关的TimeOut消息会成为stale消息被Follow拒收，导致Leader一直在Transfer状态，集群不可用

			//还有个问题，Follow需要更新commit，但因为它超时直接成了candidate，Term变得比Leader大了，这时candidate怎么也不可能更新commit，也就是remove node那条日志不会提交，leader的region一直删不掉，新leader也就选不出来
			//最后还是弄简单点，Leader多Transfer几次再变更吧。。。。
			//d.WantDeleteLeaderTimes++
			// 后去除d.IsLeader()的判断，因为当Leader给Candidate的投票可能需要后面重发，不能直接删除
			//if len(d.Region().Peers) == 2 && d.WantDeleteLeaderTimes <= 100 {
			//	engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v]--WantDeleteLeaderTimes-%v", d.Meta.StoreId, d.PeerId(), d.regionId, d.WantDeleteLeaderTimes)
			//	return 2
			//}
			engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- RemoveNode[DestroyNode](stoneId,peerID)--[%v,%v],confVersion-%v", d.Meta.StoreId, d.PeerId(), d.regionId, peerContext.StoreId, peerContext.Id, d.Region().RegionEpoch.ConfVer)
			d.destroyPeer()
			return 1
		}

		// bug想法2：当集群只有两节点时，如果不是删除自身，必须等到自己是Leader，也就是说原Leader要Transfer成功了，才能删除
		// bug想法3：但这样还是不够的，Flower向Leader请求Vote，Leader投票后就把自己删了，无法保证Flower成功成为Leader然后删除原Leader
		// 因为两节点间的通信是不稳定的，没有办法完全保证他们同时知道要删除某个节点（两军问题），无解，只能设定一个次数，当Follow一直没能成为Leader，直接删除原Leader自己晋升为Leader
		// 这里记录的是Follow 尝试commit的次数，但Follow不一定更新了commit，因此修改Timeout消息，这里的方法保证了Timeout一定成功接收
		//if len(d.Region().Peers) == 2 && !d.IsLeader() {
		//	d.WantDeleteLeaderTimes++
		//	engine_util.DPrintf("WantDeleteLeaderTimes-%v", d.WantDeleteLeaderTimes)
		//	// 试出来的值，理论上来说每次tick都会导致+1，但Candidate重新当选发送VoteRequest需要很多10-20个Tick
		//	if d.WantDeleteLeaderTimes <= 5 {
		//		return 3
		//	}
		//}
		//d.WantDeleteLeaderTimes = 0

		// 修改该节点region中的peers
		index := -1
		for i, peer := range d.Region().Peers {
			if peer.GetId() == peerContext.GetId() {
				index = i
				break
			}
		}
		if index == -1 {
			return 1
		}
		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- RemoveNode(stoneId,peerID)--[%v,%v],confver-%v", d.Meta.StoreId, d.PeerId(), d.regionId, d.Region().Peers[index].StoreId, d.Region().Peers[index].GetId(), d.Region().RegionEpoch.ConfVer)
		d.Region().Peers = append(d.Region().Peers[:index], d.Region().Peers[index+1:]...)

		d.removePeerCache(peerContext.GetId())
	}
	//修改Raft prs
	d.RaftGroup.ApplyConfChange(*cc)

	d.Region().RegionEpoch.ConfVer++

	kvWB.SetMeta(meta.RegionStateKey(d.Region().Id), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.Region(),
	})
	// 修改元数据
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
	d.ctx.storeMeta.Unlock()
	return 1
}

// 处理CompactLog Log
func (d *peerMsgHandler) handleCompactLog(request *raft_cmdpb.AdminRequest) {
	//CompactLogRequest modifies metadata, namely updates the RaftTruncatedState which is in the RaftApplyState.
	//After that, you should schedule a task to raftlog-gc worker by ScheduleCompactLog. Raftlog-gc worker will do the actual log deletion work asynchronously.
	// 可能snapshot了，后来apply compact导致truncatedState改变，出现错误
	if request.CompactLog.CompactIndex <= d.peerStorage.applyState.TruncatedState.Index {
		return
	}
	//修改ApplyState.RaftTruncatedState
	d.peerStorage.applyState.TruncatedState.Index = request.CompactLog.CompactIndex
	d.peerStorage.applyState.TruncatedState.Term = request.CompactLog.CompactTerm
	//异步删除Log,实际删除log的操作在raftLogGcTaskHandle中做
	d.ScheduleCompactLog(request.CompactLog.CompactIndex)
	engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Apply[Compact]Command(compactIndex,compactTerm) -- entry-[%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, request.CompactLog.CompactIndex, request.CompactLog.CompactTerm)
}

// 处理split Log
func (d *peerMsgHandler) handleSplit(request *raft_cmdpb.AdminRequest, kvWB *engine_util.WriteBatch) {
	split := request.Split
	// 有重复split，会导致startKey > endKey
	err := util.CheckKeyInRegion(split.SplitKey, d.Region())
	if err != nil {
		return
	}
	// 新建region，注意要将region.peers排序后分配peerId，不然可能不同peer上新建时newPeerId对应分裂的peer不同
	for i, _ := range d.Region().Peers {
		for j := i + 1; j < len(d.Region().Peers); j++ {
			if d.Region().Peers[j].Id < d.Region().Peers[i].Id {
				tmp := d.Region().Peers[i]
				d.Region().Peers[i] = d.Region().Peers[j]
				d.Region().Peers[j] = tmp
			}
		}
	}
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: split.SplitKey,
		EndKey:   d.Region().EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: make([]*metapb.Peer, 0),
	}
	for i, peer := range d.Region().Peers {
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peer.StoreId})
	}
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		panic(err)
	}
	engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Apply[Split]Command -- splitKey-%v,startKey-%v,endKey-%v,epoch-%v,newRegionId-%v,oldPeers-%v,newPeers-%v", d.Meta.StoreId, d.PeerId(), d.regionId, split.SplitKey, d.Region().StartKey, d.Region().EndKey, d.Region().RegionEpoch, split.NewRegionId, d.Region().Peers, split.NewPeerIds)

	d.Region().EndKey = split.SplitKey
	d.Region().RegionEpoch.Version++

	_ = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})

	// 修改元数据
	d.ctx.storeMeta.Lock()
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
	d.ctx.storeMeta.regions[newRegion.Id] = newRegion
	d.ctx.storeMeta.regions[d.Region().Id] = d.Region()
	d.ctx.storeMeta.Unlock()

	kvWB.SetMeta(meta.RegionStateKey(d.Region().Id), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: d.Region(),
	})
	kvWB.SetMeta(meta.RegionStateKey(newRegion.Id), &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: newRegion,
	})

	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newRegion.Id, message.Msg{RegionID: newRegion.Id, Type: message.MsgTypeStart})

}

// 检查key是否在region范围中
func (d *peerMsgHandler) checkKeyNotInRegion(request *raft_cmdpb.Request) error {
	// 因为可能存在split，当请求错误的region时应该返回错误
	var key []byte
	switch request.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = request.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = request.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = request.Delete.Key
	}
	if request.CmdType != raft_cmdpb.CmdType_Snap {
		// ErrKeyNotInRegion
		err := util.CheckKeyInRegion(key, d.Region())
		return err
	}
	return nil
}

// 检查msg的RegionEpoch是否和当前RegionEpoch相匹配
func (d *peerMsgHandler) checkEpochNotMatch(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, shouldCallback bool) bool {
	// 检查msg版本，同preProposeRaftCommand
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		if shouldCallback {
			d.callbackPropose(entry, ErrResp(errEpochNotMatching), nil)
		}
		return true
	}
	return false
}
func (d *peerMsgHandler) callbackPropose(entry *eraftpb.Entry, response *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	// call back
	if len(d.proposals) != 0 {
		// p.proposal是按照index顺序插入的
		var pro *proposal
		// 找到index和term对应的proposal
		// 一开始以为proposals中的Index是递增的，但实际上可能Log被截断后新propose的Log Index变小了。（不知道为什么也能过测试）
		// 但由于被截断Term肯定变大，因此Term是递增的

		// 删除所有过时的proposal
		for pro = d.proposals[0]; pro.term < entry.Term; pro = d.proposals[0] {
			//ErrStaleCommand: It may due to leader changes that some logs are not committed and overrided with new leaders’ logs.
			//But the client doesn’t know that and is still waiting for the response.
			//So you should return this to let the client knows and retries the command again.
			// Term不匹配
			pro.cb.Done(ErrRespStaleCommand(entry.Term))
			engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- ErrStale[Normal]Command-TermUnMatch(index,term) -- old-[%v,%v],entry-[%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, pro.index, pro.term, entry.Index, entry.Term)
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				return
			}
		}
		// 同Term的Index肯定递增
		for ; pro.index < entry.Index && pro.term == entry.Term; pro = d.proposals[0] {
			// index不匹配
			pro.cb.Done(ErrResp(&util.ErrStaleCommand{}))
			engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- ErrStale[Normal]Command-IndexLittle(index,term) -- old-[%v,%v],entry-[%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, pro.index, pro.term, entry.Index, entry.Term)
			d.proposals = d.proposals[1:]
			if len(d.proposals) == 0 {
				return
			}
		}
		if pro.term > entry.Term || pro.index > entry.Index {
			// 当前entry没有对应proposal
			return
		}
		if txn != nil {
			pro.cb.Txn = txn
		}
		pro.cb.Done(response)
		d.proposals = d.proposals[1:]
	}
}
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	// 处理Raft Message，即其他节点发送过来的Raft内部消息
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	// 处理外部发送过来的消息，即Write()和Reader()
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

// 对于RaftCmdRequest,进入HandleMsg之后，会调用proposeRaftCommand函数来进行Propose
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	//if d.stopped
	// 只有Leader才能够进行写入，需要对其进行检查
	// 对请求进行解析,恢复到原来的normal request or admin request
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// Normal request
	if msg.AdminRequest == nil {
		// 对于外部请求，记录它所在的位置
		// 检查key是否在region范围中
		if err = d.checkKeyNotInRegion(msg.Requests[0]); err != nil {
			cb.Done(ErrResp(err))
			return
		}
		// 使用Proposal来保留[index, term, callback]，callback通过channel便于在后面提交的时候进行process
		d.proposals = append(d.proposals, &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		})
		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Propose[Normal]Command(index,term,request) -- entry-[%v,%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, d.nextProposalIndex(), d.Term(), msg.Requests)
	} else {
		// 处理AdminRequest，包括四种：CompactLog、TransferLeader、ChangePeer、Split
		request := msg.AdminRequest
		response := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
		}

		if request.CmdType == raft_cmdpb.AdminCmdType_CompactLog {
			// compact
			engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Propose[Compact]Command(compactIndex,compactTerm) -- entry-[%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, request.CompactLog.CompactIndex, request.CompactLog.CompactTerm)
		} else if request.CmdType == raft_cmdpb.AdminCmdType_TransferLeader {
			// Transfer Leader
			d.RaftGroup.TransferLeader(request.TransferLeader.Peer.Id)
			response.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(response)
			return
		} else if request.CmdType == raft_cmdpb.AdminCmdType_ChangePeer {
			// change config
			// 玄学，在propose前transfer，TestConfChangeUnreliable3B怎么会有这么巧的网络导致我Follow可能超时成candidate更新不了commit，删除不了Leader集群不可用
			// 但Follow不会重新当选Leader，等都等了100tick了都当选不了，导致在apply时等着transfer很可能出问题，但这里transfer就完美避免了。。。。。。
			// 竟然还有极小概率出现问题：AppendEntry一直request失败导致超时。。。。吐血
			if len(d.Region().Peers) == 2 && request.ChangePeer.ChangeType == eraftpb.ConfChangeType_RemoveNode && request.ChangePeer.Peer.Id == d.PeerId() && d.IsLeader() {
				var toId uint64 = d.PeerId()
				for _, peer := range d.Region().Peers {
					if peer.Id != d.PeerId() {
						toId = peer.Id
						break
					}
				}
				if toId == d.PeerId() {
					panic("check the two peers transfer Leader")
				}
				engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- TransferBeforeProposeDestroy to-%v", d.Meta.StoreId, d.PeerId(), d.regionId, toId)

				d.RaftGroup.TransferLeader(toId)
				return
			}

			// 后面需要检验Region Epoch，需要将msg也放入
			peerContext, err := msg.Marshal()
			if err != nil {
				panic(err)
			}

			err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
				ChangeType: request.ChangePeer.ChangeType,
				NodeId:     request.ChangePeer.Peer.Id,
				Context:    peerContext,
			})
			// callback这里应该没用，看schedule中都没有处理返回结果，反正是过段时间重新调用
			if err != nil {
				cb.Done(ErrResp(&util.ErrStaleCommand{}))
			} else {
				response.AdminResponse = &raft_cmdpb.AdminResponse{
					CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
					TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
				}
				engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Propose[ConfChange]Command(index,term,request) -- entry-[%v,%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, d.RaftGroup.Raft.RaftLog.LastIndex(), d.Term(), request.ChangePeer)
				cb.Done(response)
			}
			return
		} else if request.CmdType == raft_cmdpb.AdminCmdType_Split {
			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}
			err = d.RaftGroup.Propose(data)
			if err != nil {
				cb.Done(ErrResp(err))
				engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- FailSplit1-%v,Propose[Split]Command(index,term,request) -- entry-[%v,%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, err, d.RaftGroup.Raft.RaftLog.LastIndex(), d.Term(), request.Split)

				return
			} else {
				response.AdminResponse = &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split:   &raft_cmdpb.SplitResponse{},
				}
				engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- Propose[Split]Command(index,term,request) -- entry-[%v,%v,%v]", d.Meta.StoreId, d.PeerId(), d.regionId, d.RaftGroup.Raft.RaftLog.LastIndex(), d.Term(), request.Split)
				cb.Done(response)
			}
		}
	}
	var data []byte
	data, err = msg.Marshal()
	if err != nil {
		panic(err)
	}
	// 最后是以日志项的形式，调用RawNode中的Propose函数来写入。
	err = d.RaftGroup.Propose(data)
	if err != nil {
		// 前面给的代码中作了是否是Leader的校验，貌似不需要了
		// ErrNotLeader: the raft command is proposed on a follower. so use it to let the client try other peers.
		// BindRespError()
		//panic(err)
		// 后面修改：因有Transfer导致无法propose，需要报错重发
		cb.Done(ErrResp(err))
		return
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	// 触发Raft的tick操作
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	// 触发GC操作，需要propose CompactLogRequest
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

// ScheduleCompactLog 将消息异步发送出去,实际删除log的操作在raftlog_gc中做
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

// 处理Raft Message，即其他节点发送过来的Raft内部消息
func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	// 验证消息，这里主要做了两个判断，一个时发送的目标StroeID是不是错误，一个时msg.RegionEpoch是否为nil
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		// 如果发送者的RegionEpoch不落后，且消息的目的地确实为Peer，则将自己Destroy
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
		// 如果用过了则删除snapshot
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	//记录从哪个节点发来了消息
	d.insertPeerCache(msg.GetFromPeer())
	// 继承自Peer,所以能直接点出来
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		// 当新增节点追上了Leader的Log进度，尝试去删除scheduler_client中缓存的操作指令
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
		// 发送一条让他删除自己的消息
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		// 如果目标peer id比自身小证明这是旧消息，不处理
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		//如果目标的peer id比自身大证明自身时旧Region，删除自身
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
		// 是否发错peer了
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
	// 如果meta中的region信息与d.Region信息不符，等待初始化
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	// 如果和现有的region有交集
	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		// 可能是region分裂了，snapRegion中的id与存在的region id不同，返回key
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
	//engine_util.DPrintf("destroy-d.ctx.router-%p", d.ctx.router)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
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

// raft_worker接收到PeerTickRaftLogGC对应的Tick消息时调用
func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	// 只有Leader才GC
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	// 需要满足无用Log超出RaftLogGcCountLimit才GC
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
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
	// 减少splitCheckTask的次数
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	// 验证是否应该执行该split
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- FailSplit2-%v,Propose[Split]Command(index,term,request) ", d.Meta.StoreId, d.PeerId(), d.regionId, err)

		cb.Done(ErrResp(err))
		return
	}
	engine_util.DPrintf("storeID,peerId,RegionId[%v,%v,%v] -- FailSplit3,Propose[Split]Command(index,term,request) ", d.Meta.StoreId, d.PeerId(), d.regionId)

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
