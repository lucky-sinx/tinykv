package runner

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/scheduler_client"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/shirou/gopsutil/disk"
)

type SchedulerAskSplitTask struct {
	Region   *metapb.Region
	SplitKey []byte
	Peer     *metapb.Peer
	Callback *message.Callback
}

type SchedulerRegionHeartbeatTask struct {
	Region          *metapb.Region
	Peer            *metapb.Peer
	PendingPeers    []*metapb.Peer
	ApproximateSize *uint64
}

type SchedulerStoreHeartbeatTask struct {
	Stats  *schedulerpb.StoreStats
	Engine *badger.DB
	Path   string
}

type SchedulerTaskHandler struct {
	storeID         uint64
	SchedulerClient scheduler_client.Client
	router          message.RaftRouter
}

func NewSchedulerTaskHandler(storeID uint64, SchedulerClient scheduler_client.Client, router message.RaftRouter) *SchedulerTaskHandler {
	return &SchedulerTaskHandler{
		storeID:         storeID,
		SchedulerClient: SchedulerClient,
		router:          router,
	}
}

func (r *SchedulerTaskHandler) Handle(t worker.Task) {
	switch t.(type) {
	case *SchedulerAskSplitTask:
		r.onAskSplit(t.(*SchedulerAskSplitTask))
	case *SchedulerRegionHeartbeatTask:
		r.onHeartbeat(t.(*SchedulerRegionHeartbeatTask))
	case *SchedulerStoreHeartbeatTask:
		r.onStoreHeartbeat(t.(*SchedulerStoreHeartbeatTask))
	default:
		log.Errorf("unsupported worker.Task: %+v", t)
	}
}

func (r *SchedulerTaskHandler) Start() {
	//设置SchedulerRegionHeartbeatTask这个任务在SchedulerClient 中的回调函数
	r.SchedulerClient.SetRegionHeartbeatResponseHandler(r.storeID, r.onRegionHeartbeatResponse)
}

//HeartbeatTask处理完成后回调，向Leader发送AdminCmdType_ChangePeer以及AdminCmdType_TransferLeader消息
func (r *SchedulerTaskHandler) onRegionHeartbeatResponse(resp *schedulerpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerRequest{
				ChangeType: changePeer.ChangeType,
				Peer:       changePeer.Peer,
			},
		}, message.NewCallback())
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: transferLeader.Peer,
			},
		}, message.NewCallback())
	}
}

func (r *SchedulerTaskHandler) onAskSplit(t *SchedulerAskSplitTask) {
	// 分配新的regionId以及peerId
	// 在测试中使用的SchedulerClient是test_raftstore.scheduler
	resp, err := r.SchedulerClient.AskSplit(context.TODO(), t.Region)
	if err != nil {
		log.Error(err)
		engine_util.DPrintf(" FailSplit5-%v", err)

		return
	}

	//通过Router发送AdminCmdType_Split
	aq := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitRequest{
			SplitKey:    t.SplitKey,
			NewRegionId: resp.NewRegionId,
			NewPeerIds:  resp.NewPeerIds,
		},
	}
	r.sendAdminRequest(t.Region.GetId(), t.Region.GetRegionEpoch(), t.Peer, aq, t.Callback)
}

func (r *SchedulerTaskHandler) onHeartbeat(t *SchedulerRegionHeartbeatTask) {
	var size int64
	if t.ApproximateSize != nil {
		size = int64(*t.ApproximateSize)
	}

	req := &schedulerpb.RegionHeartbeatRequest{
		Region:          t.Region,
		Leader:          t.Peer,
		PendingPeers:    t.PendingPeers,
		ApproximateSize: uint64(size),
	}
	// 向SchedulerClient发送RegionHeartbeat请求，触发它处理对应Region中未完成的任务，如addNode
	// 在测试中使用的SchedulerClient是test_raftstore.scheduler
	r.SchedulerClient.RegionHeartbeat(req)
}

func (r *SchedulerTaskHandler) onStoreHeartbeat(t *SchedulerStoreHeartbeatTask) {
	diskStat, err := disk.Usage(t.Path)
	if err != nil {
		log.Error(err)
		return
	}

	capacity := diskStat.Total
	lsmSize, vlogSize := t.Engine.Size()
	usedSize := t.Stats.UsedSize + uint64(lsmSize) + uint64(vlogSize) // t.Stats.UsedSize contains size of snapshot files.
	available := uint64(0)
	if capacity > usedSize {
		available = capacity - usedSize
	}

	t.Stats.Capacity = capacity
	t.Stats.UsedSize = usedSize
	t.Stats.Available = available

	r.SchedulerClient.StoreHeartbeat(context.TODO(), t.Stats)
}

func (r *SchedulerTaskHandler) sendAdminRequest(regionID uint64, epoch *metapb.RegionEpoch, peer *metapb.Peer, req *raft_cmdpb.AdminRequest, callback *message.Callback) {
	cmd := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    regionID,
			Peer:        peer,
			RegionEpoch: epoch,
		},
		AdminRequest: req,
	}
	r.router.SendRaftCommand(cmd, callback)
}
