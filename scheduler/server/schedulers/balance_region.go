// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. 获取健康的 store 列表
	allStores := cluster.GetStores()
	healthStores := make([]*core.StoreInfo, 0)
	for i, _ := range allStores {
		// store 必须状态是 up 且最近心跳的间隔小于集群判断宕机的时间阈值
		if allStores[i].IsUp() && allStores[i].DownTime() <= cluster.GetMaxStoreDownTime() {
			healthStores = append(healthStores, allStores[i])
		}
	}
	// 如果列表长度小于等于 1 则不可调度，返回空即可
	if len(healthStores) <= 1 {
		return nil
	}
	// 按照 regionSize 对 store 从大到小排序
	sort.Slice(healthStores, func(i, j int) bool {
		return healthStores[i].GetRegionSize() > healthStores[j].GetRegionSize()
	})

	// 2.寻找可调度的store
	fromStore := new(core.StoreInfo)
	// 寻找store上哪个region被调度
	var regionInfo *core.RegionInfo = nil
	for _, store := range healthStores {
		fromStore = store
		// 按照大小在所有 store 上从大到小依次寻找可以调度的 region，优先级依次是 pending，follower，leader
		//pending
		cluster.GetPendingRegionsWithLock(store.GetID(), func(regionsContainer core.RegionsContainer) {
			regionInfo = regionsContainer.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}

		//follower
		cluster.GetFollowersWithLock(store.GetID(), func(regionsContainer core.RegionsContainer) {
			regionInfo = regionsContainer.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}

		//leader
		cluster.GetLeadersWithLock(store.GetID(), func(regionsContainer core.RegionsContainer) {
			regionInfo = regionsContainer.RandomRegion(nil, nil)
		})
		if regionInfo != nil {
			break
		}
	}
	if regionInfo == nil {
		return nil
	}
	// 获得被调度region所在的所有store
	regionInStores := regionInfo.GetStoreIds()
	// 如果能够获取到 region 且 region 的 peer 个数等于集群的副本数，则说明该 region 可能可以在该 store 上被调度走
	if len(regionInStores) != cluster.GetMaxReplicas() {
		return nil
	}
	// 3.寻找被调度的 store
	// 按照大小在所有 store 上从小到大依次寻找不存在该 region 的 store
	targetStore := make([]*core.StoreInfo, 0)
	for i := range healthStores {
		flag := true
		for j := range regionInStores {
			if healthStores[i].GetID() == j {
				flag = false
				break
			}
		}
		if flag {
			targetStore = append(targetStore, healthStores[i])
		}
	}
	if len(targetStore) == 0 {
		return nil
	}
	// 按照 regionSize 对 store 从小到大排序
	sort.Slice(targetStore, func(i, j int) bool {
		return targetStore[i].GetRegionSize() < targetStore[j].GetRegionSize()
	})
	toStore := targetStore[0]
	// 找到后判断迁移是否有价值，即两个 store 的大小差值是否大于 region 的两倍大小，这样迁移之后其大小关系依然不会发生改变
	if fromStore.GetRegionSize()-toStore.GetRegionSize() <= 2*regionInfo.GetApproximateSize() {
		return nil
	}
	// 在新 store 上申请一个该 region 的 peer
	peer, err := cluster.AllocPeer(toStore.GetID())
	if err != nil {
		return nil
	}
	//创建对应的 MovePeerOperator 即可
	movePeerOperator, err1 := operator.CreateMovePeerOperator("balance_region", cluster, regionInfo, operator.OpBalance, fromStore.GetID(), toStore.GetID(), peer.GetId())
	if err1 != nil {
		return nil
	}
	return movePeerOperator
}
