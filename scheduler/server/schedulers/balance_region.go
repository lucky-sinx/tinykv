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
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
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

	// 1.首先，Scheduler 将选择所有合适的 store。然后根据它们的 region 大小进行排序。
	stores := cluster.GetStores()
	suitableStores := filter.SelectSourceStores(stores, []filter.Filter{filter.StoreStateFilter{ActionScope: s.GetName(), TransferLeader: true}}, cluster)
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	// 2.遍历suitableStores寻找合适的可转移的region
	var region *core.RegionInfo
	var sourceStoreIndex, targetStoreIndex = -1, -1
	for index, store := range suitableStores {
		var callback = func(container core.RegionsContainer) {
			// select a random region
			region = container.RandomRegion(nil, nil)
		}
		//首先，它将尝试选择一个挂起的 region
		cluster.GetPendingRegionsWithLock(store.GetID(), callback)
		if region != nil {
			sourceStoreIndex = index
			break
		}
		//如果没有一个挂起的 region，它将尝试找到一个 Follower region。
		cluster.GetFollowersWithLock(store.GetID(), callback)
		if region != nil {
			sourceStoreIndex = index
			break
		}
		//如果没有一个挂起的 region，它将尝试找到一个 Follower region。
		cluster.GetLeadersWithLock(store.GetID(), callback)
		if region != nil {
			sourceStoreIndex = index
			break
		}
	}
	if region == nil {
		// 找不到合适的region
		return nil
	}
	if len(region.GetStoreIds()) < cluster.GetMaxReplicas() {
		return nil
	}

	// 3.寻找target store,regionSize最小的store
	for i := len(suitableStores) - 1; i > sourceStoreIndex; i-- {
		storeId := suitableStores[i].GetID()
		_, ok := region.GetStoreIds()[storeId]
		if !ok {
			targetStoreIndex = i
			break
		}
	}
	if targetStoreIndex == -1 {
		return nil
	}
	// 4.判断是否值得进行move操作
	if suitableStores[sourceStoreIndex].GetRegionSize()-suitableStores[targetStoreIndex].GetRegionSize() >
		2*region.GetApproximateSize() {
		newPeer, err := cluster.AllocPeer(suitableStores[targetStoreIndex].GetID())
		if err != nil {
			return nil
		}
		peerOperator, err := operator.CreateMovePeerOperator("", cluster, region, operator.OpBalance, suitableStores[sourceStoreIndex].GetID(),
			suitableStores[targetStoreIndex].GetID(), newPeer.GetId())
		if err == nil {
			return peerOperator
		}
	}
	return nil
}
