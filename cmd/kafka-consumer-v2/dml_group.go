// Copyright 2026 PingCAP, Inc.
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

package main

import (
	"slices"
	"sort"
	"sync"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type dmlGroup struct {
	partition int32
	tableID   int64

	events           []*commonEvent.DMLEvent
	highWatermark    uint64
	appliedWatermark uint64
	mu               sync.Mutex
}

func newDMLGroup(partition int32, tableID int64) *dmlGroup {
	return &dmlGroup{
		partition: partition,
		tableID:   tableID,
		events:    make([]*commonEvent.DMLEvent, 0, 1024),
	}
}

func (g *dmlGroup) Append(row *commonEvent.DMLEvent, source *messageSource, force bool) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if row.CommitTs > g.highWatermark {
		g.highWatermark = row.CommitTs
	}

	mergeDMLEvent := func(dst, src *commonEvent.DMLEvent) {
		dst.Rows.Append(src.Rows, 0, src.Rows.NumRows())
		dst.RowTypes = append(dst.RowTypes, src.RowTypes...)
		dst.Length += src.Length
		dst.PostTxnFlushed = append(dst.PostTxnFlushed, src.PostTxnFlushed...)
	}

	attachSource := func(event *commonEvent.DMLEvent) {
		event.AddPostFlushFunc(source.Done)
	}

	var lastDMLEvent *commonEvent.DMLEvent
	if len(g.events) > 0 {
		lastDMLEvent = g.events[len(g.events)-1]
	}
	if lastDMLEvent == nil || lastDMLEvent.GetCommitTs() < row.GetCommitTs() {
		attachSource(row)
		g.events = append(g.events, row)
		return true, nil
	}

	if lastDMLEvent.GetCommitTs() == row.GetCommitTs() {
		if !sameTableInfo(lastDMLEvent, row) {
			log.Warn("skip replayed DML event due to incompatible table info",
				zap.Int32("partition", g.partition), zap.Int64("tableID", g.tableID),
				zap.Uint64("commitTs", row.CommitTs),
				zap.Any("previous", lastDMLEvent),
				zap.Any("now", row))
			return false, nil
		}
		attachSource(row)
		mergeDMLEvent(lastDMLEvent, row)
		return true, nil
	}

	if !force {
		return false, errors.Errorf("append event with smaller commit ts, partition=%d tableID=%d lastCommitTs=%d commitTs=%d",
			g.partition, g.tableID, lastDMLEvent.GetCommitTs(), row.GetCommitTs())
	}

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > row.CommitTs
	})
	if i > 0 && g.events[i-1].CommitTs == row.CommitTs {
		previous := g.events[i-1]
		if !sameTableInfo(previous, row) {
			log.Warn("skip replayed DML event due to incompatible table info",
				zap.Int32("partition", g.partition), zap.Int64("tableID", g.tableID),
				zap.Uint64("commitTs", row.CommitTs),
				zap.Any("previous", previous),
				zap.Any("now", row))
			return false, nil
		}
		attachSource(row)
		mergeDMLEvent(previous, row)
		return true, nil
	}

	attachSource(row)
	g.events = slices.Insert(g.events, i, row)
	return true, nil
}

func (g *dmlGroup) MarkApplied(commitTs uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if commitTs > g.appliedWatermark {
		g.appliedWatermark = commitTs
	}
}

func (g *dmlGroup) AppliedWatermark() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.appliedWatermark
}

func (g *dmlGroup) HighWatermark() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.highWatermark
}

func (g *dmlGroup) Observe(commitTs uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if commitTs > g.highWatermark {
		g.highWatermark = commitTs
	}
}

func (g *dmlGroup) PopDispatchable(maxCommitTs uint64) []*commonEvent.DMLEvent {
	g.mu.Lock()
	defer g.mu.Unlock()

	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > maxCommitTs
	})
	if i == 0 {
		return nil
	}
	result := append([]*commonEvent.DMLEvent(nil), g.events[:i]...)
	clear(g.events[:i])
	g.events = g.events[i:]
	return result
}

func sameTableInfo(previous, now *commonEvent.DMLEvent) bool {
	previousInfo := previous.TableInfo.ToTiDBTableInfo()
	nowInfo := now.TableInfo.ToTiDBTableInfo()
	if previousInfo.UpdateTS > nowInfo.UpdateTS {
		log.Warn("previous DML event has newer table info version",
			zap.Any("previous", previous),
			zap.Any("now", now))
		return false
	}
	return commonType.NewColumnSchema4Decoder(previousInfo).SameWithTableInfo(nowInfo)
}

type resolvedDMLBatch struct {
	events   []*commonEvent.DMLEvent
	groupMax map[*dmlGroup]uint64
}

func newResolvedDMLBatch() resolvedDMLBatch {
	return resolvedDMLBatch{
		events:   make([]*commonEvent.DMLEvent, 0),
		groupMax: make(map[*dmlGroup]uint64),
	}
}

func (b *resolvedDMLBatch) AppendGroup(group *dmlGroup, resolveTs uint64) {
	i := sort.Search(len(group.events), func(i int) bool {
		return group.events[i].CommitTs > resolveTs
	})
	if i == 0 {
		return
	}
	lastCommitTs := group.events[i-1].CommitTs
	b.events = append(b.events, group.events[:i]...)
	clear(group.events[:i])
	group.events = group.events[i:]
	if lastCommitTs > b.groupMax[group] {
		b.groupMax[group] = lastCommitTs
	}
}

func (b resolvedDMLBatch) Empty() bool {
	return len(b.events) == 0
}
