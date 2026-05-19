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

package maintainer

import (
	"container/heap"
	"fmt"
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type routeConflictDetector struct {
	changefeedID common.ChangeFeedID
	keyspaceMeta common.KeyspaceMeta
	router       routing.Router
	registry     *routing.TargetTableRegistry
	schemaStore  schemastore.SchemaStore

	pendingQueue pendingEventKeyHeap
	pendingEvents map[eventKey]*routePendingEvent

	reportError func(error)
	failed      bool
}

type routePendingEvent struct {
	transition *routeTransition
	prechecked bool
}

type routeDDLInfo struct {
	key           eventKey
	commitTs      uint64
	isSyncPoint   bool
	blockTables   *heartbeatpb.InfluencedTables
	droppedTables *heartbeatpb.InfluencedTables
	addedTables   []*heartbeatpb.Table
	updatedSchema []*heartbeatpb.SchemaIDChange
	mode          int64
}

type routeTransition struct {
	commitTs uint64
	removes  []int64
	adds     []routing.RouteBinding
}

func newRouteConflictDetector(
	changefeedID common.ChangeFeedID,
	keyspaceMeta common.KeyspaceMeta,
	replicaConfig *config.ReplicaConfig,
	reportError func(error),
) (*routeConflictDetector, error) {
	if replicaConfig == nil || replicaConfig.Sink == nil {
		return nil, nil
	}
	rules := replicaConfig.Sink.DispatchRules
	if len(rules) == 0 {
		return nil, nil
	}

	hasRouteRules := false
	for _, r := range rules {
		if r.TargetSchema != "" || r.TargetTable != "" {
			hasRouteRules = true
			break
		}
	}
	if !hasRouteRules {
		return nil, nil
	}

	router, err := routing.NewRouter(changefeedID, util.GetOrZero(replicaConfig.CaseSensitive), rules)
	if err != nil {
		return nil, err
	}
	registry, err := routing.NewTargetTableRegistry(nil)
	if err != nil {
		return nil, err
	}
	registry.SetChangefeedID(changefeedID)

	return &routeConflictDetector{
		changefeedID:  changefeedID,
		keyspaceMeta:  keyspaceMeta,
		router:        router,
		registry:      registry,
		schemaStore:   appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore),
		pendingEvents: make(map[eventKey]*routePendingEvent),
		reportError:   reportError,
	}, nil
}

func (d *routeConflictDetector) initializeFromTables(tables []commonEvent.Table, startTs uint64) error {
	if d == nil {
		return nil
	}
	bindings := make([]routing.RouteBinding, 0, len(tables))
	for _, t := range tables {
		tableInfo, err := d.schemaStore.GetTableInfo(d.keyspaceMeta, t.TableID, startTs)
		if err != nil {
			return err
		}
		result, err := d.router.RouteName(tableInfo.GetSchemaName(), tableInfo.GetTableName())
		if err != nil {
			return err
		}
		bindings = append(bindings, routing.NewRouteBinding(tableInfo, t.TableID, t.SchemaID, result))
	}

	registry, err := routing.NewTargetTableRegistry(bindings)
	if err != nil {
		return err
	}
	registry.SetChangefeedID(d.changefeedID)
	d.registry = registry

	log.Info("route registry initialized",
		zap.String("changefeed", d.changefeedID.Name()),
		zap.Int("bindingCount", len(bindings)))
	return nil
}

func (d *routeConflictDetector) precheck(info routeDDLInfo) (bool, error) {
	if !d.needsCheck(info) {
		return true, nil
	}
	pending, ok, err := d.getOrBuildPendingEvent(info)
	if err != nil {
		return false, d.fail(err)
	}
	if !ok {
		return true, nil
	}
	if !d.isPendingHead(info.key) {
		log.Info("route transition waits for earlier DDL",
			zap.String("changefeed", d.changefeedID.Name()),
			zap.Uint64("commitTs", info.commitTs),
			zap.Int64("mode", info.mode))
		return false, nil
	}
	if pending.prechecked {
		return true, nil
	}
	if err := d.registry.ValidateReplace(pending.transition.removes, pending.transition.adds); err != nil {
		return false, d.fail(err)
	}
	pending.prechecked = true
	log.Info("route transition prechecked",
		zap.String("changefeed", d.changefeedID.Name()),
		zap.Uint64("commitTs", info.commitTs),
		zap.Int("removes", len(pending.transition.removes)),
		zap.Int("adds", len(pending.transition.adds)))
	return true, nil
}

func (d *routeConflictDetector) apply(info routeDDLInfo) error {
	if !d.needsCheck(info) {
		return nil
	}
	pending, ok, err := d.getOrBuildPendingEvent(info)
	if err != nil {
		return d.fail(err)
	}
	if !ok {
		return nil
	}
	if !d.isPendingHead(info.key) {
		return d.fail(errors.ErrTableRouteConflict.GenWithStack(
			"route transition apply out of order, changefeed=%s, commitTs=%d",
			d.changefeedID.Name(), info.commitTs))
	}
	if !pending.prechecked {
		if err := d.registry.ValidateReplace(pending.transition.removes, pending.transition.adds); err != nil {
			return d.fail(err)
		}
		pending.prechecked = true
	}
	if err := d.registry.Replace(pending.transition.removes, pending.transition.adds); err != nil {
		return d.fail(err)
	}
	d.popPendingHead(info.key)
	log.Info("route transition applied",
		zap.String("changefeed", d.changefeedID.Name()),
		zap.Uint64("commitTs", info.commitTs),
		zap.Int("removes", len(pending.transition.removes)),
		zap.Int("adds", len(pending.transition.adds)))
	return nil
}

func (d *routeConflictDetector) needsCheck(info routeDDLInfo) bool {
	if d == nil || d.registry == nil || info.isSyncPoint {
		return false
	}
	if info.droppedTables != nil || len(info.addedTables) > 0 || len(info.updatedSchema) > 0 {
		return true
	}
	return info.blockTables != nil && len(info.blockTables.TableIDs) > 0
}

func (d *routeConflictDetector) getOrBuildPendingEvent(info routeDDLInfo) (*routePendingEvent, bool, error) {
	if pending, ok := d.pendingEvents[info.key]; ok {
		return pending, true, nil
	}
	transition, err := d.buildTransition(info)
	if err != nil {
		return nil, false, err
	}
	if transition == nil || (len(transition.removes) == 0 && len(transition.adds) == 0) {
		return nil, false, nil
	}
	pending := &routePendingEvent{transition: transition}
	heap.Push(&d.pendingQueue, info.key)
	d.pendingEvents[info.key] = pending
	return pending, true, nil
}

func (d *routeConflictDetector) isPendingHead(key eventKey) bool {
	return len(d.pendingQueue) > 0 && d.pendingQueue[0] == key
}

func (d *routeConflictDetector) popPendingHead(key eventKey) {
	if len(d.pendingQueue) == 0 || d.pendingQueue[0] != key {
		log.Panic("route pending queue head mismatch",
			zap.String("changefeed", d.changefeedID.Name()),
			zap.Any("expected", key),
			zap.Any("actual", d.pendingQueue))
	}
	heap.Pop(&d.pendingQueue)
	delete(d.pendingEvents, key)
}

func (d *routeConflictDetector) buildTransition(info routeDDLInfo) (*routeTransition, error) {
	builder := newRouteTransitionBuilder(info.commitTs)

	if info.droppedTables != nil {
		switch info.droppedTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			for _, tableID := range info.droppedTables.TableIDs {
				builder.addRemove(tableID)
			}
		case heartbeatpb.InfluenceType_DB:
			for _, b := range d.registry.GetBindingsBySchemaID(info.droppedTables.SchemaID) {
				builder.addRemove(b.ReplicaTableID)
			}
		case heartbeatpb.InfluenceType_All:
			for _, b := range d.registry.Snapshot() {
				builder.addRemove(b.ReplicaTableID)
			}
		}
	}

	for _, add := range info.addedTables {
		binding, err := d.buildBindingForTable(add.TableID, add.SchemaID, info.commitTs)
		if err != nil {
			return nil, err
		}
		builder.addBinding(binding)
	}

	for _, change := range info.updatedSchema {
		binding, err := d.buildBindingForTable(change.TableID, change.NewSchemaID, info.commitTs)
		if err != nil {
			return nil, err
		}
		builder.addRemove(change.TableID)
		builder.addBinding(binding)
	}

	if info.blockTables != nil && info.blockTables.InfluenceType == heartbeatpb.InfluenceType_Normal {
		for _, tableID := range info.blockTables.TableIDs {
			if builder.hasRemove(tableID) || builder.hasAdd(tableID) {
				continue
			}
			existing, exists := d.registry.GetBindingByReplicaID(tableID)
			if !exists {
				return nil, fmt.Errorf("route registry binding not found for table %d", tableID)
			}
			binding, err := d.buildBindingForTable(tableID, existing.SourceSchemaID, info.commitTs)
			if err != nil {
				return nil, err
			}
			if existing.Target != binding.Target ||
				existing.Source.Schema != binding.Source.Schema ||
				existing.Source.Table != binding.Source.Table {
				builder.addRemove(tableID)
				builder.addBinding(binding)
			}
		}
	}

	return builder.finish(), nil
}

func (d *routeConflictDetector) buildBindingForTable(tableID, schemaID int64, commitTs uint64) (routing.RouteBinding, error) {
	tableInfo, err := d.schemaStore.GetTableInfo(d.keyspaceMeta, tableID, commitTs)
	if err != nil {
		return routing.RouteBinding{}, err
	}
	if schemaID == 0 {
		if existing, ok := d.registry.GetBindingByReplicaID(tableID); ok {
			schemaID = existing.SourceSchemaID
		} else {
			return routing.RouteBinding{}, fmt.Errorf("schema ID hint is missing for table %d", tableID)
		}
	}
	result, err := d.router.RouteName(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return routing.RouteBinding{}, err
	}
	return routing.NewRouteBinding(tableInfo, tableID, schemaID, result), nil
}

func (d *routeConflictDetector) fail(err error) error {
	if err == nil {
		return nil
	}
	if d.reportError != nil && !d.failed {
		d.failed = true
		d.reportError(err)
	}
	return err
}

type routeTransitionBuilder struct {
	transition routeTransition
	removeSet  map[int64]struct{}
	addsByID   map[int64]routing.RouteBinding
}

func newRouteTransitionBuilder(commitTs uint64) *routeTransitionBuilder {
	return &routeTransitionBuilder{
		transition: routeTransition{commitTs: commitTs},
		removeSet:  make(map[int64]struct{}),
		addsByID:   make(map[int64]routing.RouteBinding),
	}
}

func (b *routeTransitionBuilder) addRemove(tableID int64) {
	if _, ok := b.removeSet[tableID]; ok {
		return
	}
	b.removeSet[tableID] = struct{}{}
	b.transition.removes = append(b.transition.removes, tableID)
}

func (b *routeTransitionBuilder) hasRemove(tableID int64) bool {
	_, ok := b.removeSet[tableID]
	return ok
}

func (b *routeTransitionBuilder) addBinding(binding routing.RouteBinding) {
	b.addsByID[binding.ReplicaTableID] = binding
}

func (b *routeTransitionBuilder) hasAdd(tableID int64) bool {
	_, ok := b.addsByID[tableID]
	return ok
}

func (b *routeTransitionBuilder) finish() *routeTransition {
	if len(b.addsByID) > 0 {
		ids := make([]int64, 0, len(b.addsByID))
		for id := range b.addsByID {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			b.transition.adds = append(b.transition.adds, b.addsByID[id])
		}
	}
	if len(b.transition.removes) == 0 && len(b.transition.adds) == 0 {
		return nil
	}
	return &b.transition
}
