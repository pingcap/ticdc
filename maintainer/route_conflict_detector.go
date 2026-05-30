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
	"slices"
	"time"

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
	tables       map[int64]routeTableBinding

	pendingQueue  pendingEventKeyHeap
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
	commitTs       uint64
	removeTableIDs []int64
	removes        []routing.TableKey
	adds           []routeTableBinding
}

type routeTableBinding struct {
	tableID        int64
	sourceSchemaID int64
	binding        routing.RouteBinding
}

func newRouteConflictDetector(
	changefeedID common.ChangeFeedID,
	keyspaceMeta common.KeyspaceMeta,
	replicaConfig *config.ReplicaConfig,
	reportError func(error),
	tables []commonEvent.Table,
) (*routeConflictDetector, error) {
	if replicaConfig == nil || replicaConfig.Sink == nil {
		return nil, nil
	}
	if !replicaConfig.Sink.TableRouteEnabled() {
		return nil, nil
	}

	router, err := routing.NewRouter(changefeedID, util.GetOrZero(replicaConfig.CaseSensitive), replicaConfig.Sink.DispatchRules)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	currentTables := make(map[int64]routeTableBinding, len(tables))
	registry := routing.NewTargetTableRegistry(changefeedID, len(tables))
	for _, t := range tables {
		binding, err := router.Route(t.SchemaName, t.TableName)
		if err != nil {
			return nil, err
		}

		if err := registry.Add(binding); err != nil {
			return nil, err
		}

		currentTables[t.TableID] = routeTableBinding{
			tableID:        t.TableID,
			sourceSchemaID: t.SchemaID,
			binding:        binding,
		}
	}

	// todo: this log may can be removed after test huge number of tables.
	log.Info("route conflict detector initialized",
		zap.String("changefeed", changefeedID.Name()),
		zap.Int("tableCount", len(tables)),
		zap.Duration("duration", time.Since(start)))

	return &routeConflictDetector{
		changefeedID:  changefeedID,
		keyspaceMeta:  keyspaceMeta,
		router:        router,
		registry:      registry,
		tables:        currentTables,
		schemaStore:   appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore),
		pendingEvents: make(map[eventKey]*routePendingEvent),
		reportError:   reportError,
	}, nil
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
	adds := routeTransitionAdds(pending.transition)
	if err := d.registry.ApplyTransition(pending.transition.removes, adds, false); err != nil {
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
	adds := routeTransitionAdds(pending.transition)
	if err := d.registry.ApplyTransition(pending.transition.removes, adds, true); err != nil {
		return d.fail(err)
	}
	for _, tableID := range pending.transition.removeTableIDs {
		delete(d.tables, tableID)
	}
	for _, add := range pending.transition.adds {
		d.tables[add.tableID] = add
	}
	d.popPendingHead(info.key)
	log.Info("route transition applied",
		zap.String("changefeed", d.changefeedID.Name()),
		zap.Uint64("commitTs", info.commitTs),
		zap.Int("removes", len(pending.transition.removes)),
		zap.Int("adds", len(pending.transition.adds)))
	return nil
}

func routeTransitionAdds(transition *routeTransition) []routing.RouteBinding {
	adds := make([]routing.RouteBinding, 0, len(transition.adds))
	for _, add := range transition.adds {
		adds = append(adds, add.binding)
	}
	return adds
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
				if existing, ok := d.tables[tableID]; ok {
					builder.addRemove(tableID, existing.binding.Source)
				}
			}
		case heartbeatpb.InfluenceType_DB:
			for _, existing := range d.tables {
				if existing.sourceSchemaID == info.droppedTables.SchemaID {
					builder.addRemove(existing.tableID, existing.binding.Source)
				}
			}
		case heartbeatpb.InfluenceType_All:
			for _, existing := range d.tables {
				builder.addRemove(existing.tableID, existing.binding.Source)
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
		existing, exists := d.tables[change.TableID]
		if !exists {
			return nil, errors.ErrInternalCheckFailed.GenWithStack(
				"route registry binding not found for table %d", change.TableID)
		}
		binding, err := d.buildBindingForTable(change.TableID, change.NewSchemaID, info.commitTs)
		if err != nil {
			return nil, err
		}
		builder.addRemove(change.TableID, existing.binding.Source)
		builder.addBinding(binding)
	}

	if info.blockTables != nil && info.blockTables.InfluenceType == heartbeatpb.InfluenceType_Normal {
		for _, tableID := range info.blockTables.TableIDs {
			if builder.hasRemove(tableID) || builder.hasAdd(tableID) {
				continue
			}
			existing, exists := d.tables[tableID]
			if !exists {
				return nil, errors.ErrInternalCheckFailed.GenWithStack(
					"route registry binding not found for table %d", tableID)
			}
			binding, err := d.buildBindingForTable(tableID, existing.sourceSchemaID, info.commitTs)
			if err != nil {
				return nil, err
			}
			if existing.binding.Target != binding.binding.Target ||
				existing.binding.Source.Schema != binding.binding.Source.Schema ||
				existing.binding.Source.Table != binding.binding.Source.Table {
				builder.addRemove(tableID, existing.binding.Source)
				builder.addBinding(binding)
			}
		}
	}

	return builder.finish(), nil
}

func (d *routeConflictDetector) buildBindingForTable(tableID, schemaID int64, commitTs uint64) (routeTableBinding, error) {
	// New-table DDL reaches the detector before the table dispatcher is admitted,
	// so the schema lookup must not depend on dispatcher table registration.
	tableInfo, err := d.schemaStore.ForceGetTableInfo(d.keyspaceMeta, tableID, commitTs)
	if err != nil {
		return routeTableBinding{}, err
	}
	if schemaID == 0 {
		if existing, ok := d.tables[tableID]; ok {
			schemaID = existing.sourceSchemaID
		} else {
			return routeTableBinding{}, errors.ErrInternalCheckFailed.GenWithStack(
				"schema ID hint is missing for table %d", tableID)
		}
	}
	binding, err := d.router.Route(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return routeTableBinding{}, err
	}
	return routeTableBinding{
		tableID:        tableID,
		sourceSchemaID: schemaID,
		binding:        binding,
	}, nil
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
	addsByID   map[int64]routeTableBinding
}

func newRouteTransitionBuilder(commitTs uint64) *routeTransitionBuilder {
	return &routeTransitionBuilder{
		transition: routeTransition{commitTs: commitTs},
		removeSet:  make(map[int64]struct{}),
		addsByID:   make(map[int64]routeTableBinding),
	}
}

func (b *routeTransitionBuilder) addRemove(tableID int64, source routing.TableKey) {
	if _, ok := b.removeSet[tableID]; ok {
		return
	}
	b.removeSet[tableID] = struct{}{}
	b.transition.removeTableIDs = append(b.transition.removeTableIDs, tableID)
	b.transition.removes = append(b.transition.removes, source)
}

func (b *routeTransitionBuilder) hasRemove(tableID int64) bool {
	_, ok := b.removeSet[tableID]
	return ok
}

func (b *routeTransitionBuilder) addBinding(binding routeTableBinding) {
	b.addsByID[binding.tableID] = binding
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
		slices.Sort(ids)
		for _, id := range ids {
			b.transition.adds = append(b.transition.adds, b.addsByID[id])
		}
	}
	if len(b.transition.removes) == 0 && len(b.transition.adds) == 0 {
		return nil
	}
	return &b.transition
}
