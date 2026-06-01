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
	"maps"
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

type routeAdmin struct {
	changefeedID common.ChangeFeedID
	keyspaceMeta common.KeyspaceMeta
	router       routing.Router
	registry     *routing.TargetTableRegistry
	schemaStore  schemastore.SchemaStore
	tableSources map[int64]admission
	sourceRefs   map[routing.TableKey]int

	pendingQueue  pendingEventKeyHeap
	pendingEvents map[eventKey]*routePendingEvent
	// Route admin receives every BlockTables event because same-schema RENAME
	// TABLE is only discoverable by comparing schema snapshots. Events that do
	// not change source or target names, such as column/index DDLs, are cached
	// after the first comparison so resend/apply paths stay cheap.
	routeNeutralEventCache map[eventKey]struct{}

	reportError func(error)
	failed      bool
}

type routePendingEvent struct {
	transition *routeTransition
	prechecked bool
}

type tableRouteAdmission interface {
	precheck(info routeAdmissionInfo) (bool, error)
	apply(info routeAdmissionInfo) error
}

type routeAdmissionInfo struct {
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
	adds           []admission
}

type admission struct {
	tableID        int64
	sourceSchemaID int64
	binding        routing.RouteBinding
}

type routeAdmissionChange struct {
	releases []routing.TableKey
	admits   []routing.RouteBinding
}

func newRouteAdmin(
	changefeedID common.ChangeFeedID,
	keyspaceMeta common.KeyspaceMeta,
	replicaConfig *config.ReplicaConfig,
	reportError func(error),
	tables []commonEvent.Table,
) (*routeAdmin, error) {
	if replicaConfig == nil || replicaConfig.Sink == nil {
		return nil, nil
	}
	if !replicaConfig.Sink.TableRouteEnabled() {
		return nil, nil
	}

	router, err := routing.NewRouter(
		changefeedID,
		util.GetOrZero(replicaConfig.CaseSensitive),
		replicaConfig.Sink.DispatchRules,
	)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	tableSources := make(map[int64]admission, len(tables))
	sourceRefs := make(map[routing.TableKey]int, len(tables))
	admin := &routeAdmin{
		changefeedID:           changefeedID,
		keyspaceMeta:           keyspaceMeta,
		router:                 router,
		registry:               routing.NewTargetTableRegistry(changefeedID, len(tables)),
		tableSources:           tableSources,
		sourceRefs:             sourceRefs,
		schemaStore:            appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore),
		pendingEvents:          make(map[eventKey]*routePendingEvent),
		routeNeutralEventCache: make(map[eventKey]struct{}),
		reportError:            reportError,
	}

	for _, t := range tables {
		binding, err := admin.route(t.SchemaName, t.TableName)
		if err != nil {
			return nil, err
		}

		if sourceRefs[binding.Source] == 0 {
			if err := admin.applyAdmissionChange(routeAdmissionChange{admits: []routing.RouteBinding{binding}}, true); err != nil {
				return nil, err
			}
		}

		tableSources[t.TableID] = admission{
			tableID:        t.TableID,
			sourceSchemaID: t.SchemaID,
			binding:        binding,
		}
		sourceRefs[binding.Source]++
	}

	// todo: this log may can be removed after test huge number of tables.
	log.Info("route admin initialized",
		zap.String("changefeed", changefeedID.Name()),
		zap.Int("tableCount", len(tables)),
		zap.Duration("duration", time.Since(start)))

	return admin, nil
}

func (a *routeAdmin) route(schema, table string) (routing.RouteBinding, error) {
	return a.router.Route(schema, table)
}

func (a *routeAdmin) applyAdmissionChange(change routeAdmissionChange, mutate bool) error {
	return a.registry.ApplyTransition(change.releases, change.admits, mutate)
}

func (a *routeAdmin) precheck(info routeAdmissionInfo) (bool, error) {
	if !a.needsCheck(info) {
		return true, nil
	}
	if a.hasRouteNeutralEvent(info.key) {
		return true, nil
	}
	pending, ok, err := a.getOrBuildPendingEvent(info)
	if err != nil {
		return false, a.fail(err)
	}
	if !ok {
		return true, nil
	}
	if !a.isPendingHead(info.key) {
		log.Info("route transition waits for earlier DDL",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Uint64("commitTs", info.commitTs),
			zap.Int64("mode", info.mode))
		return false, nil
	}
	if pending.prechecked {
		return true, nil
	}
	transition := pending.transition
	change, err := a.applyTransition(transition, false)
	if err != nil {
		return false, a.fail(err)
	}
	pending.prechecked = true
	log.Info("route transition prechecked",
		zap.String("changefeed", a.changefeedID.Name()),
		zap.Uint64("commitTs", info.commitTs),
		zap.Int("releases", len(change.releases)),
		zap.Int("admits", len(change.admits)),
		zap.Int("tableRemoves", len(transition.removeTableIDs)),
		zap.Int("tableAdds", len(transition.adds)))
	return true, nil
}

func (a *routeAdmin) apply(info routeAdmissionInfo) error {
	if !a.needsCheck(info) {
		return nil
	}
	if a.consumeRouteNeutralEvent(info.key) {
		return nil
	}
	pending, ok := a.pendingEvents[info.key]
	if !ok {
		var err error
		pending, ok, err = a.getOrBuildPendingEvent(info)
		if err != nil {
			return a.fail(err)
		}
		if !ok {
			a.consumeRouteNeutralEvent(info.key)
			return nil
		}
	}
	if !a.isPendingHead(info.key) {
		return a.fail(errors.ErrTableRouteConflict.GenWithStack(
			"route transition apply out of order, changefeed=%s, commitTs=%d",
			a.changefeedID.Name(), info.commitTs))
	}
	transition := pending.transition
	change, err := a.applyTransition(transition, true)
	if err != nil {
		return a.fail(err)
	}
	a.popPendingHead(info.key)
	log.Info("route transition applied",
		zap.String("changefeed", a.changefeedID.Name()),
		zap.Uint64("commitTs", info.commitTs),
		zap.Int("releases", len(change.releases)),
		zap.Int("admits", len(change.admits)),
		zap.Int("tableRemoves", len(transition.removeTableIDs)),
		zap.Int("tableAdds", len(transition.adds)))
	return nil
}

func (a *routeAdmin) needsCheck(info routeAdmissionInfo) bool {
	if a == nil || a.registry == nil || info.isSyncPoint {
		return false
	}
	if info.droppedTables != nil || len(info.addedTables) > 0 || len(info.updatedSchema) > 0 {
		return true
	}
	return info.blockTables != nil && len(info.blockTables.TableIDs) > 0
}

func (a *routeAdmin) getOrBuildPendingEvent(info routeAdmissionInfo) (*routePendingEvent, bool, error) {
	if pending, ok := a.pendingEvents[info.key]; ok {
		return pending, true, nil
	}
	transition, err := a.buildTransition(info)
	if err != nil {
		return nil, false, err
	}
	if transition == nil {
		a.routeNeutralEventCache[info.key] = struct{}{}
		return nil, false, nil
	}
	pending := &routePendingEvent{transition: transition}
	heap.Push(&a.pendingQueue, info.key)
	a.pendingEvents[info.key] = pending
	return pending, true, nil
}

func (a *routeAdmin) hasRouteNeutralEvent(key eventKey) bool {
	_, ok := a.routeNeutralEventCache[key]
	return ok
}

func (a *routeAdmin) consumeRouteNeutralEvent(key eventKey) bool {
	_, ok := a.routeNeutralEventCache[key]
	delete(a.routeNeutralEventCache, key)
	return ok
}

func (a *routeAdmin) isPendingHead(key eventKey) bool {
	return len(a.pendingQueue) > 0 && a.pendingQueue[0] == key
}

func (a *routeAdmin) popPendingHead(key eventKey) {
	if len(a.pendingQueue) == 0 || a.pendingQueue[0] != key {
		log.Panic("route pending queue head mismatch",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Any("expected", key),
			zap.Any("actual", a.pendingQueue))
	}
	heap.Pop(&a.pendingQueue)
	delete(a.pendingEvents, key)
}

func (a *routeAdmin) buildTransition(info routeAdmissionInfo) (*routeTransition, error) {
	builder := newRouteTransitionBuilder(info.commitTs)

	if info.droppedTables != nil {
		switch info.droppedTables.InfluenceType {
		case heartbeatpb.InfluenceType_Normal:
			for _, tableID := range info.droppedTables.TableIDs {
				if _, ok := a.tableSources[tableID]; ok {
					builder.addRemove(tableID)
				}
			}
		case heartbeatpb.InfluenceType_DB:
			for _, existing := range a.tableSources {
				if existing.sourceSchemaID == info.droppedTables.SchemaID {
					builder.addRemove(existing.tableID)
				}
			}
		case heartbeatpb.InfluenceType_All:
			for _, existing := range a.tableSources {
				builder.addRemove(existing.tableID)
			}
		}
	}

	for _, add := range info.addedTables {
		binding, err := a.buildBindingForTable(add.TableID, add.SchemaID, info.commitTs)
		if err != nil {
			return nil, err
		}
		if existing, ok := a.tableSources[add.TableID]; ok {
			if existing.equal(binding) {
				continue
			}
			builder.addRemove(add.TableID)
		}
		builder.addBinding(binding)
	}

	for _, change := range info.updatedSchema {
		_, exists := a.tableSources[change.TableID]
		if !exists {
			return nil, errors.ErrInternalCheckFailed.GenWithStack(
				"route registry binding not found for table %d", change.TableID)
		}
		binding, err := a.buildBindingForTable(change.TableID, change.NewSchemaID, info.commitTs)
		if err != nil {
			return nil, err
		}
		builder.addRemove(change.TableID)
		builder.addBinding(binding)
	}

	if info.blockTables != nil && info.blockTables.InfluenceType == heartbeatpb.InfluenceType_Normal {
		for _, tableID := range info.blockTables.TableIDs {
			if tableID == common.DDLSpanTableID {
				continue
			}
			if builder.hasRemove(tableID) || builder.hasAdd(tableID) {
				continue
			}
			existing, exists := a.tableSources[tableID]
			if !exists {
				continue
			}
			binding, err := a.buildBindingForTable(tableID, existing.sourceSchemaID, info.commitTs)
			if err != nil {
				return nil, err
			}
			if existing.binding.Target != binding.binding.Target ||
				existing.binding.Source.Schema != binding.binding.Source.Schema ||
				existing.binding.Source.Table != binding.binding.Source.Table {
				builder.addRemove(tableID)
				builder.addBinding(binding)
			}
		}
	}

	return builder.finish(), nil
}

func (a *routeAdmin) buildBindingForTable(tableID, schemaID int64, commitTs uint64) (admission, error) {
	// Some DDL route transitions reach route admission before the target table
	// dispatcher is admitted, so route precheck asks only for the source name
	// and must not depend on dispatcher table registration.
	tableName, err := a.schemaStore.GetTableNameByID(a.keyspaceMeta, tableID, commitTs)
	if err != nil {
		return admission{}, err
	}
	if schemaID == 0 {
		if existing, ok := a.tableSources[tableID]; ok {
			schemaID = existing.sourceSchemaID
		} else {
			return admission{}, errors.ErrInternalCheckFailed.GenWithStack(
				"schema ID hint is missing for table %d", tableID)
		}
	}
	binding, err := a.route(tableName.Schema, tableName.Table)
	if err != nil {
		return admission{}, err
	}
	return admission{
		tableID:        tableID,
		sourceSchemaID: schemaID,
		binding:        binding,
	}, nil
}

func (a *routeAdmin) applyTransition(transition *routeTransition, mutate bool) (routeAdmissionChange, error) {
	change, nextRefs, err := a.buildAdmissionChange(transition)
	if err != nil {
		return routeAdmissionChange{}, err
	}
	if err := a.applyAdmissionChange(change, mutate); err != nil {
		return routeAdmissionChange{}, err
	}
	if !mutate {
		return change, nil
	}
	for _, tableID := range transition.removeTableIDs {
		delete(a.tableSources, tableID)
	}
	for _, add := range transition.adds {
		a.tableSources[add.tableID] = add
	}
	a.sourceRefs = nextRefs
	return change, nil
}

func (a *routeAdmin) buildAdmissionChange(
	transition *routeTransition,
) (routeAdmissionChange, map[routing.TableKey]int, error) {
	nextRefs := make(map[routing.TableKey]int, len(a.sourceRefs)+len(transition.adds))
	maps.Copy(nextRefs, a.sourceRefs)

	addsBySource := make(map[routing.TableKey]routing.RouteBinding, len(transition.adds))
	for _, tableID := range transition.removeTableIDs {
		existing, ok := a.tableSources[tableID]
		if !ok {
			continue
		}
		source := existing.binding.Source
		if nextRefs[source] > 0 {
			nextRefs[source]--
		}
	}
	for _, add := range transition.adds {
		source := add.binding.Source
		if existing, ok := addsBySource[source]; ok && !existing.Target.Equal(add.binding.Target) {
			return routeAdmissionChange{}, nil, errors.ErrInternalCheckFailed.GenWithStack(
				"source `%s`.`%s` is added to multiple targets `%s`.`%s` and `%s`.`%s` in one transition",
				source.Schema, source.Table,
				existing.Target.Schema, existing.Target.Table,
				add.binding.Target.Schema, add.binding.Target.Table)
		}
		addsBySource[source] = add.binding
		nextRefs[source]++
	}

	change := routeAdmissionChange{
		releases: make([]routing.TableKey, 0),
		admits:   make([]routing.RouteBinding, 0),
	}
	for source, count := range a.sourceRefs {
		if count > 0 && nextRefs[source] == 0 {
			change.releases = append(change.releases, source)
		}
	}
	for source, count := range nextRefs {
		if a.sourceRefs[source] != 0 || count == 0 {
			continue
		}
		binding, ok := addsBySource[source]
		if !ok {
			return routeAdmissionChange{}, nil, errors.ErrInternalCheckFailed.GenWithStack(
				"route admission binding not found for source `%s`.`%s`", source.Schema, source.Table)
		}
		change.admits = append(change.admits, binding)
	}
	slices.SortFunc(change.releases, compareTableKey)
	slices.SortFunc(change.admits, compareRouteBinding)

	for source, count := range nextRefs {
		if count == 0 {
			delete(nextRefs, source)
		}
	}
	return change, nextRefs, nil
}

func (a admission) equal(b admission) bool {
	return a.tableID == b.tableID &&
		a.sourceSchemaID == b.sourceSchemaID &&
		a.binding.Source.Equal(b.binding.Source) &&
		a.binding.Target.Equal(b.binding.Target)
}

func compareTableKey(a, b routing.TableKey) int {
	if a.Schema < b.Schema {
		return -1
	}
	if a.Schema > b.Schema {
		return 1
	}
	if a.Table < b.Table {
		return -1
	}
	if a.Table > b.Table {
		return 1
	}
	return 0
}

func compareRouteBinding(a, b routing.RouteBinding) int {
	if n := compareTableKey(a.Source, b.Source); n != 0 {
		return n
	}
	return compareTableKey(a.Target, b.Target)
}

func (a *routeAdmin) fail(err error) error {
	if err == nil {
		return nil
	}
	if a.reportError != nil && !a.failed {
		a.failed = true
		a.reportError(err)
	}
	return err
}

type routeTransitionBuilder struct {
	transition routeTransition
	removeSet  map[int64]struct{}
	addsByID   map[int64]admission
}

func newRouteTransitionBuilder(commitTs uint64) *routeTransitionBuilder {
	return &routeTransitionBuilder{
		transition: routeTransition{commitTs: commitTs},
		removeSet:  make(map[int64]struct{}),
		addsByID:   make(map[int64]admission),
	}
}

func (b *routeTransitionBuilder) addRemove(tableID int64) {
	if _, ok := b.removeSet[tableID]; ok {
		return
	}
	b.removeSet[tableID] = struct{}{}
	b.transition.removeTableIDs = append(b.transition.removeTableIDs, tableID)
}

func (b *routeTransitionBuilder) hasRemove(tableID int64) bool {
	_, ok := b.removeSet[tableID]
	return ok
}

func (b *routeTransitionBuilder) addBinding(binding admission) {
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
	if len(b.transition.removeTableIDs) == 0 && len(b.transition.adds) == 0 {
		return nil
	}
	return &b.transition
}
