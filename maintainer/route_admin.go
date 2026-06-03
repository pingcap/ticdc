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
	"cmp"
	"container/heap"
	"slices"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/routing"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// routeAdmin owns table-route admission state in the maintainer.
//
// It turns ordered DDL barrier events into source-table admission/release
// transitions, validates those transitions against TargetTableRegistry before a
// conflicting dispatcher can be admitted, and applies the transition after the
// DDL is known to have advanced. Related DDL events are serialized by eventKey;
// route-neutral DDLs are remembered so repeated barrier handling stays cheap.
type routeAdmin struct {
	changefeedID common.ChangeFeedID
	keyspaceMeta common.KeyspaceMeta
	router       routing.Router
	registry     *routing.TargetTableRegistry
	// getTableNameByID is intentionally narrower than SchemaStore. Route
	// admission only needs the source name at the DDL commit ts when an
	// existing table's binding must be refreshed without dispatcher-provided
	// routeTables.
	getTableNameByID getTableNameByIDFunc
	// tableSources is the admission snapshot for currently admitted source tables,
	// keyed by logical table ID. DDL transitions use it to map table IDs from
	// barrier metadata back to source names and current routed targets.
	tableSources map[int64]admission
	// sourceRefs counts how many admitted physical/logical table IDs currently
	// share the same source name. The route registry should admit a source only
	// on the first reference and release it only after the last reference leaves.
	sourceRefs map[routing.TableKey]int

	// pendingQueue and pendingEvents keep route-affecting DDL transitions in
	// commit order. A later route transition can be prechecked only after every
	// earlier route transition has been applied to the admission snapshot.
	pendingQueue  pendingEventKeyHeap
	pendingEvents map[eventKey]*routePendingEvent
	// Route admin receives every BlockTables event because same-schema RENAME
	// TABLE is only discoverable by comparing schema snapshots. Events that do
	// not change source or target names, such as column/index DDLs, are cached
	// after the first comparison so resend/apply paths stay cheap.
	routeNeutralEventCache map[eventKey]struct{}

	// reportError reports unrecoverable route admission errors to the
	// changefeed-level error path. failed suppresses duplicate reports from
	// dispatcher/barrier resends of the same broken state.
	reportError func(error)
	failed      bool
}

type getTableNameByIDFunc func(common.KeyspaceMeta, int64, uint64) (common.TableName, error)

// routePendingEvent is the cached route transition for one DDL event waiting in
// pendingQueue. prechecked means the transition has already passed the registry
// dry-run, so repeated barrier reports do not redo the same validation.
type routePendingEvent struct {
	transition *routeTransition
	prechecked bool
}

// tableRouteAdmission is the minimal protocol Barrier needs from table route
// admission. Keeping this interface small avoids coupling Barrier to routeAdmin
// internals.
type tableRouteAdmission interface {
	precheck(info routeAdmission) (bool, error)
	apply(info routeAdmission) error
}

// routeAdmission is the route-relevant snapshot derived from one BarrierEvent.
// routeAdmin consumes this instead of BarrierEvent so route logic depends only
// on DDL admission inputs, not on Barrier's dispatcher bookkeeping.
type routeAdmission struct {
	// key orders route-affecting DDLs in the same order Barrier uses.
	key eventKey
	// commitTs is the schema snapshot timestamp used when maintainer must rebuild a table name.
	commitTs uint64
	// isSyncPoint is part of key and distinguishes sync-point barriers from DDL barriers at the same ts.
	isSyncPoint bool
	// blockTables is the DDL's affected table scope. routeAdmin uses it only to
	// refresh already-admitted route owners when their source/target names may change.
	blockTables *commonEvent.InfluencedTables
	// droppedTables releases existing route owners.
	droppedTables *commonEvent.InfluencedTables
	// routeTables carries complete dispatcher-provided per-table source/target
	// names. Incomplete protocol entries are filtered out at the BarrierEvent
	// boundary.
	routeTables []routeTableAdmissionInfo
	// updatedSchema carries per-table schema ID changes, such as cross-schema RENAME TABLE.
	updatedSchema []commonEvent.SchemaIDChange
}

// routeTableAdmissionInfo is the complete, route-specific table binding form
// accepted by routeAdmin. The protobuf adapter must not pass partial entries.
type routeTableAdmissionInfo struct {
	tableID  int64
	schemaID int64
	binding  routing.RouteBinding
}

// routeTransition is the normalized mutation produced from one routeAdmission.
// removeTableIDs releases currently admitted table IDs; adds admits the source
// names visible at commitTs. The same transition is used for both precheck and
// apply so the validated state change is not rebuilt differently later.
type routeTransition struct {
	commitTs       uint64
	removeTableIDs []int64
	adds           []admission
}

// admission is the maintainer's current view of one admitted source table ID.
// sourceSchemaID is kept separately because DROP DATABASE releases by schema ID,
// while the registry conflict check is based on the source/target names.
type admission struct {
	tableID        int64
	sourceSchemaID int64
	binding        routing.RouteBinding
}

// routeAdmissionChange is the registry-level projection of routeTransition:
// source names to release and source-to-target bindings to admit.
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
	getTableNameByID getTableNameByIDFunc,
) (*routeAdmin, error) {
	if replicaConfig == nil || replicaConfig.Sink == nil {
		return nil, nil
	}
	if !replicaConfig.Sink.TableRouteEnabled() {
		return nil, nil
	}
	if getTableNameByID == nil {
		return nil, errors.ErrInternalCheckFailed.GenWithStack("route table name getter is nil")
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
		getTableNameByID:       getTableNameByID,
		pendingEvents:          make(map[eventKey]*routePendingEvent),
		routeNeutralEventCache: make(map[eventKey]struct{}),
		reportError:            reportError,
	}

	for _, t := range tables {
		binding, err := admin.router.Route(t.SchemaName, t.TableName)
		if err != nil {
			return nil, err
		}

		if sourceRefs[binding.Source] == 0 {
			if err := admin.registry.ApplyTransition(nil, []routing.RouteBinding{binding}, true); err != nil {
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

func (a *routeAdmin) precheck(info routeAdmission) (bool, error) {
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
			zap.Uint64("commitTs", info.commitTs))
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

func (a *routeAdmin) apply(info routeAdmission) error {
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

func (a *routeAdmin) needsCheck(info routeAdmission) bool {
	if a == nil || a.registry == nil || info.isSyncPoint {
		return false
	}
	if info.droppedTables != nil || len(info.routeTables) > 0 || len(info.updatedSchema) > 0 {
		return true
	}
	return info.blockTables != nil && len(info.blockTables.TableIDs) > 0
}

func (a *routeAdmin) getOrBuildPendingEvent(info routeAdmission) (*routePendingEvent, bool, error) {
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

func (a *routeAdmin) buildTransition(info routeAdmission) (*routeTransition, error) {
	builder := newRouteTransitionBuilder(info.commitTs)

	if info.droppedTables != nil {
		switch info.droppedTables.InfluenceType {
		case commonEvent.InfluenceTypeNormal:
			for _, tableID := range info.droppedTables.TableIDs {
				if _, ok := a.tableSources[tableID]; ok {
					builder.addRemove(tableID)
				}
			}
		case commonEvent.InfluenceTypeDB:
			for _, existing := range a.tableSources {
				if existing.sourceSchemaID == info.droppedTables.SchemaID {
					builder.addRemove(existing.tableID)
				}
			}
		case commonEvent.InfluenceTypeAll:
			for _, existing := range a.tableSources {
				builder.addRemove(existing.tableID)
			}
		}
	}

	for _, change := range info.updatedSchema {
		if a.hasRouteAdmission(info.routeTables, change.TableID) {
			continue
		}
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

	for _, table := range info.routeTables {
		if builder.hasRemove(table.tableID) {
			continue
		}
		binding, err := a.buildBindingForRouteAdmission(table)
		if err != nil {
			return nil, err
		}
		a.addBindingToTransition(builder, binding)
	}

	if info.blockTables != nil && info.blockTables.InfluenceType == commonEvent.InfluenceTypeNormal {
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
			if a.hasRouteAdmission(info.routeTables, tableID) {
				continue
			}
			binding, err := a.buildBindingForTable(tableID, existing.sourceSchemaID, info.commitTs)
			if err != nil {
				return nil, err
			}
			if !existing.equal(binding) {
				builder.addRemove(tableID)
				builder.addBinding(binding)
			}
		}
	}

	return builder.finish(), nil
}

func (a *routeAdmin) addBindingToTransition(builder *routeTransitionBuilder, binding admission) {
	if existing, ok := a.tableSources[binding.tableID]; ok {
		if existing.equal(binding) {
			return
		}
		builder.addRemove(binding.tableID)
	}
	builder.addBinding(binding)
}

func (a *routeAdmin) buildBindingForTable(tableID, schemaID int64, commitTs uint64) (admission, error) {
	tableName, err := a.getTableNameByID(a.keyspaceMeta, tableID, commitTs)
	if err != nil {
		return admission{}, err
	}
	binding, err := a.router.Route(tableName.Schema, tableName.Table)
	if err != nil {
		return admission{}, err
	}
	return a.newAdmission(tableID, schemaID, binding)
}

func (a *routeAdmin) buildBindingForRouteAdmission(table routeTableAdmissionInfo) (admission, error) {
	return a.newAdmission(table.tableID, table.schemaID, table.binding)
}

func (a *routeAdmin) newAdmission(tableID, schemaID int64, binding routing.RouteBinding) (admission, error) {
	if schemaID == 0 {
		existing, ok := a.tableSources[tableID]
		if !ok {
			return admission{}, errors.ErrInternalCheckFailed.GenWithStack(
				"schema ID hint is missing for table %d", tableID)
		}
		schemaID = existing.sourceSchemaID
	}
	return admission{
		tableID:        tableID,
		sourceSchemaID: schemaID,
		binding:        binding,
	}, nil
}

func (a *routeAdmin) hasRouteAdmission(tables []routeTableAdmissionInfo, tableID int64) bool {
	for _, table := range tables {
		if table.tableID == tableID {
			return true
		}
	}
	return false
}

func (a *routeAdmin) applyTransition(transition *routeTransition, mutate bool) (routeAdmissionChange, error) {
	change, refDeltas, err := a.buildAdmissionChange(transition)
	if err != nil {
		return routeAdmissionChange{}, err
	}
	if err := a.registry.ApplyTransition(change.releases, change.admits, mutate); err != nil {
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
	for source, delta := range refDeltas {
		next := a.sourceRefs[source] + delta
		if next == 0 {
			delete(a.sourceRefs, source)
			continue
		}
		a.sourceRefs[source] = next
	}
	return change, nil
}

func (a *routeAdmin) buildAdmissionChange(
	transition *routeTransition,
) (routeAdmissionChange, map[routing.TableKey]int, error) {
	addsBySource := make(map[routing.TableKey]routing.RouteBinding, len(transition.adds))
	refDeltas := make(map[routing.TableKey]int, len(transition.removeTableIDs)+len(transition.adds))
	for _, tableID := range transition.removeTableIDs {
		existing, ok := a.tableSources[tableID]
		if !ok {
			continue
		}
		source := existing.binding.Source
		refDeltas[source]--
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
		refDeltas[source]++
	}

	change := routeAdmissionChange{
		releases: make([]routing.TableKey, 0),
		admits:   make([]routing.RouteBinding, 0),
	}
	for source, delta := range refDeltas {
		oldCount := a.sourceRefs[source]
		nextCount := oldCount + delta
		if nextCount < 0 {
			return routeAdmissionChange{}, nil, errors.ErrInternalCheckFailed.GenWithStack(
				"route admission source reference count becomes negative for `%s`.`%s`",
				source.Schema, source.Table)
		}
		if oldCount > 0 && nextCount == 0 {
			change.releases = append(change.releases, source)
			continue
		}
		if oldCount != 0 || nextCount <= 0 {
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

	return change, refDeltas, nil
}

func (a admission) equal(b admission) bool {
	return a.tableID == b.tableID &&
		a.sourceSchemaID == b.sourceSchemaID &&
		a.binding.Source.Equal(b.binding.Source) &&
		a.binding.Target.Equal(b.binding.Target)
}

func compareTableKey(a, b routing.TableKey) int {
	if n := cmp.Compare(a.Schema, b.Schema); n != 0 {
		return n
	}
	return cmp.Compare(a.Table, b.Table)
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
