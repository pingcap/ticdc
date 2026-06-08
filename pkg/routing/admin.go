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

package routing

import (
	"cmp"
	"container/heap"
	"slices"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// Admin owns table-route admission state.
//
// It turns ordered DDL barrier events into source-table admission/release
// transitions, validates those transitions against TargetTableRegistry before a
// conflicting dispatcher can be admitted, and applies the transition after the
// DDL is known to have advanced. Related DDL events are serialized by commit ts.
type Admin struct {
	changefeedID common.ChangeFeedID
	router       Router
	registry     *TargetTableRegistry
	// activeRoutes is the route admission snapshot keyed by logical source
	// schema/table name. Partition DDLs may change physical table IDs, but they
	// do not change this lifecycle unless the logical source route changes.
	activeRoutes map[TableKey]RouteBinding

	// pendingQueue and pendingEvents keep route-affecting DDL transitions in
	// commit order. A later route transition can be prechecked only after every
	// earlier route transition has been applied to the admission snapshot.
	pendingQueue  pendingKeyHeap
	pendingEvents map[pendingKey]*pendingEvent

	// reportError reports unrecoverable route admission errors to the
	// changefeed-level error path. failed suppresses duplicate reports from
	// dispatcher/barrier resends of the same broken state.
	reportError func(error)
	failed      bool
}

type Action int

const (
	Admit Action = iota
	Release
	ReleaseSchema
)

// Admission is one source route transition reported by dispatcher.
type Admission struct {
	Action  Action
	Source  TableKey
	Binding RouteBinding
}

// AdmissionEvent is the route-relevant snapshot derived from one DDL barrier.
type AdmissionEvent struct {
	// CommitTs orders route-affecting DDLs in the same order Barrier uses.
	CommitTs uint64
	// IsSyncPoint distinguishes sync-point barriers from DDL barriers at the same ts.
	IsSyncPoint bool
	// Admissions carries dispatcher-provided source route transitions. DDL type
	// interpretation stays in dispatcher; Admin only consumes route-level
	// admit/release operations.
	Admissions []Admission
}

// pendingEvent is the cached route transition for one DDL event waiting in
// pendingQueue. prechecked means the transition has already passed the registry
// dry-run, so repeated barrier reports do not redo the same validation.
type pendingEvent struct {
	transition *routeTransition
	prechecked bool
}

// routeTransition is the normalized mutation produced from one AdmissionEvent.
// The same transition is used for both precheck and apply so the validated
// state change is not rebuilt differently later.
type routeTransition struct {
	commitTs       uint64
	releases       []TableKey
	releaseSchemas []string
	admits         []RouteBinding
}

// routeAdmissionChange is the registry-level projection of routeTransition:
// source names to release and source-to-target bindings to admit.
type routeAdmissionChange struct {
	releases []TableKey
	admits   []RouteBinding
}

type pendingKey struct {
	commitTs    uint64
	isSyncPoint bool
}

type pendingKeyHeap []pendingKey

func (h pendingKeyHeap) Len() int { return len(h) }

func (h pendingKeyHeap) Less(i, j int) bool {
	return comparePendingKey(h[i], h[j]) < 0
}

func (h pendingKeyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *pendingKeyHeap) Push(x any) {
	*h = append(*h, x.(pendingKey))
}

func (h *pendingKeyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func comparePendingKey(a, b pendingKey) int {
	if n := cmp.Compare(a.commitTs, b.commitTs); n != 0 {
		return n
	}
	if a.isSyncPoint == b.isSyncPoint {
		return 0
	}
	if !a.isSyncPoint {
		return -1
	}
	return 1
}

func eventKey(info AdmissionEvent) pendingKey {
	return pendingKey{commitTs: info.CommitTs, isSyncPoint: info.IsSyncPoint}
}

// NewAdmin creates a table route admin when table route is enabled.
func NewAdmin(
	changefeedID common.ChangeFeedID,
	replicaConfig *config.ReplicaConfig,
	reportError func(error),
	tables []commonEvent.Table,
) (*Admin, error) {
	if replicaConfig == nil || replicaConfig.Sink == nil {
		return nil, nil
	}
	if !replicaConfig.Sink.TableRouteEnabled() {
		return nil, nil
	}

	router, err := NewRouter(
		changefeedID,
		util.GetOrZero(replicaConfig.CaseSensitive),
		replicaConfig.Sink.DispatchRules,
	)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	activeRoutes := make(map[TableKey]RouteBinding, len(tables))
	admin := &Admin{
		changefeedID:  changefeedID,
		router:        router,
		registry:      NewTargetTableRegistry(changefeedID, len(tables)),
		activeRoutes:  activeRoutes,
		pendingEvents: make(map[pendingKey]*pendingEvent),
		reportError:   reportError,
	}

	for _, t := range tables {
		binding, err := admin.router.Route(t.SchemaName, t.TableName)
		if err != nil {
			return nil, err
		}

		if existing, ok := activeRoutes[binding.Source]; ok {
			if !existing.Target.Equal(binding.Target) {
				return nil, errors.ErrInternalCheckFailed.GenWithStack(
					"source `%s`.`%s` maps to multiple targets `%s`.`%s` and `%s`.`%s` during route admin initialization",
					binding.Source.Schema, binding.Source.Table,
					existing.Target.Schema, existing.Target.Table,
					binding.Target.Schema, binding.Target.Table)
			}
			continue
		}
		if err := admin.registry.ApplyTransition(nil, []RouteBinding{binding}, true); err != nil {
			return nil, err
		}
		activeRoutes[binding.Source] = binding
	}

	// todo: this log may can be removed after test huge number of tables.
	log.Info("route admin initialized",
		zap.String("changefeed", changefeedID.Name()),
		zap.Int("tableCount", len(tables)),
		zap.Duration("duration", time.Since(start)))

	return admin, nil
}

// SetErrorReporter updates the error reporter used by Precheck and Apply.
func (a *Admin) SetErrorReporter(reportError func(error)) {
	a.reportError = reportError
}

// Precheck validates a route transition without mutating the route registry.
func (a *Admin) Precheck(info AdmissionEvent) (bool, error) {
	if !a.needsCheck(info) {
		return true, nil
	}
	pending, ok := a.getOrBuildPendingEvent(info)
	if !ok {
		return true, nil
	}
	key := eventKey(info)
	if !a.isPendingHead(key) {
		log.Info("route transition waits for earlier DDL",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Uint64("commitTs", info.CommitTs))
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
		zap.Uint64("commitTs", info.CommitTs),
		zap.Int("releases", len(change.releases)),
		zap.Int("releaseSchemas", len(transition.releaseSchemas)),
		zap.Int("admits", len(change.admits)))
	return true, nil
}

// Apply applies a route transition after the DDL has advanced.
func (a *Admin) Apply(info AdmissionEvent) error {
	if !a.needsCheck(info) {
		return nil
	}
	key := eventKey(info)
	pending, ok := a.pendingEvents[key]
	if !ok {
		pending, ok = a.getOrBuildPendingEvent(info)
		if !ok {
			return nil
		}
	}
	if !a.isPendingHead(key) {
		err := errors.ErrTableRouteConflict.GenWithStack(
			"route transition apply out of order, changefeed=%s, commitTs=%d",
			a.changefeedID.Name(), info.CommitTs)
		return a.fail(err)
	}
	transition := pending.transition
	change, err := a.applyTransition(transition, true)
	if err != nil {
		return a.fail(err)
	}
	a.popPendingHead(key)
	log.Info("route transition applied",
		zap.String("changefeed", a.changefeedID.Name()),
		zap.Uint64("commitTs", info.CommitTs),
		zap.Int("releases", len(change.releases)),
		zap.Int("releaseSchemas", len(transition.releaseSchemas)),
		zap.Int("admits", len(change.admits)))
	return nil
}

func (a *Admin) needsCheck(info AdmissionEvent) bool {
	if a == nil || a.registry == nil || info.IsSyncPoint {
		return false
	}
	return len(info.Admissions) > 0
}

func (a *Admin) getOrBuildPendingEvent(info AdmissionEvent) (*pendingEvent, bool) {
	key := eventKey(info)
	if pending, ok := a.pendingEvents[key]; ok {
		return pending, true
	}
	transition := a.buildTransition(info)
	if transition == nil {
		return nil, false
	}
	pending := &pendingEvent{transition: transition}
	heap.Push(&a.pendingQueue, key)
	a.pendingEvents[key] = pending
	return pending, true
}

func (a *Admin) isPendingHead(key pendingKey) bool {
	return len(a.pendingQueue) > 0 && a.pendingQueue[0] == key
}

func (a *Admin) popPendingHead(key pendingKey) {
	if len(a.pendingQueue) == 0 || a.pendingQueue[0] != key {
		log.Panic("route pending queue head mismatch",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Any("expected", key),
			zap.Any("actual", a.pendingQueue))
	}
	heap.Pop(&a.pendingQueue)
	delete(a.pendingEvents, key)
}

func (a *Admin) buildTransition(info AdmissionEvent) *routeTransition {
	transition := &routeTransition{commitTs: info.CommitTs}
	releaseSet := make(map[TableKey]struct{})
	schemaSet := make(map[string]struct{})
	admitSet := make(map[routeBindingKey]struct{})
	for _, table := range info.Admissions {
		switch table.Action {
		case Admit:
			key := routeBindingKey{source: table.Binding.Source, target: table.Binding.Target}
			if _, ok := admitSet[key]; ok {
				continue
			}
			admitSet[key] = struct{}{}
			transition.admits = append(transition.admits, table.Binding)
		case Release:
			if _, ok := releaseSet[table.Source]; ok {
				continue
			}
			releaseSet[table.Source] = struct{}{}
			transition.releases = append(transition.releases, table.Source)
		case ReleaseSchema:
			if _, ok := schemaSet[table.Source.Schema]; ok {
				continue
			}
			schemaSet[table.Source.Schema] = struct{}{}
			transition.releaseSchemas = append(transition.releaseSchemas, table.Source.Schema)
		default:
		}
	}
	slices.SortFunc(transition.releases, compareTableKey)
	slices.Sort(transition.releaseSchemas)
	slices.SortFunc(transition.admits, compareRouteBinding)
	if len(transition.releases) == 0 &&
		len(transition.releaseSchemas) == 0 &&
		len(transition.admits) == 0 {
		return nil
	}
	return transition
}

func (a *Admin) applyTransition(transition *routeTransition, mutate bool) (routeAdmissionChange, error) {
	change := a.buildAdmissionChange(transition)
	if err := a.registry.ApplyTransition(change.releases, change.admits, mutate); err != nil {
		return routeAdmissionChange{}, err
	}
	if !mutate {
		return change, nil
	}
	for _, source := range change.releases {
		delete(a.activeRoutes, source)
	}
	for _, admit := range change.admits {
		a.activeRoutes[admit.Source] = admit
	}
	return change, nil
}

func (a *Admin) buildAdmissionChange(transition *routeTransition) routeAdmissionChange {
	change := routeAdmissionChange{
		releases: make([]TableKey, 0, len(transition.releases)),
		admits:   append([]RouteBinding(nil), transition.admits...),
	}
	releaseSet := make(map[TableKey]struct{}, len(transition.releases))
	for _, source := range transition.releases {
		if _, ok := releaseSet[source]; ok {
			continue
		}
		releaseSet[source] = struct{}{}
		change.releases = append(change.releases, source)
	}
	for _, schema := range transition.releaseSchemas {
		for source := range a.activeRoutes {
			if source.Schema != schema {
				continue
			}
			if _, ok := releaseSet[source]; ok {
				continue
			}
			releaseSet[source] = struct{}{}
			change.releases = append(change.releases, source)
		}
	}
	slices.SortFunc(change.releases, compareTableKey)
	slices.SortFunc(change.admits, compareRouteBinding)
	return change
}

func compareTableKey(a, b TableKey) int {
	if n := cmp.Compare(a.Schema, b.Schema); n != 0 {
		return n
	}
	return cmp.Compare(a.Table, b.Table)
}

func compareRouteBinding(a, b RouteBinding) int {
	if n := compareTableKey(a.Source, b.Source); n != 0 {
		return n
	}
	return compareTableKey(a.Target, b.Target)
}

func (a *Admin) fail(err error) error {
	if err == nil {
		return nil
	}
	if a.reportError != nil && !a.failed {
		a.failed = true
		a.reportError(err)
	}
	return err
}

type routeBindingKey struct {
	source TableKey
	target TableKey
}
