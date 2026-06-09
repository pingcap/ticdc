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

	// pendingQueue and pendingTransitions keep route-affecting DDL transitions in
	// commit order. A later route transition can be prechecked only after every
	// earlier route transition has been applied to the admission snapshot.
	pendingQueue       []uint64
	pendingTransitions map[uint64]*routeTransition

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

// routeTransition is the normalized mutation produced from one DDL barrier.
// The same transition is used for both precheck and apply so the validated
// state change is not rebuilt differently later.
type routeTransition struct {
	releases       []TableKey
	releaseSchemas []string
	admits         []RouteBinding
	prechecked     bool
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
		changefeedID:       changefeedID,
		router:             router,
		registry:           NewTargetTableRegistry(changefeedID, len(tables)),
		activeRoutes:       activeRoutes,
		pendingTransitions: make(map[uint64]*routeTransition),
		reportError:        reportError,
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
func (a *Admin) Precheck(commitTs uint64, admissions []Admission) (bool, error) {
	if !a.needsCheck(admissions) {
		return true, nil
	}
	transition, ok := a.getOrBuildPendingEvent(commitTs, admissions)
	if !ok {
		return true, nil
	}
	if !a.isPendingHead(commitTs) {
		log.Info("route transition waits for earlier DDL",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Uint64("commitTs", commitTs))
		return false, nil
	}
	if transition.prechecked {
		return true, nil
	}
	releases, admits, err := a.applyTransition(transition, false)
	if err != nil {
		return false, a.fail(err)
	}
	transition.prechecked = true
	log.Info("route transition prechecked",
		zap.String("changefeed", a.changefeedID.Name()),
		zap.Uint64("commitTs", commitTs),
		zap.Int("releases", len(releases)),
		zap.Int("releaseSchemas", len(transition.releaseSchemas)),
		zap.Int("admits", len(admits)))
	return true, nil
}

// Apply applies a route transition after the DDL has advanced.
func (a *Admin) Apply(commitTs uint64, admissions []Admission) error {
	if a == nil || a.registry == nil {
		return nil
	}
	transition, ok := a.pendingTransitions[commitTs]
	if !ok {
		if len(admissions) == 0 {
			return nil
		}
		transition, ok = a.getOrBuildPendingEvent(commitTs, admissions)
		if !ok {
			return nil
		}
	}
	if !a.isPendingHead(commitTs) {
		err := errors.ErrTableRouteConflict.GenWithStack(
			"route transition apply out of order, changefeed=%s, commitTs=%d",
			a.changefeedID.Name(), commitTs)
		return a.fail(err)
	}
	releases, admits, err := a.applyTransition(transition, true)
	if err != nil {
		return a.fail(err)
	}
	a.popPendingHead(commitTs)
	log.Info("route transition applied",
		zap.String("changefeed", a.changefeedID.Name()),
		zap.Uint64("commitTs", commitTs),
		zap.Int("releases", len(releases)),
		zap.Int("releaseSchemas", len(transition.releaseSchemas)),
		zap.Int("admits", len(admits)))
	return nil
}

func (a *Admin) needsCheck(admissions []Admission) bool {
	if a == nil || a.registry == nil {
		return false
	}
	return len(admissions) > 0
}

func (a *Admin) getOrBuildPendingEvent(commitTs uint64, admissions []Admission) (*routeTransition, bool) {
	if transition, ok := a.pendingTransitions[commitTs]; ok {
		return transition, true
	}
	transition := a.buildTransition(admissions)
	if transition == nil {
		return nil, false
	}
	a.pendingQueue = append(a.pendingQueue, commitTs)
	slices.Sort(a.pendingQueue)
	a.pendingTransitions[commitTs] = transition
	return transition, true
}

func (a *Admin) isPendingHead(commitTs uint64) bool {
	return len(a.pendingQueue) > 0 && a.pendingQueue[0] == commitTs
}

func (a *Admin) popPendingHead(commitTs uint64) {
	if len(a.pendingQueue) == 0 || a.pendingQueue[0] != commitTs {
		log.Panic("route pending queue head mismatch",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Uint64("expected", commitTs),
			zap.Any("actual", a.pendingQueue))
	}
	a.pendingQueue = a.pendingQueue[1:]
	delete(a.pendingTransitions, commitTs)
}

func (a *Admin) buildTransition(admissions []Admission) *routeTransition {
	transition := &routeTransition{}
	releaseSet := make(map[TableKey]struct{})
	schemaSet := make(map[string]struct{})
	admitSet := make(map[routeBindingKey]struct{})
	for _, table := range admissions {
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

func (a *Admin) applyTransition(transition *routeTransition, mutate bool) ([]TableKey, []RouteBinding, error) {
	releases, admits := a.buildAdmissionChange(transition)
	if err := a.registry.ApplyTransition(releases, admits, mutate); err != nil {
		return nil, nil, err
	}
	if !mutate {
		return releases, admits, nil
	}
	for _, source := range releases {
		delete(a.activeRoutes, source)
	}
	for _, admit := range admits {
		a.activeRoutes[admit.Source] = admit
	}
	return releases, admits, nil
}

func (a *Admin) buildAdmissionChange(transition *routeTransition) ([]TableKey, []RouteBinding) {
	releases := make([]TableKey, 0, len(transition.releases))
	admits := transition.admits
	releaseSet := make(map[TableKey]struct{}, len(transition.releases))
	for _, source := range transition.releases {
		if _, ok := releaseSet[source]; ok {
			continue
		}
		releaseSet[source] = struct{}{}
		releases = append(releases, source)
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
			releases = append(releases, source)
		}
	}
	slices.SortFunc(releases, compareTableKey)
	slices.SortFunc(admits, compareRouteBinding)
	return releases, admits
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
