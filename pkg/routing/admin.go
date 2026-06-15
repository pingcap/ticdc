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
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
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
	// lastAppliedCommitTs makes Apply idempotent across barrier resends. DDL
	// commit ts is the ordering fence for route transitions in this admin.
	lastAppliedCommitTs uint64

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

// routeTransition is the normalized admission intent produced from one DDL
// barrier. Precheck and Apply share this intent so resend/retry does not rebuild
// a different set of admit/release actions from dispatcher payloads.
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

		if _, ok := activeRoutes[binding.Source]; ok {
			continue
		}
		if err := admin.registry.ApplyTransition(nil, []RouteBinding{binding}, true); err != nil {
			return nil, err
		}
		activeRoutes[binding.Source] = binding
	}
	return admin, nil
}

// SetErrorReporter updates the error reporter used by Precheck and Apply.
func (a *Admin) SetErrorReporter(reportError func(error)) {
	a.reportError = reportError
}

// Precheck validates a route transition without mutating the route registry.
// false means the caller must not continue yet: either an earlier route
// transition still has to apply, or a conflict has already been reported.
func (a *Admin) Precheck(commitTs uint64, admissions []Admission) bool {
	if a == nil || a.registry == nil || len(admissions) == 0 {
		return true
	}
	if a.hasApplied(commitTs) {
		return true
	}
	transition := a.getOrBuildPendingEvent(commitTs, admissions)
	if len(a.pendingQueue) == 0 || a.pendingQueue[0] != commitTs {
		return false
	}
	if transition.prechecked {
		return true
	}
	err := a.applyTransition(transition, false)
	if err != nil {
		a.fail(err)
		return false
	}
	transition.prechecked = true
	return true
}

// Apply applies a route transition after the DDL has advanced.
func (a *Admin) Apply(commitTs uint64, admissions []Admission) bool {
	if a == nil || a.registry == nil {
		return true
	}
	if a.hasApplied(commitTs) {
		return true
	}
	transition, ok := a.pendingTransitions[commitTs]
	if !ok {
		if len(admissions) == 0 {
			return true
		}
		transition = a.getOrBuildPendingEvent(commitTs, admissions)
	}
	if len(a.pendingQueue) == 0 || a.pendingQueue[0] != commitTs {
		log.Panic("route pending queue head mismatch",
			zap.String("changefeed", a.changefeedID.Name()),
			zap.Uint64("expected", commitTs),
			zap.Any("actual", a.pendingQueue))
	}
	err := a.applyTransition(transition, true)
	if err != nil {
		a.fail(err)
		return false
	}
	a.pendingQueue = a.pendingQueue[1:]
	delete(a.pendingTransitions, commitTs)
	a.lastAppliedCommitTs = commitTs
	return true
}

func (a *Admin) hasApplied(commitTs uint64) bool {
	return a != nil && a.lastAppliedCommitTs >= commitTs
}

func (a *Admin) getOrBuildPendingEvent(commitTs uint64, admissions []Admission) *routeTransition {
	if transition, ok := a.pendingTransitions[commitTs]; ok {
		return transition
	}
	transition := a.buildTransition(admissions)
	a.pendingQueue = append(a.pendingQueue, commitTs)
	// Admission order is commit-ts order, not arrival order. Recovery/resend and
	// live status paths can discover pending transitions from different sources,
	// so keep the smallest unapplied commit ts at the queue head.
	slices.Sort(a.pendingQueue)
	a.pendingTransitions[commitTs] = transition
	return transition
}

func (a *Admin) buildTransition(admissions []Admission) *routeTransition {
	transition := &routeTransition{}
	for _, table := range admissions {
		switch table.Action {
		case Admit:
			transition.admits = append(transition.admits, table.Binding)
		case Release:
			transition.releases = append(transition.releases, table.Source)
		case ReleaseSchema:
			transition.releaseSchemas = append(transition.releaseSchemas, table.Source.Schema)
		default:
		}
	}
	return transition
}

func (a *Admin) applyTransition(transition *routeTransition, mutate bool) error {
	releases, admits := a.buildAdmissionChange(transition)
	if err := a.registry.ApplyTransition(releases, admits, mutate); err != nil {
		return err
	}
	if !mutate {
		return nil
	}
	for _, source := range releases {
		delete(a.activeRoutes, source)
	}
	for _, admit := range admits {
		a.activeRoutes[admit.Source] = admit
	}
	return nil
}

// buildAdmissionChange resolves the transition into release and admit slices
// ready for registry validation. ReleaseSchema actions are expanded against the
// current active route snapshot when the transition is evaluated.
func (a *Admin) buildAdmissionChange(transition *routeTransition) ([]TableKey, []RouteBinding) {
	releases := append([]TableKey(nil), transition.releases...)
	admits := transition.admits
	for _, schema := range transition.releaseSchemas {
		for source := range a.activeRoutes {
			if source.Schema == schema {
				releases = append(releases, source)
			}
		}
	}
	return releases, admits
}

func (a *Admin) fail(err error) {
	if err == nil {
		return
	}
	if a.reportError != nil && !a.failed {
		a.failed = true
		a.reportError(err)
	}
}
