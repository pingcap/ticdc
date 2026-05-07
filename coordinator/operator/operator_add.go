// Copyright 2024 PingCAP, Inc.
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

package operator

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// None indicates the operator is not canceled and should keep scheduling.
	None int32 = iota
	// NodeRemoved indicates the destination node went offline before the operator finished.
	NodeRemoved
	// TaskRemoved indicates the changefeed task was removed while the operator was running.
	TaskRemoved
)

// AddMaintainerOperator schedules a maintainer to a target node for a changefeed.
//
// Note: OperatorController can call Schedule/Check/OnNodeRemove concurrently under a
// shared read lock, so the operator's internal state must be concurrency-safe.
type AddMaintainerOperator struct {
	cf       *changefeed.Changefeed
	dest     node.ID
	finished atomic.Bool
	// canceled records why the operator stops scheduling.
	// It must be atomic because Schedule can run concurrently with OnNodeRemove/OnTaskRemoved.
	canceled atomic.Int32
	db       *changefeed.ChangefeedDB
}

// NewAddMaintainerOperator creates an AddMaintainerOperator to schedule the given changefeed to dest.
func NewAddMaintainerOperator(
	db *changefeed.ChangefeedDB,
	cf *changefeed.Changefeed,
	dest node.ID,
) *AddMaintainerOperator {
	return &AddMaintainerOperator{
		cf:   cf,
		dest: dest,
		db:   db,
	}
}

// Check observes maintainer status updates and marks the operator finished once the maintainer
// is working and bootstrap is done on the destination node.
func (m *AddMaintainerOperator) Check(from node.ID, status *heartbeatpb.MaintainerStatus) {
	if status == nil {
		return
	}

	// Require bootstrap to be done before considering the maintainer successfully started.
	// This avoids false positives when a removal-only maintainer reports Working.
	if !m.finished.Load() && from == m.dest &&
		status.State == heartbeatpb.ComponentState_Working &&
		status.BootstrapDone {
		log.Info("maintainer report working status",
			zap.String("changefeed", m.cf.ID.String()))
		m.finished.Store(true)
	}
}

// Schedule builds the "add maintainer" command message, or returns nil when finished/canceled.
func (m *AddMaintainerOperator) Schedule() *messaging.TargetMessage {
	if m.finished.Load() || m.canceled.Load() != None {
		return nil
	}
	return m.cf.NewAddMaintainerMessage(m.dest)
}

// OnNodeRemove cancels the operator when the destination node goes offline.
//
// Once canceled, PostFinish will mark the maintainer as absent so it can be rescheduled.
func (m *AddMaintainerOperator) OnNodeRemove(n node.ID) {
	if n == m.dest {
		m.finished.Store(true)
		m.canceled.Store(NodeRemoved)
	}
}

// AffectedNodes returns the nodes that may be touched by this operator.
func (m *AddMaintainerOperator) AffectedNodes() []node.ID {
	return []node.ID{m.dest}
}

// ID returns the changefeed ID this operator works on.
func (m *AddMaintainerOperator) ID() common.ChangeFeedID {
	return m.cf.ID
}

// IsFinished reports whether the operator has finished scheduling.
func (m *AddMaintainerOperator) IsFinished() bool {
	return m.finished.Load()
}

// OnTaskRemoved cancels the operator when the changefeed task is removed.
func (m *AddMaintainerOperator) OnTaskRemoved() {
	m.finished.Store(true)
	m.canceled.Store(TaskRemoved)
}

// Start binds the changefeed to the destination node before the first scheduling attempt.
func (m *AddMaintainerOperator) Start() {
	m.db.BindChangefeedToNode("", m.dest, m.cf)
}

// PostFinish applies side effects after the operator is removed from the controller.
func (m *AddMaintainerOperator) PostFinish() {
	switch m.canceled.Load() {
	case None:
		m.db.MarkMaintainerReplicating(m.cf)
		m.cf.SetIsNew(false)
	case NodeRemoved:
		m.db.MarkMaintainerAbsent(m.cf)
	}
	// TaskRemoved only happens when the changefeed is removed, so we don't need to do anything.
}

// String returns a human-readable description of the operator, for logging.
func (m *AddMaintainerOperator) String() string {
	return fmt.Sprintf("add maintainer operator: %s, dest:%s",
		m.cf.ID, m.dest)
}

// Type returns the operator type used by metrics/logging.
func (m *AddMaintainerOperator) Type() string {
	return "add"
}

// BlockTsForward indicates whether this operator blocks changefeed checkpoint forwarding.
func (m *AddMaintainerOperator) BlockTsForward() bool {
	log.Panic("unreachable code")
	return false
}
