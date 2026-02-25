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
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	// recoverableMaxAttempts limits how many times we try dispatcher-level recovery
	// before downgrading to the changefeed-level error path.
	recoverableMaxAttempts = 6
	// recoverableResetInterval resets the restart budget after the dispatcher stays
	// healthy (no recoverable) for a while.
	recoverableResetInterval = 10 * time.Minute
)

type recoverableState struct {
	lastSeen time.Time
	attempts int
}

type recoverableRestartDecision int

const (
	recoverableRestartDecisionRestart recoverableRestartDecision = iota
	recoverableRestartDecisionDowngrade
)

type recoverDispatcherHandler struct {
	maintainer         *Maintainer
	operatorController *operator.Controller
	spanController     *span.Controller
	// tracked tracks dispatcher-level recovery attempts.
	// Access is serialized by maintainer event loop.
	tracked map[common.DispatcherID]recoverableState
}

func newRecoverDispatcherHandler(m *Maintainer) *recoverDispatcherHandler {
	return &recoverDispatcherHandler{
		maintainer: m,
		// Recover dispatcher requests are handled on the default replication mode path.
		// If other modes support this flow in the future, extend here.
		operatorController: m.controller.getOperatorController(common.DefaultMode),
		spanController:     m.controller.getSpanController(common.DefaultMode),
		tracked:            make(map[common.DispatcherID]recoverableState),
	}
}

func (h *recoverDispatcherHandler) handle(source node.ID, req *heartbeatpb.RecoverDispatcherRequest) []*heartbeatpb.RecoverDispatcherResponseEntry {
	if !h.validateRequest(source, req) {
		return nil
	}

	entries := make([]*heartbeatpb.RecoverDispatcherResponseEntry, 0, len(req.Identities))
	seen := make(map[common.DispatcherID]struct{}, len(req.Identities))
	for _, identity := range req.Identities {
		if identity == nil {
			continue
		}
		if identity.DispatcherID == nil {
			continue
		}
		dispatcherID := common.NewDispatcherIDFromPB(identity.DispatcherID)
		if _, ok := seen[dispatcherID]; ok {
			continue
		}
		seen[dispatcherID] = struct{}{}
		state, keepProcessing := h.tryRecoverDispatcher(source, identity, dispatcherID)
		entries = append(entries, &heartbeatpb.RecoverDispatcherResponseEntry{
			Identity: &heartbeatpb.RecoverDispatcherIdentity{
				DispatcherID:    identity.DispatcherID,
				DispatcherEpoch: identity.DispatcherEpoch,
				MaintainerEpoch: identity.MaintainerEpoch,
			},
			State: state,
		})
		if keepProcessing {
			continue
		}
		return entries
	}
	return entries
}

// validateRequest returns true when request validation passes and caller should continue
// processing dispatcher IDs. It returns false when the request should be ignored.
func (h *recoverDispatcherHandler) validateRequest(source node.ID, req *heartbeatpb.RecoverDispatcherRequest) bool {
	// Ignore the request before maintainer bootstrap completes.
	if !h.maintainer.initialized.Load() {
		log.Warn("ignore recover dispatcher request before maintainer initialized",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("sourceNode", source),
			zap.Int("dispatcherCount", len(req.Identities)))
		return false
	}
	if len(req.Identities) == 0 {
		log.Warn("recover dispatcher request has no dispatchers",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("sourceNode", source))
		return false
	}

	log.Warn("recover dispatcher request received, restart dispatchers",
		zap.Stringer("changefeedID", h.maintainer.changefeedID),
		zap.Stringer("sourceNode", source),
		zap.Int("dispatcherCount", len(req.Identities)))
	return true
}

// tryRecoverDispatcher returns response state for this dispatcher and whether caller can keep
// processing remaining dispatchers in the same batch.
func (h *recoverDispatcherHandler) tryRecoverDispatcher(
	source node.ID,
	identity *heartbeatpb.RecoverDispatcherIdentity,
	dispatcherID common.DispatcherID,
) (heartbeatpb.RecoverDispatcherResponseState, bool) {
	if identity.MaintainerEpoch != h.currentMaintainerEpoch() {
		log.Info("ignore recover dispatcher request with stale maintainer epoch",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Uint64("requestMaintainerEpoch", identity.MaintainerEpoch),
			zap.Uint64("currentMaintainerEpoch", h.currentMaintainerEpoch()))
		return heartbeatpb.RecoverDispatcherResponseState_SUPERSEDED, true
	}

	if existing := h.operatorController.GetOperator(dispatcherID); existing != nil {
		if existing.Type() == operator.RestartDispatcherOperatorType {
			log.Info("restart dispatcher operator is already in flight, keep recover request pending",
				zap.Stringer("changefeedID", h.maintainer.changefeedID),
				zap.Stringer("dispatcherID", dispatcherID),
				zap.Stringer("sourceNode", source))
			return heartbeatpb.RecoverDispatcherResponseState_RUNNING, true
		}
		// If a non-restart operator is already in-flight, this recover request is superseded.
		log.Info("ignore recover dispatcher request because non restart operator already exists",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Stringer("sourceNode", source),
			zap.String("operatorType", existing.Type()))
		return heartbeatpb.RecoverDispatcherResponseState_SUPERSEDED, true
	}

	decision, state := h.makeDecision(dispatcherID)
	if decision == recoverableRestartDecisionDowngrade {
		log.Warn("recover dispatcher request exceeded dispatcher restart budget, downgrade to changefeed error path",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Stringer("sourceNode", source),
			zap.Int("restartAttempts", state.attempts))

		h.maintainer.onError(source, &heartbeatpb.RunningError{
			Time: time.Now().String(),
			Code: string(errors.ErrMaintainerRecoverableRestartExceededAttempts.RFCCode()),
			Message: fmt.Sprintf(
				"recover dispatcher request exceeded dispatcher restart budget, downgrade to changefeed error path, dispatcherID=%s, restartAttempts=%d",
				dispatcherID.String(), state.attempts,
			),
		})
		return heartbeatpb.RecoverDispatcherResponseState_FAILED, false
	}

	replication := h.spanController.GetTaskByID(dispatcherID)
	if replication == nil {
		log.Warn("dispatcher not found, ignore recover dispatcher request",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID))
		return heartbeatpb.RecoverDispatcherResponseState_SUPERSEDED, true
	}

	origin := replication.GetNodeID()
	if origin == "" {
		log.Warn("dispatcher has empty node ID, ignore recover dispatcher request",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID))
		return heartbeatpb.RecoverDispatcherResponseState_SUPERSEDED, true
	}

	op := operator.NewRestartDispatcherOperator(h.spanController, replication, origin)
	if ok := h.operatorController.AddOperator(op); !ok {
		existing := h.operatorController.GetOperator(dispatcherID)
		if existing != nil && existing.Type() == operator.RestartDispatcherOperatorType {
			log.Info("restart dispatcher operator already exists, keep recover request pending",
				zap.Stringer("changefeedID", h.maintainer.changefeedID),
				zap.Stringer("dispatcherID", dispatcherID))
			return heartbeatpb.RecoverDispatcherResponseState_RUNNING, true
		}
		log.Info("recover dispatcher request superseded because operator already exists when adding restart operator",
			zap.Stringer("changefeedID", h.maintainer.changefeedID),
			zap.Stringer("dispatcherID", dispatcherID))
		return heartbeatpb.RecoverDispatcherResponseState_SUPERSEDED, true
	}
	state.attempts++
	h.tracked[dispatcherID] = state
	return heartbeatpb.RecoverDispatcherResponseState_ACCEPTED, true
}

func (h *recoverDispatcherHandler) getRestartState(
	dispatcherID common.DispatcherID,
) recoverableState {
	state := h.tracked[dispatcherID]

	now := time.Now()
	if !state.lastSeen.IsZero() && now.Sub(state.lastSeen) >= recoverableResetInterval {
		state = recoverableState{}
	}
	state.lastSeen = now
	h.tracked[dispatcherID] = state
	return state
}

func (h *recoverDispatcherHandler) makeDecision(dispatcherID common.DispatcherID) (recoverableRestartDecision, recoverableState) {
	state := h.getRestartState(dispatcherID)

	if state.attempts >= recoverableMaxAttempts {
		return recoverableRestartDecisionDowngrade, state
	}
	// Intentionally no per-dispatcher time-based skip/backoff here.
	// Recover requests are deduplicated by dispatcher+epoch at sink side.
	// For maintainer, once a request arrives for a new epoch, we should execute it.
	// Same-epoch duplicates are handled by the in-flight operator existence check.
	return recoverableRestartDecisionRestart, state
}

func (h *recoverDispatcherHandler) currentMaintainerEpoch() uint64 {
	if h.maintainer.info == nil {
		return 0
	}
	return h.maintainer.info.Epoch
}
