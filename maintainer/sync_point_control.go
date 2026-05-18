package maintainer

import (
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

func (m *Maintainer) syncPointControlEnabled() bool {
	if m == nil || m.info == nil || m.info.Config == nil {
		return false
	}
	return util.GetOrZero(m.info.Config.EnableSyncPoint) && util.GetOrZero(m.info.Config.SyncPointInterval) > 0
}

func (m *Maintainer) nextSyncPointTsAfter(ts uint64) uint64 {
	if !m.syncPointControlEnabled() {
		return 0
	}
	interval := util.GetOrZero(m.info.Config.SyncPointInterval)
	nextTs := syncpoint.CalculateStartSyncPointTs(ts, interval, false)
	if nextTs <= ts {
		nextTime := oracle.GetTimeFromTS(nextTs).Add(interval)
		nextTs = oracle.GoTimeToTS(nextTime)
	}
	return nextTs
}

func (m *Maintainer) newSyncPointControl(skipStartTs, skipEndTs uint64) common.SyncPointControl {
	m.syncPointControlEpoch++
	return common.SyncPointControl{
		Epoch:       m.syncPointControlEpoch,
		SkipStartTs: skipStartTs,
		SkipEndTs:   skipEndTs,
	}
}

func (m *Maintainer) recordSyncPointControlEpoch(control common.SyncPointControl) {
	if control.Epoch > m.syncPointControlEpoch {
		m.syncPointControlEpoch = control.Epoch
	}
}

func (m *Maintainer) setDesiredAndAuthoritativeSyncPointControl(control common.SyncPointControl) {
	m.recordSyncPointControlEpoch(control)
	m.desiredSyncPointControl = control.Clone()
	m.authoritativeSyncPointControl = control.Clone()
}

func (m *Maintainer) shouldSkipSyncPoint(commitTs uint64) bool {
	return m.authoritativeSyncPointControl.Contains(commitTs)
}

func (m *Maintainer) currentSyncPointLag() time.Duration {
	if m == nil {
		return 0
	}
	watermark := m.getWatermark()
	if watermark.CheckpointTs == 0 || watermark.CheckpointTs == math.MaxUint64 {
		return 0
	}
	pdPhysicalTime := oracle.GetPhysical(m.pdClock.CurrentTime())
	phyCheckpointTs := oracle.ExtractPhysical(watermark.CheckpointTs)
	if pdPhysicalTime <= phyCheckpointTs {
		return 0
	}
	return time.Duration(pdPhysicalTime-phyCheckpointTs) * time.Millisecond
}

func (m *Maintainer) allNodesAppliedDesiredSyncPointControl() bool {
	for _, id := range m.bootstrapper.GetAllNodeIDs() {
		applied, ok := m.nodeAppliedSyncPointControl[id]
		if !ok || !applied.Equal(m.desiredSyncPointControl) {
			return false
		}
	}
	return true
}

func (m *Maintainer) reconcileSyncPointControl() {
	if !m.syncPointControlEnabled() || !m.syncPointControlReady {
		return
	}

	nowTs := m.pdClock.CurrentTS()
	lag := m.currentSyncPointLag()
	authoritative := m.authoritativeSyncPointControl
	desired := m.desiredSyncPointControl

	if lag > m.syncPointDirectPassThreshold {
		if !authoritative.IsOpenEnded() {
			startTs := m.nextSyncPointTsAfter(nowTs)
			if startTs == 0 {
				return
			}
			control := m.newSyncPointControl(startTs, 0)
			m.setDesiredAndAuthoritativeSyncPointControl(control)
			log.Info("syncpoint skip window enabled",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Uint64("epoch", control.Epoch),
				zap.Uint64("skipStartTs", control.SkipStartTs))
		}
		return
	}

	if !authoritative.IsOpenEnded() {
		if !authoritative.Disabled() && authoritative.SkipEndTs != 0 && m.getWatermark().CheckpointTs >= authoritative.SkipEndTs {
			control := m.newSyncPointControl(0, 0)
			m.setDesiredAndAuthoritativeSyncPointControl(control)
			log.Info("syncpoint skip window fully cleared",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Uint64("epoch", control.Epoch))
		}
		return
	}

	if lag > m.syncPointDirectPassResume {
		return
	}

	if desired.IsOpenEnded() {
		endTs := m.nextSyncPointTsAfter(nowTs)
		if endTs == 0 || endTs <= authoritative.SkipStartTs {
			return
		}
		control := m.newSyncPointControl(authoritative.SkipStartTs, endTs)
		m.desiredSyncPointControl = control
		log.Info("syncpoint skip window starts resuming",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Uint64("epoch", control.Epoch),
			zap.Uint64("skipStartTs", control.SkipStartTs),
			zap.Uint64("skipEndTs", control.SkipEndTs))
		return
	}

	if desired.SkipEndTs != 0 && desired.SkipEndTs <= nowTs && !m.allNodesAppliedDesiredSyncPointControl() {
		endTs := m.nextSyncPointTsAfter(nowTs)
		if endTs != 0 && endTs > desired.SkipStartTs {
			control := m.newSyncPointControl(desired.SkipStartTs, endTs)
			m.desiredSyncPointControl = control
			log.Info("syncpoint resume boundary reproposed",
				zap.Stringer("changefeedID", m.changefeedID),
				zap.Uint64("epoch", control.Epoch),
				zap.Uint64("skipStartTs", control.SkipStartTs),
				zap.Uint64("skipEndTs", control.SkipEndTs))
		}
		return
	}

	if m.allNodesAppliedDesiredSyncPointControl() && !authoritative.Equal(desired) {
		m.authoritativeSyncPointControl = desired.Clone()
		log.Info("syncpoint authoritative window resumed",
			zap.Stringer("changefeedID", m.changefeedID),
			zap.Uint64("epoch", desired.Epoch),
			zap.Uint64("skipStartTs", desired.SkipStartTs),
			zap.Uint64("skipEndTs", desired.SkipEndTs))
	}
}

func (m *Maintainer) broadcastSyncPointControl() {
	if !m.syncPointControlEnabled() || !m.syncPointControlReady {
		return
	}
	msgs := make([]*messaging.TargetMessage, 0, len(m.bootstrapper.GetAllNodeIDs()))
	control := m.desiredSyncPointControl
	for _, id := range m.bootstrapper.GetAllNodeIDs() {
		msgs = append(msgs, messaging.NewSingleTargetMessage(
			id,
			messaging.HeartbeatCollectorTopic,
			&heartbeatpb.SyncPointControlMessage{
				ChangefeedID: m.changefeedID.ToPB(),
				Control:      control.ToPB(),
			},
		))
	}
	m.sendMessages(msgs)
}

func (m *Maintainer) recoverSyncPointControlFromBootstrapResponses(responses map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) {
	if len(responses) == 0 {
		return
	}

	var (
		hasControl bool
		supported  = true
		openEnded  bool
		minStartTs uint64
		maxEndTs   uint64
		maxEpoch   uint64
	)

	for id, resp := range responses {
		if resp.SyncPointControl == nil {
			supported = false
			continue
		}
		control := common.NewSyncPointControlFromPB(resp.SyncPointControl)
		m.nodeAppliedSyncPointControl[id] = control
		if control.Epoch > maxEpoch {
			maxEpoch = control.Epoch
		}
		if control.Disabled() {
			continue
		}
		if !hasControl || control.SkipStartTs < minStartTs {
			minStartTs = control.SkipStartTs
		}
		if control.IsOpenEnded() {
			openEnded = true
		} else if control.SkipEndTs > maxEndTs {
			maxEndTs = control.SkipEndTs
		}
		hasControl = true
	}

	if maxEpoch > m.syncPointControlEpoch {
		m.syncPointControlEpoch = maxEpoch
	}
	if !supported {
		m.syncPointControlReady = false
		m.desiredSyncPointControl = common.NewDisabledSyncPointControl()
		m.authoritativeSyncPointControl = common.NewDisabledSyncPointControl()
		log.Warn("disable syncpoint skip window because some nodes do not support syncpoint control",
			zap.Stringer("changefeedID", m.changefeedID))
		return
	}
	m.syncPointControlReady = true
	if !hasControl {
		return
	}

	recovered := m.newSyncPointControl(minStartTs, maxEndTs)
	if openEnded {
		recovered.SkipEndTs = 0
	}
	m.setDesiredAndAuthoritativeSyncPointControl(recovered)
	log.Info("recover syncpoint control from bootstrap responses",
		zap.Stringer("changefeedID", m.changefeedID),
		zap.Uint64("epoch", recovered.Epoch),
		zap.Uint64("skipStartTs", recovered.SkipStartTs),
		zap.Uint64("skipEndTs", recovered.SkipEndTs))
}
