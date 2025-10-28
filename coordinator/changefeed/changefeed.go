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

package changefeed

import (
	"encoding/json"
	"net/url"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Changefeed is a memory present for changefeed info and status
type Changefeed struct {
	ID       common.ChangeFeedID
	info     *atomic.Pointer[config.ChangeFeedInfo]
	sinkType common.SinkType
	isNew    bool // only true when the changefeed is newly created or resumed by overwriteCheckpointTs

	// nodeIDMu protects nodeID
	nodeIDMu sync.Mutex
	nodeID   node.ID

	configBytes []byte
	// it's saved to the backend db
	lastSavedCheckpointTs    *atomic.Uint64
	logCoordinatorResolvedTs *atomic.Uint64
	// the heartbeatpb.MaintainerStatus is read only
	status *atomic.Pointer[heartbeatpb.MaintainerStatus]

	backoff *Backoff
}

// NewChangefeed creates a new changefeed instance
func NewChangefeed(cfID common.ChangeFeedID,
	info *config.ChangeFeedInfo,
	checkpointTs uint64,
	isNew bool,
) *Changefeed {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Panic("unable to parse sink-uri",
			zap.String("url", info.SinkURI), zap.Error(err))
	}
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}

	res := &Changefeed{
		ID:                       cfID,
		info:                     atomic.NewPointer(info),
		configBytes:              bytes,
		lastSavedCheckpointTs:    atomic.NewUint64(checkpointTs),
		logCoordinatorResolvedTs: atomic.NewUint64(checkpointTs),
		sinkType:                 getSinkType(uri.Scheme),
		isNew:                    isNew,
		// Initialize the status
		status: atomic.NewPointer(
			&heartbeatpb.MaintainerStatus{
				ChangefeedID: cfID.ToPB(),
				CheckpointTs: checkpointTs,
				FeedState:    string(info.State),
			}),
		backoff: NewBackoff(cfID, *info.Config.ChangefeedErrorStuckDuration, checkpointTs),
	}
	// Must set retrying to true when the changefeed is in warning state.
	if info.State == config.StateWarning {
		res.backoff.retrying.Store(true)
	}

	log.Info("changefeed instance created",
		zap.String("id", cfID.String()),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.String("state", string(info.State)),
		zap.String("info", info.String()))
	return res
}

func (c *Changefeed) GetInfo() *config.ChangeFeedInfo {
	return c.info.Load()
}

func (c *Changefeed) SetInfo(info *config.ChangeFeedInfo) {
	c.info.Store(info)
}

func (c *Changefeed) StartFinished() {
	c.backoff.StartFinished()
}

func (c *Changefeed) GetNodeID() node.ID {
	c.nodeIDMu.Lock()
	defer c.nodeIDMu.Unlock()
	return c.nodeID
}

func (c *Changefeed) SetNodeID(n node.ID) {
	c.nodeIDMu.Lock()
	defer c.nodeIDMu.Unlock()
	c.nodeID = n
}

func (c *Changefeed) GetID() common.ChangeFeedID {
	return c.ID
}

func (c *Changefeed) GetGroupID() replica.GroupID {
	// currently we only have one scheduler group for changefeed
	return replica.DefaultGroupID
}

func (c *Changefeed) ShouldRun() bool {
	return c.backoff.ShouldRun()
}

// UpdateStatus updates the changefeed status
// It returns true if the status is changed
// It returns false if the status is not changed
// It returns the new state and error if the status is changed
func (c *Changefeed) UpdateStatus(newStatus *heartbeatpb.MaintainerStatus) (bool, config.FeedState, *heartbeatpb.RunningError) {
	old := c.status.Load()
	failpoint.Inject("CoordinatorDontUpdateChangefeedCheckpoint", func() {
		newStatus.CheckpointTs = old.CheckpointTs
	})

	if newStatus != nil && newStatus.CheckpointTs >= old.CheckpointTs {
		c.status.Store(newStatus)
		if old.BootstrapDone != newStatus.BootstrapDone {
			log.Info("Received changefeed status with bootstrapDone",
				zap.Stringer("changefeed", c.ID),
				zap.Bool("bootstrapDone", newStatus.BootstrapDone))
			return true, config.StateNormal, nil
		}

		info := c.GetInfo()
		// the changefeed reaches the targetTs
		if info.TargetTs != 0 && newStatus.CheckpointTs >= info.TargetTs {
			return true, config.StateFinished, nil
		}

		return c.backoff.CheckStatus(newStatus)
	}

	return false, config.StateNormal, nil
}

func (c *Changefeed) GetLogCoordinatorResolvedTs() uint64 {
	return c.logCoordinatorResolvedTs.Load()
}

func (c *Changefeed) SetLogCoordinatorResolvedTs(logCoordinatorResolvedTs uint64) {
	c.logCoordinatorResolvedTs.Store(logCoordinatorResolvedTs)
}

func (c *Changefeed) ForceUpdateStatus(newStatus *heartbeatpb.MaintainerStatus) (bool, config.FeedState, *heartbeatpb.RunningError) {
	c.status.Store(newStatus)
	return c.backoff.CheckStatus(newStatus)
}

func (c *Changefeed) NeedCheckpointTsMessage() bool {
	return c.sinkType == common.KafkaSinkType || c.sinkType == common.CloudStorageSinkType
}

func (c *Changefeed) SetIsNew(isNew bool) {
	c.isNew = isNew
}

// GetStatus returns the changefeed status.
// Note: the returned status is a pointer, so it's not safe to modify it!
func (c *Changefeed) GetStatus() *heartbeatpb.MaintainerStatus {
	return c.status.Load()
}

// GetClonedStatus returns a deep copy of the changefeed status
func (c *Changefeed) GetClonedStatus() *heartbeatpb.MaintainerStatus {
	status := c.status.Load()
	if status == nil {
		return nil
	}

	clone := &heartbeatpb.MaintainerStatus{
		CheckpointTs: status.CheckpointTs,
		FeedState:    status.FeedState,
		State:        status.State,
		Err:          make([]*heartbeatpb.RunningError, 0, len(status.Err)),
	}
	for _, err := range status.Err {
		clonedErr := &heartbeatpb.RunningError{
			Time:    err.Time,
			Node:    err.Node,
			Code:    err.Code,
			Message: err.Message,
		}
		clone.Err = append(clone.Err, clonedErr)
	}

	cfID := status.ChangefeedID
	if cfID != nil {
		clone.ChangefeedID = &heartbeatpb.ChangefeedID{
			High:     cfID.High,
			Low:      cfID.Low,
			Name:     cfID.Name,
			Keyspace: cfID.Keyspace,
		}
	}

	return clone
}

func (c *Changefeed) SetLastSavedCheckPointTs(ts uint64) {
	c.lastSavedCheckpointTs.Store(ts)
}

func (c *Changefeed) GetLastSavedCheckPointTs() uint64 {
	return c.lastSavedCheckpointTs.Load()
}

func (c *Changefeed) NewAddMaintainerMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.AddMaintainerRequest{
			Id:              c.ID.ToPB(),
			CheckpointTs:    c.GetStatus().CheckpointTs,
			Config:          c.configBytes,
			IsNewChangefeed: c.isNew,
		})
}

func (c *Changefeed) NewRemoveMaintainerMessage(server node.ID, casCade, removed bool) *messaging.TargetMessage {
	return RemoveMaintainerMessage(c.ID, server, casCade, removed)
}

func (c *Changefeed) NewCheckpointTsMessage(ts uint64) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(c.GetNodeID(),
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CheckpointTsMessage{
			ChangefeedID: c.ID.ToPB(),
			CheckpointTs: ts,
		})
}

func RemoveMaintainerMessage(id common.ChangeFeedID, server node.ID, casCade bool, removed bool) *messaging.TargetMessage {
	casCade = casCade || removed
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{
			Id:      id.ToPB(),
			Cascade: casCade,
			Removed: removed,
		})
}

// getSinkType returns the sink type of the url.
func getSinkType(scheme string) common.SinkType {
	if config.IsMySQLCompatibleScheme(scheme) {
		return common.MysqlSinkType
	}
	if config.IsMQScheme(scheme) {
		return common.KafkaSinkType
	}
	if config.IsStorageScheme(scheme) {
		return common.CloudStorageSinkType
	}
	return common.BlackHoleSinkType
}
