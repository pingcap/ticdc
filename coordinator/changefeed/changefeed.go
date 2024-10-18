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

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

type Changefeed struct {
	ID       model.ChangeFeedID
	Status   *heartbeatpb.MaintainerStatus
	Info     *model.ChangeFeedInfo
	isMQSink bool

	nodeID      node.ID
	configBytes []byte

	lastSavedCheckpointTs uint64
}

func NewChangefeed(cfID model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	checkpointTs uint64) *Changefeed {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}
	bytes, err := json.Marshal(info)
	if err != nil {
		log.Panic("unable to marshal changefeed config",
			zap.Error(err))
	}
	return &Changefeed{
		ID:                    cfID,
		Info:                  info,
		configBytes:           bytes,
		lastSavedCheckpointTs: checkpointTs,
		isMQSink:              sink.IsMQScheme(uri.Scheme),
		// init the first status
		Status: &heartbeatpb.MaintainerStatus{
			CheckpointTs: checkpointTs,
			FeedState:    string(info.State),
		},
	}
}

func (c *Changefeed) SetNodeID(n node.ID) {
	c.nodeID = n
}

func (c *Changefeed) GetNodeID() node.ID {
	return c.nodeID
}

func (c *Changefeed) UpdateStatus(status any) {
	if status != nil {
		newStatus := status.(*heartbeatpb.MaintainerStatus)
		if newStatus.CheckpointTs > c.Status.CheckpointTs {
			c.Status = newStatus
		}
	}
}

func (c *Changefeed) NewAddInferiorMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.AddMaintainerRequest{
			Id:           c.ID.ID,
			CheckpointTs: c.Status.CheckpointTs,
			Config:       c.configBytes,
		})
}

func (c *Changefeed) NewRemoveInferiorMessage(server node.ID, caseCade bool) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.RemoveMaintainerRequest{
			Id:      c.ID.ID,
			Cascade: caseCade,
		})
}

func (c *Changefeed) NewCheckpointTsMessage(ts uint64) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(c.nodeID,
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CheckpointTsMessage{
			ChangefeedID: c.ID.ID,
			CheckpointTs: ts,
		})
}
