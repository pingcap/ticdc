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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

func TestSlowMessageFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		message  *messaging.TargetMessage
		hasKeys  []string
		missKeys []string
	}{
		{
			name:    "nil",
			message: nil,
			hasKeys: []string{
				"message",
			},
			missKeys: []string{
				"messageType",
				"statusCount",
			},
		},
		{
			name: "heartbeat",
			message: &messaging.TargetMessage{
				From:  node.ID("node1"),
				To:    node.ID("node2"),
				Topic: messaging.MaintainerManagerTopic,
				Type:  messaging.TypeHeartBeatRequest,
				Message: []messaging.IOTypeT{
					&heartbeatpb.HeartBeatRequest{
						Watermark: &heartbeatpb.Watermark{
							CheckpointTs: 10,
							ResolvedTs:   20,
						},
						RedoWatermark: &heartbeatpb.Watermark{
							CheckpointTs: 30,
							ResolvedTs:   40,
						},
						Statuses: []*heartbeatpb.TableSpanStatus{
							{CheckpointTs: 10},
							{CheckpointTs: 11},
						},
						CompeleteStatus: true,
					},
				},
				Sequence: 1,
			},
			hasKeys: []string{
				"from",
				"to",
				"messageType",
				"topic",
				"messageCount",
				"sequence",
				"statusCount",
				"completeStatus",
				"heartbeatCheckpointTs",
				"heartbeatResolvedTs",
				"redoHeartbeatCheckpointTs",
				"redoHeartbeatResolvedTs",
			},
		},
		{
			name: "non-heartbeat",
			message: &messaging.TargetMessage{
				From:  node.ID("node1"),
				To:    node.ID("node2"),
				Topic: messaging.MaintainerManagerTopic,
				Type:  messaging.TypeMaintainerCloseResponse,
				Message: []messaging.IOTypeT{
					&heartbeatpb.MaintainerCloseResponse{},
				},
			},
			hasKeys: []string{
				"from",
				"to",
				"messageType",
				"topic",
				"messageCount",
			},
			missKeys: []string{
				"statusCount",
				"heartbeatCheckpointTs",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := zapFieldKeys(slowMessageFields(tt.message))
			for _, key := range tt.hasKeys {
				if _, ok := keys[key]; !ok {
					t.Fatalf("slowMessageFields() missing key %q", key)
				}
			}
			for _, key := range tt.missKeys {
				if _, ok := keys[key]; ok {
					t.Fatalf("slowMessageFields() unexpectedly has key %q", key)
				}
			}
		})
	}
}

func zapFieldKeys(fields []zap.Field) map[string]struct{} {
	keys := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		keys[field.Key] = struct{}{}
	}
	return keys
}
