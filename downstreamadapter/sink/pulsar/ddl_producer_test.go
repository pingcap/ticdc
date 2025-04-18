// Copyright 2023 PingCAP, Inc.
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

package pulsar

import (
	"context"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// TestPulsarSyncSendMessage is a integration test for pulsar producer
func TestPulsarSyncSendMessage(t *testing.T) {
	// leakutil.VerifyNone(t)

	type args struct {
		ctx          context.Context
		topic        string
		message      *common.Message
		changefeedID commonType.ChangeFeedID
		pulsarConfig *config.PulsarConfig
		errCh        chan error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test SyncSendMessage",
			args: args{
				ctx:          context.Background(),
				topic:        "test",
				changefeedID: commonType.NewChangefeedID4Test("test_namespace", "test"),
				message: &common.Message{
					Value:        []byte("this value for test input data"),
					PartitionKey: str2Pointer("test_key"),
				},
				errCh: make(chan error),
			},
		},
	}
	for _, tt := range tests {
		p := newMockDDLProducer()
		err := p.syncSendMessage(tt.args.ctx, tt.args.topic, tt.args.message)
		require.NoError(t, err)
		require.Len(t, p.(*mockProducer).GetEvents(tt.args.topic), 1)

		p.close()
	}
}

// TestPulsarSyncBroadcastMessage is a integration test for pulsar producer
func TestPulsarSyncBroadcastMessage(t *testing.T) {
	// leakutil.VerifyNone(t)

	type args struct {
		ctx          context.Context
		topic        string
		partition    int32
		message      *common.Message
		changefeedID commonType.ChangeFeedID
		pulsarConfig *config.PulsarConfig
		errCh        chan error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test SyncBroadcastMessage",
			args: args{
				ctx:          context.Background(),
				topic:        "test",
				partition:    1,
				changefeedID: commonType.NewChangefeedID4Test("test_namespace", "test"),
				message: &common.Message{
					Value:        []byte("this value for test input data"),
					PartitionKey: str2Pointer("test_key"),
				},
				errCh: make(chan error),
			},
		},
	}
	for _, tt := range tests {
		p := newMockDDLProducer()
		err := p.syncSendMessage(tt.args.ctx, tt.args.topic, tt.args.message)
		require.NoError(t, err)
		require.Len(t, p.(*mockProducer).GetEvents(tt.args.topic), 1)

		p.close()
	}
}
