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

package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
	"github.com/stretchr/testify/require"
)

func TestReportTransientErrorByChannelState(t *testing.T) {
	dispatcherID := commonType.NewDispatcherID()
	baseErr := &sarama.ProducerError{
		Err: sarama.ErrNetworkException,
		Msg: &sarama.ProducerMessage{
			Topic:     "test-topic",
			Partition: 1,
			Metadata: &messageMetadata{
				recoverInfo: &recoverable.RecoverInfo{
					Dispatchers: []recoverable.DispatcherEpoch{
						{
							DispatcherID: dispatcherID,
							Epoch:        1,
						},
					},
				},
			},
		},
	}

	t.Run("handled true when output channel available", func(t *testing.T) {
		reporter := recoverable.NewReporter(1)
		p := &saramaAsyncProducer{
			changefeedID: commonType.NewChangeFeedIDWithName("test", commonType.DefaultKeyspaceName),
			reporter:     reporter,
		}

		handled := p.reportTransientError(baseErr)
		require.True(t, handled)

		select {
		case event := <-reporter.OutputCh():
			require.Equal(t, []recoverable.DispatcherEpoch{{DispatcherID: dispatcherID, Epoch: 1}}, event.Dispatchers)
		default:
			t.Fatal("expected recoverable event to be sent")
		}
	})

	t.Run("handled false when output channel is full", func(t *testing.T) {
		reporter := recoverable.NewReporter(1)
		_, reported := reporter.Report([]recoverable.DispatcherEpoch{
			{
				DispatcherID: commonType.NewDispatcherID(),
				Epoch:        1,
			},
		})
		require.True(t, reported)
		p := &saramaAsyncProducer{
			changefeedID: commonType.NewChangeFeedIDWithName("test", commonType.DefaultKeyspaceName),
			reporter:     reporter,
		}

		handled := p.reportTransientError(baseErr)
		require.False(t, handled)
		require.Equal(t, 1, len(reporter.OutputCh()))
	})
}
