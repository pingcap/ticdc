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
	"context"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAsyncSendClosedProducer(t *testing.T) {
	producer := &asyncProducer{}
	producer.closed.Store(true)

	err := producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{})

	require.ErrorIs(t, err, errors.ErrKafkaSinkClosed)
}

func TestAsyncRunCallbackReturnsQueuedErrorAndCloses(t *testing.T) {
	producer := &asyncProducer{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-callback"),
		errCh:        make(chan error, 1),
	}
	producer.errCh <- errors.New("queued async error")

	err := producer.AsyncRunCallback(context.Background())

	require.ErrorContains(t, err, "queued async error")
	require.True(t, producer.closed.Load())
}

func TestCloseDoesNotAcknowledgeBufferedMessage(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)

	var callbackCalled atomic.Bool
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-close"),
		errCh:        make(chan error, 1),
	}
	err = producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{
		Callback: func() {
			callbackCalled.Store(true)
		},
	})
	require.NoError(t, err)

	producer.Close()

	require.False(t, callbackCalled.Load())
	err = producer.AsyncRunCallback(context.Background())
	require.ErrorIs(t, err, kgo.ErrClientClosed)
}
