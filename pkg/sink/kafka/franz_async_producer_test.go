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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestAsyncSendClosedProducer(t *testing.T) {
	producer := &asyncProducer{}
	producer.closed.Store(true)

	err := producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{})

	require.ErrorIs(t, err, errors.ErrKafkaSinkClosed)
}

func TestAsyncSendCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	producer := &asyncProducer{}
	require.ErrorIs(t, producer.AsyncSend(ctx, "topic", 0, &codeccommon.Message{}), context.Canceled)
}

func TestAsyncRunCallbackReturnsQueuedError(t *testing.T) {
	producer := &asyncProducer{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-callback"),
		errCh:        make(chan error, 1),
	}
	producer.errCh <- context.DeadlineExceeded

	err := producer.AsyncRunCallback(context.Background())

	require.ErrorIs(t, err, context.DeadlineExceeded)
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

func TestAsyncProducerCallbackExactlyOnce(t *testing.T) {
	const topic = "async-topic"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()
	o := testOptions(cluster.ListenAddrs())

	producer, err := (&franzFactory{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-success"),
		clientOpts:   testClientOptions(t, o),
		producerOpts: producerOptions(o),
	}).AsyncProducer(context.Background())
	require.NoError(t, err)
	defer producer.Close()

	var calls atomic.Int32
	called := make(chan struct{}, 10)
	message := &codeccommon.Message{
		Value: []byte("value"),
		Callback: func() {
			calls.Add(1)
			called <- struct{}{}
		},
	}
	require.NoError(t, producer.AsyncSend(context.Background(), topic, 0, message))

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("produce callback was not called")
	}

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
}

func TestAsyncProducerReportsProduceFailure(t *testing.T) {
	const topic = "async-error"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		return produceResponseWithError(req, 0, kerr.InvalidTopicException.Code)
	})
	o := testOptions(cluster.ListenAddrs())

	producer, err := (&franzFactory{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-error"),
		clientOpts:   testClientOptions(t, o),
		producerOpts: producerOptions(o),
	}).AsyncProducer(context.Background())
	require.NoError(t, err)
	defer producer.Close()

	var callbackCalled atomic.Bool
	message := &codeccommon.Message{
		Value:    []byte("value"),
		Callback: func() { callbackCalled.Store(true) },
	}
	require.NoError(t, producer.AsyncSend(context.Background(), topic, 0, message))

	callbackCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = producer.AsyncRunCallback(callbackCtx)
	require.ErrorIs(t, err, errors.ErrKafkaSendMessage)
	require.ErrorIs(t, err, kerr.InvalidTopicException)
	require.False(t, callbackCalled.Load())
}

func TestBufferBackpressureCanBeCanceled(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.MaxBufferedBytes(10), kgo.RecordRetries(100))
	require.NoError(t, err)

	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "backpressure"),
		errCh:        make(chan error, 1),
	}
	t.Cleanup(producer.Close)

	require.NoError(t, producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{Value: make([]byte, 10)}))
	require.Equal(t, int64(1), client.BufferedProduceRecords())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- producer.AsyncSend(ctx, "topic", 0, &codeccommon.Message{Value: make([]byte, 10)}) }()

	select {
	case <-done:
		t.Fatal("second send did not wait for buffer space")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()

	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("canceled send remained blocked")
	}

	callbackCtx, callbackCancel := context.WithTimeout(context.Background(), time.Second)
	defer callbackCancel()
	require.ErrorIs(t, producer.AsyncRunCallback(callbackCtx), context.Canceled)
}
