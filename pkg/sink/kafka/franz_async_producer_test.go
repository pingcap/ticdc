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

func TestAsyncSendClosed(t *testing.T) {
	producer := &asyncProducer{}
	producer.closed.Store(true)

	err := producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{})

	require.ErrorIs(t, err, errors.ErrKafkaSinkClosed)
}

func TestAsyncSendCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	producer := &asyncProducer{}
	require.ErrorIs(t, producer.AsyncSend(ctx, "topic", 0, &codeccommon.Message{}), context.Canceled)
}

func TestAsyncPartition(t *testing.T) {
	const topic = "async-partition"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	defer cluster.Close()
	partition := make(chan int32, 1)
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		partition <- req.(*kmsg.ProduceRequest).Topics[0].Partitions[0].Partition
		return produceResponseWithError(req, -1, 0)
	})
	o := testOptions(cluster.ListenAddrs())
	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-partition"),
		resultCh:     make(chan asyncProduceResult, 1),
	}

	require.NoError(t, producer.AsyncSend(t.Context(), topic, 2, &codeccommon.Message{Value: []byte("value")}))
	require.Eventually(t, func() bool { return client.BufferedProduceRecords() == 0 }, time.Second, time.Millisecond)
	require.Equal(t, int32(2), <-partition)
}

func TestAsyncCallbackStopsWithClient(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-callback-close"),
		resultCh:     make(chan asyncProduceResult, 1),
	}

	done := make(chan error, 1)
	go func() { done <- producer.AsyncRunCallback(context.Background()) }()
	client.Close()

	select {
	case err = <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("callback runner did not stop with the client")
	}
}

func TestFactoryCloseUnblocksPromise(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	require.NoError(t, err)
	factory := &franzFactory{client: client}
	defer factory.Close()

	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-close-full-result-channel"),
		resultCh:     make(chan asyncProduceResult, 1),
	}
	producer.resultCh <- asyncProduceResult{}
	require.NoError(t, producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{}))
	require.Equal(t, int64(1), client.BufferedProduceRecords())

	factory.Close()

	require.Eventually(t, func() bool {
		return client.BufferedProduceRecords() == 0
	}, time.Second, time.Millisecond)
}

func TestAsyncCallbackOnce(t *testing.T) {
	const topic = "async-topic"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()
	o := testOptions(cluster.ListenAddrs())

	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-success"),
		resultCh:     make(chan asyncProduceResult, producerMaxBufferedRecords),
	}
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
	require.Eventually(t, func() bool { return producer.client.BufferedProduceRecords() == 0 }, time.Second, time.Millisecond)
	require.Zero(t, calls.Load())

	callbackCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- producer.AsyncRunCallback(callbackCtx) }()

	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("produce callback was not called")
	}

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, int32(1), calls.Load())
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestAsyncCallbackIsolation(t *testing.T) {
	const topic = "async-callback-isolation"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()
	o := testOptions(cluster.ListenAddrs())

	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-callback-isolation"),
		resultCh:     make(chan asyncProduceResult, producerMaxBufferedRecords),
	}
	defer producer.Close()

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	secondCallback := make(chan struct{})
	require.NoError(t, producer.AsyncSend(context.Background(), topic, 0, &codeccommon.Message{
		Value: []byte("first"),
		Callback: func() {
			close(callbackStarted)
			<-releaseCallback
		},
	}))
	require.NoError(t, producer.AsyncSend(context.Background(), topic, 0, &codeccommon.Message{
		Value:    []byte("second"),
		Callback: func() { close(secondCallback) },
	}))

	callbackCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- producer.AsyncRunCallback(callbackCtx) }()
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("produce callback was not called")
	}
	require.Eventually(t, func() bool { return producer.client.BufferedProduceRecords() == 0 }, time.Second, time.Millisecond)
	select {
	case <-secondCallback:
		t.Fatal("second callback ran before the first callback completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseCallback)
	select {
	case <-secondCallback:
	case <-time.After(time.Second):
		t.Fatal("second callback was not called")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestAsyncProduceFailure(t *testing.T) {
	const topic = "async-error"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		return produceResponseWithError(req, 0, kerr.InvalidTopicException.Code)
	})
	o := testOptions(cluster.ListenAddrs())

	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-error"),
		resultCh:     make(chan asyncProduceResult, producerMaxBufferedRecords),
	}
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
	requireKafkaSendError(t, err, kerr.InvalidTopicException)
	require.False(t, callbackCalled.Load())
}

func TestBufferBackpressure(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.MaxBufferedBytes(10), kgo.RecordRetries(100))
	require.NoError(t, err)

	producer := &asyncProducer{
		client:       client,
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "backpressure"),
		resultCh:     make(chan asyncProduceResult, 2),
	}
	t.Cleanup(client.Close)
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
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled send remained blocked")
	}
}
