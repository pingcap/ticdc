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

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// mockClientForHeartbeat is a mock sarama.Client used to verify
// that the Brokers() method is called as part of the heartbeat logic.
type mockClientForHeartbeat struct {
	sarama.Client // Embed the interface to avoid implementing all methods.
	brokersCalled chan struct{}
}

// Brokers is the mocked method. It sends a signal to a channel when called.
func (c *mockClientForHeartbeat) Brokers() []*sarama.Broker {
	// The function under test iterates over the returned slice.
	// Returning nil is fine, as we only want to check if this method was called.
	c.brokersCalled <- struct{}{}
	return nil
}

// Close closes the signal channel.
func (c *mockClientForHeartbeat) Close() error {
	close(c.brokersCalled)
	return nil
}

func TestSaramaSyncProducerHeartbeatCallsBrokers(t *testing.T) {
	t.Parallel()

	mockClient := &mockClientForHeartbeat{
		brokersCalled: make(chan struct{}, 10),
	}

	producer := &saramaSyncProducer{
		id:     commonType.NewChangeFeedIDWithName("test-sync-producer", "default"),
		client: mockClient,
		closed: atomic.NewBool(false),
	}

	producer.Heartbeat()
	select {
	case <-mockClient.brokersCalled:
	default:
		t.Fatal("Heartbeat should have called client.Brokers(), but it did not")
	}
}

type fakeAsyncProducer struct {
	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
}

func newFakeAsyncProducer() *fakeAsyncProducer {
	return &fakeAsyncProducer{
		input:     make(chan *sarama.ProducerMessage, 16),
		successes: make(chan *sarama.ProducerMessage, 16),
		errors:    make(chan *sarama.ProducerError, 16),
	}
}

func (p *fakeAsyncProducer) AsyncClose()                                 {}
func (p *fakeAsyncProducer) Close() error                                { return nil }
func (p *fakeAsyncProducer) Input() chan<- *sarama.ProducerMessage       { return p.input }
func (p *fakeAsyncProducer) Successes() <-chan *sarama.ProducerMessage   { return p.successes }
func (p *fakeAsyncProducer) Errors() <-chan *sarama.ProducerError        { return p.errors }
func (p *fakeAsyncProducer) IsTransactional() bool                       { return false }
func (p *fakeAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag     { return 0 }
func (p *fakeAsyncProducer) BeginTxn() error                             { return nil }
func (p *fakeAsyncProducer) CommitTxn() error                            { return nil }
func (p *fakeAsyncProducer) AbortTxn() error                             { return nil }
func (p *fakeAsyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}
func (p *fakeAsyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error { return nil }

func TestSaramaAsyncProducerHeartbeatCallsBrokers(t *testing.T) {
	t.Parallel()

	mockClient := &mockClientForHeartbeat{
		brokersCalled: make(chan struct{}, 10),
	}

	producer := &saramaAsyncProducer{
		client:       mockClient,
		producer:     newFakeAsyncProducer(),
		changefeedID: commonType.NewChangeFeedIDWithName("test-async-producer", "default"),
		closed:       atomic.NewBool(false),
		failpointCh:  make(chan *sarama.ProducerError, 1),
	}

	producer.Heartbeat()
	select {
	case <-mockClient.brokersCalled:
	default:
		t.Fatal("Heartbeat should have called client.Brokers(), but it did not")
	}
}

func TestSaramaAsyncProducerRunCallbackOnSuccess(t *testing.T) {
	t.Parallel()

	fakeProducer := newFakeAsyncProducer()
	producer := &saramaAsyncProducer{
		client:       &mockClientForHeartbeat{brokersCalled: make(chan struct{}, 1)},
		producer:     fakeProducer,
		changefeedID: commonType.NewChangeFeedIDWithName("test-async-producer", "default"),
		closed:       atomic.NewBool(false),
		failpointCh:  make(chan *sarama.ProducerError, 1),
	}

	callbackCh := make(chan struct{}, 1)
	fakeProducer.successes <- &sarama.ProducerMessage{
		Metadata: &messageMetadata{
			callback: func() { callbackCh <- struct{}{} },
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- producer.AsyncRunCallback(ctx)
	}()

	select {
	case <-callbackCh:
	case <-time.After(1 * time.Second):
		t.Fatal("expected callback to be invoked from Successes()")
	}

	cancel()
	err := <-errCh
	require.Error(t, err)
}
