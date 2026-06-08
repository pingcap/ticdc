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
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type testSyncProducerClient struct {
	closeCalls int
	closeErr   error
}

func (c *testSyncProducerClient) Brokers() []*sarama.Broker {
	return nil
}

func (c *testSyncProducerClient) Close() error {
	c.closeCalls++
	return c.closeErr
}

type testSaramaSyncProducer struct {
	closeCalls int
}

func (p *testSaramaSyncProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, nil
}

func (p *testSaramaSyncProducer) SendMessages([]*sarama.ProducerMessage) error {
	return nil
}

func (p *testSaramaSyncProducer) Close() error {
	p.closeCalls++
	return nil
}

func TestSaramaSyncProducerCloseClosesClientAndProducer(t *testing.T) {
	client := &testSyncProducerClient{}
	producer := &testSaramaSyncProducer{}
	p := &saramaSyncProducer{
		id:       common.NewChangeFeedIDWithName("test", "default"),
		client:   client,
		producer: producer,
		closed:   atomic.NewBool(false),
	}

	p.Close()

	require.Equal(t, 1, client.closeCalls)
	require.Equal(t, 1, producer.closeCalls)
}

func TestSaramaSyncProducerCloseStillClosesProducerWhenClientCloseFails(t *testing.T) {
	client := &testSyncProducerClient{closeErr: errors.New("boom")}
	producer := &testSaramaSyncProducer{}
	p := &saramaSyncProducer{
		id:       common.NewChangeFeedIDWithName("test", "default"),
		client:   client,
		producer: producer,
		closed:   atomic.NewBool(false),
	}

	p.Close()

	require.Equal(t, 1, client.closeCalls)
	require.Equal(t, 1, producer.closeCalls)
}
