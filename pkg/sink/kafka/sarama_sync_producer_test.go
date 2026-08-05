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
	"io"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type syncProducerBackendTestDouble struct {
	messages   []syncProducerMessage
	sendErr    error
	closeErr   error
	closeCalls int
	closeOrder *[]string
}

func (p *syncProducerBackendTestDouble) SendMessage(msg syncProducerMessage) error {
	p.messages = append(p.messages, msg)
	return p.sendErr
}

func (p *syncProducerBackendTestDouble) SendMessages(msgs []syncProducerMessage) error {
	p.messages = append(p.messages, msgs...)
	return p.sendErr
}

func (p *syncProducerBackendTestDouble) Close() error {
	p.closeCalls++
	if p.closeOrder != nil {
		*p.closeOrder = append(*p.closeOrder, "producer")
	}
	return p.closeErr
}

type closeTestDouble struct {
	closeErr   error
	closeCalls int
	closeOrder *[]string
}

func (c *closeTestDouble) Close() error {
	c.closeCalls++
	*c.closeOrder = append(*c.closeOrder, "client")
	return c.closeErr
}

func TestProducerRejectsSendAfterClose(t *testing.T) {
	t.Parallel()

	message := &codecCommon.Message{}
	syncProducer := &saramaSyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, syncProducer.SendMessage("topic", 1, message), errors.ErrKafkaSinkClosed)
	require.ErrorIs(t, syncProducer.SendMessages("topic", 1, message), errors.ErrKafkaSinkClosed)

	asyncProducer := &saramaAsyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, asyncProducer.AsyncSend(context.Background(), "topic", 0, message), errors.ErrKafkaSinkClosed)
}

func TestSyncProducerClose(t *testing.T) {
	tests := []struct {
		name           string
		clientCloseErr error
	}{
		{
			name: "closes client and producer once in order",
		},
		{
			name:           "still closes producer when client close fails",
			clientCloseErr: io.ErrClosedPipe,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			closeOrder := make([]string, 0, 2)
			client := &closeTestDouble{
				closeErr:   test.clientCloseErr,
				closeOrder: &closeOrder,
			}
			backend := &syncProducerBackendTestDouble{closeOrder: &closeOrder}
			producer := &saramaSyncProducer{
				id:       common.NewChangeFeedIDWithName("test", "default"),
				client:   client,
				producer: backend,
				closed:   atomic.NewBool(false),
			}

			producer.Close()
			producer.Close()

			require.Equal(t, []string{"client", "producer"}, closeOrder)
			require.Equal(t, 1, client.closeCalls)
			require.Equal(t, 1, backend.closeCalls)
		})
	}
}

func TestSyncProducerErrorWrappedOnce(t *testing.T) {
	cause := io.ErrClosedPipe
	tests := []struct {
		name string
		send func(*saramaSyncProducer, *codecCommon.Message) error
	}{
		{
			name: "single message",
			send: func(producer *saramaSyncProducer, message *codecCommon.Message) error {
				return producer.SendMessage("topic", 0, message)
			},
		},
		{
			name: "message batch",
			send: func(producer *saramaSyncProducer, message *codecCommon.Message) error {
				return producer.SendMessages("topic", 1, message)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &syncProducerBackendTestDouble{sendErr: cause}
			producer := &saramaSyncProducer{
				id:       common.NewChangeFeedIDWithName("test", "default"),
				producer: backend,
				closed:   atomic.NewBool(false),
			}
			message := &codecCommon.Message{LogInfo: &codecCommon.MessageLogInfo{}}

			err := test.send(producer, message)

			requireKafkaSendError(t, err, cause)
		})
	}
}

func TestAsyncProducerErrorWrappedOnce(t *testing.T) {
	cause := io.ErrClosedPipe
	producer := &saramaAsyncProducer{
		changefeedID: common.NewChangeFeedIDWithName("test", "default"),
	}

	err := producer.handleProducerError(cause, &codecCommon.MessageLogInfo{})

	requireKafkaSendError(t, err, cause)
}

func requireKafkaSendError(t *testing.T, err, cause error) {
	t.Helper()
	require.ErrorIs(t, err, errors.ErrKafkaSendMessage)
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, strings.Count(err.Error(), string(errors.ErrKafkaSendMessage.RFCCode())))
	require.NotContains(t, err.Error(), "keyspace=test")
}
