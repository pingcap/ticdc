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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestProducerRejectsSendAfterClose(t *testing.T) {
	t.Parallel()

	message := &codecCommon.Message{}
	syncProducer := &saramaSyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, syncProducer.SendMessage(t.Context(), "topic", 1, message), errors.ErrKafkaSinkClosed)
	require.ErrorIs(t, syncProducer.SendMessages(t.Context(), "topic", 1, message), errors.ErrKafkaSinkClosed)

	asyncProducer := &saramaAsyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, asyncProducer.AsyncSend(context.Background(), "topic", 0, message), errors.ErrKafkaSinkClosed)
}

func TestSyncProducerClose(t *testing.T) {
	tests := []struct {
		name           string
		clientCloseErr error
	}{
		{
			name: "closes client and producer",
		},
		{
			name:           "still closes producer when client close fails",
			clientCloseErr: io.ErrClosedPipe,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := NewMocksaramaSyncClient(ctrl)
			producer := NewMocksaramaSyncProducerClient(ctrl)
			gomock.InOrder(
				client.EXPECT().Close().Return(test.clientCloseErr),
				producer.EXPECT().Close().Return(nil),
			)

			p := &saramaSyncProducer{
				id:       common.NewChangeFeedIDWithName("test", "default"),
				client:   client,
				producer: producer,
				closed:   atomic.NewBool(false),
			}

			p.Close()
		})
	}
}

func TestSyncProducerErrorWrappedOnce(t *testing.T) {
	cause := io.ErrClosedPipe
	tests := []struct {
		name       string
		expectSend func(*MocksaramaSyncProducerClient)
		send       func(*saramaSyncProducer, *codecCommon.Message) error
	}{
		{
			name: "single message",
			expectSend: func(producer *MocksaramaSyncProducerClient) {
				producer.EXPECT().SendMessage(gomock.Any()).Return(int32(0), int64(0), cause)
			},
			send: func(producer *saramaSyncProducer, message *codecCommon.Message) error {
				return producer.SendMessage(t.Context(), "topic", 0, message)
			},
		},
		{
			name: "message batch",
			expectSend: func(producer *MocksaramaSyncProducerClient) {
				producer.EXPECT().SendMessages(gomock.Any()).Return(cause)
			},
			send: func(producer *saramaSyncProducer, message *codecCommon.Message) error {
				return producer.SendMessages(t.Context(), "topic", 1, message)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			producer := NewMocksaramaSyncProducerClient(ctrl)
			test.expectSend(producer)
			p := &saramaSyncProducer{
				id:       common.NewChangeFeedIDWithName("test", "default"),
				producer: producer,
				closed:   atomic.NewBool(false),
			}
			message := &codecCommon.Message{LogInfo: &codecCommon.MessageLogInfo{}}

			err := test.send(p, message)

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
