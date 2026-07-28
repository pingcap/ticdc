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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestProducerRejectsSendAfterClose(t *testing.T) {
	t.Parallel()

	message := &common.Message{}
	syncProducer := &saramaSyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, syncProducer.SendMessage("topic", 1, message), cerror.ErrKafkaSinkClosed)
	require.ErrorIs(t, syncProducer.SendMessages("topic", 1, message), cerror.ErrKafkaSinkClosed)

	asyncProducer := &saramaAsyncProducer{closed: atomic.NewBool(true)}
	require.ErrorIs(t, asyncProducer.AsyncSend(context.Background(), "topic", 0, message), cerror.ErrKafkaSinkClosed)
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
			clientCloseErr: errors.New("boom"),
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
				id:       commonType.NewChangeFeedIDWithName("test", "default"),
				client:   client,
				producer: producer,
				closed:   atomic.NewBool(false),
			}

			p.Close()
		})
	}
}
