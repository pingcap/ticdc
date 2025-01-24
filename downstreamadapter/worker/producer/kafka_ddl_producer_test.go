// Copyright 2025 PingCAP, Inc.
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

package producer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	common "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestDDLSyncBroadcastMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()
	options.MaxMessages = 1

	changefeed := commonType.NewChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(ctx, options, changefeed)
	require.NoError(t, err)
	factory.(*kafka.MockFactory).ErrorReporter = t

	syncProducer, err := factory.SyncProducer()
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	for i := 0; i < kafka.DefaultMockPartitionNum; i++ {
		syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	}
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{})
	require.NoError(t, err)

	p.Close()
	err = p.SyncBroadcastMessage(ctx, kafka.DefaultMockTopicName,
		kafka.DefaultMockPartitionNum, &common.Message{})
	require.ErrorIs(t, err, errors.ErrKafkaProducerClosed)
	cancel()
}

func TestDDLSyncSendMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	options := getOptions()

	changefeed := commonType.NewChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(ctx, options, changefeed)
	require.NoError(t, err)
	factory.(*kafka.MockFactory).ErrorReporter = t

	syncProducer, err := factory.SyncProducer()
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndSucceed()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{})
	require.NoError(t, err)

	p.Close()
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
	cancel()
}

func TestDDLProducerSendMsgFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	options := getOptions()
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	ctx = context.WithValue(ctx, "testing.T", t)

	// This will make the first send failed.
	changefeed := commonType.NewChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(ctx, options, changefeed)
	require.NoError(t, err)
	factory.(*kafka.MockFactory).ErrorReporter = t

	syncProducer, err := factory.SyncProducer()
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)
	defer p.Close()

	syncProducer.(*kafka.MockSaramaSyncProducer).Producer.ExpectSendMessageAndFail(sarama.ErrMessageTooLarge)
	err = p.SyncSendMessage(ctx, kafka.DefaultMockTopicName, 0, &common.Message{})
	require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
}

func TestDDLProducerDoubleClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := getOptions()

	changefeed := commonType.NewChangefeedID4Test("test", "test")
	factory, err := kafka.NewMockFactory(ctx, options, changefeed)
	require.NoError(t, err)
	factory.(*kafka.MockFactory).ErrorReporter = t

	syncProducer, err := factory.SyncProducer()
	require.NoError(t, err)

	p := NewKafkaDDLProducer(ctx, changefeed, syncProducer)

	p.Close()
	p.Close()
}
