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
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestAsyncSendClosedProducer(t *testing.T) {
	producer := &kafkaAsyncProducer{closed: atomic.NewBool(true)}

	err := producer.AsyncSend(context.Background(), "topic", 0, &codeccommon.Message{})

	require.ErrorIs(t, err, errors.ErrKafkaProducerClosed)
}

func TestAsyncRunCallbackReturnsQueuedErrorAndCloses(t *testing.T) {
	producer := &kafkaAsyncProducer{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-callback"),
		closed:       atomic.NewBool(false),
		errCh:        make(chan error, 1),
	}
	producer.errCh <- errors.New("queued async error")

	err := producer.AsyncRunCallback(context.Background())

	require.ErrorContains(t, err, "queued async error")
	require.True(t, producer.closed.Load())
}

func TestAsyncSendLegacyFailpointAnnotatesDMLContext(t *testing.T) {
	enableLegacyKafkaSinkFailpointForTest(t, kafkaSinkAsyncSendErrorFailpoint)

	producer := &kafkaAsyncProducer{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "async-legacy-failpoint"),
		closed:       atomic.NewBool(false),
		errCh:        make(chan error, 1),
	}
	message := &codeccommon.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
		LogInfo: &codeccommon.MessageLogInfo{Rows: []codeccommon.RowLogInfo{
			{
				Type:     "insert",
				Database: "db",
				Table:    "t",
				StartTs:  1,
				CommitTs: 2,
				PrimaryKeys: []codeccommon.ColumnLogInfo{
					{Name: "id", Value: 1},
				},
			},
		}},
	}

	err := producer.AsyncSend(context.Background(), "topic", 0, message)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = producer.AsyncRunCallback(ctx)

	require.ErrorContains(t, err, "kafka sink injected error")
	require.ErrorContains(t, err, "keyspace=default")
	require.ErrorContains(t, err, "changefeed=async-legacy-failpoint")
	require.ErrorContains(t, err, "eventType=dml")
	require.ErrorContains(t, err, `"Table":"t"`)
}
