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
	stderrors "errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestSyncProducerClosedReturnsProducerClosed(t *testing.T) {
	producer := &kafkaSyncProducer{closed: atomic.NewBool(true)}

	err := producer.SendMessage("topic", 1, &codeccommon.Message{})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)

	err = producer.SendMessages("topic", 1, &codeccommon.Message{})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
}

func TestBuildRecord(t *testing.T) {
	message := &codeccommon.Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	record := buildRecord("topic", 3, message)

	require.Equal(t, "topic", record.Topic)
	require.Equal(t, int32(3), record.Partition)
	require.Equal(t, []byte("key"), record.Key)
	require.Equal(t, []byte("value"), record.Value)
}

func TestSyncProducerWrapSendErrorAnnotatesEventContext(t *testing.T) {
	producer := &kafkaSyncProducer{
		id: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "sync-error"),
	}

	testCases := []struct {
		name     string
		message  *codeccommon.Message
		contains []string
	}{
		{
			name: "ddl",
			message: &codeccommon.Message{LogInfo: &codeccommon.MessageLogInfo{
				DDL: &codeccommon.DDLLogInfo{
					Query:    "create table t(id int primary key)",
					StartTs:  10,
					CommitTs: 20,
				},
			}},
			contains: []string{
				"keyspace=default",
				"changefeed=sync-error",
				"eventType=ddl",
				`ddlQuery="create table t(id int primary key)"`,
				"ddlStartTs=10",
				"ddlCommitTs=20",
			},
		},
		{
			name: "checkpoint",
			message: &codeccommon.Message{LogInfo: &codeccommon.MessageLogInfo{
				Checkpoint: &codeccommon.CheckpointLogInfo{CommitTs: 30},
			}},
			contains: []string{
				"keyspace=default",
				"changefeed=sync-error",
				"eventType=checkpoint",
				"checkpointTs=30",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := producer.wrapSendError(tc.message, stderrors.New("send failed"))
			require.ErrorIs(t, err, cerror.ErrKafkaSendMessage)
			require.ErrorContains(t, err, "send failed")
			for _, expected := range tc.contains {
				require.ErrorContains(t, err, expected)
			}
		})
	}
}
