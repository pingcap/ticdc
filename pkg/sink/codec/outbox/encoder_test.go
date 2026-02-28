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

package outbox

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestEncodeInsertRow(t *testing.T) {
	ddl, insertEvent, _, _ := codecCommon.NewLargeEvent4Test(t)
	_ = ddl
	encoder, err := NewEncoder(context.Background(), &codecCommon.Config{
		Protocol:          config.ProtocolOutboxJSON,
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		OutboxIDColumn:    "tu1",
		OutboxKeyColumn:   "tu2",
		OutboxValueColumn: "varcharT",
		OutboxHeaderColumns: map[string]string{
			"event_type": "charT",
			"event_date": "dateT",
		},
	})
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(context.Background(), "", insertEvent)
	require.NoError(t, err)

	msgs := encoder.Build()
	require.Len(t, msgs, 1)
	require.NotEmpty(t, msgs[0].Key)
	require.NotEmpty(t, msgs[0].Value)
	require.Len(t, msgs[0].Headers, 3)
	require.Equal(t, "Id", msgs[0].Headers[0].Key)
	require.Equal(t, "event_date", msgs[0].Headers[1].Key)
	require.Equal(t, "event_type", msgs[0].Headers[2].Key)
}

func TestSkipNonInsertRows(t *testing.T) {
	_, _, updateEvent, deleteEvent := codecCommon.NewLargeEvent4Test(t)
	encoder, err := NewEncoder(context.Background(), &codecCommon.Config{
		Protocol:          config.ProtocolOutboxJSON,
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		OutboxIDColumn:    "tu1",
		OutboxKeyColumn:   "tu2",
		OutboxValueColumn: "varcharT",
	})
	require.NoError(t, err)

	require.NoError(t, encoder.AppendRowChangedEvent(context.Background(), "", updateEvent))
	require.NoError(t, encoder.AppendRowChangedEvent(context.Background(), "", deleteEvent))
	require.Nil(t, encoder.Build())
}

func TestRequiredColumnsValidation(t *testing.T) {
	_, insertEvent, _, _ := codecCommon.NewLargeEvent4Test(t)
	testCases := []struct {
		name            string
		idColumn        string
		keyColumn       string
		valueColumn     string
		expectedErrPart string
	}{
		{
			name:            "missing column",
			idColumn:        "missing_id",
			keyColumn:       "tu2",
			valueColumn:     "varcharT",
			expectedErrPart: "not found",
		},
		{
			name:            "null column",
			idColumn:        "tu4",
			keyColumn:       "tu2",
			valueColumn:     "varcharT",
			expectedErrPart: "is null",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			encoder, err := NewEncoder(context.Background(), &codecCommon.Config{
				Protocol:          config.ProtocolOutboxJSON,
				MaxMessageBytes:   config.DefaultMaxMessageBytes,
				OutboxIDColumn:    tc.idColumn,
				OutboxKeyColumn:   tc.keyColumn,
				OutboxValueColumn: tc.valueColumn,
			})
			require.NoError(t, err)

			err = encoder.AppendRowChangedEvent(context.Background(), "", insertEvent)
			require.ErrorContains(t, err, tc.expectedErrPart)
		})
	}
}

func TestEncodeCheckpointAndDDL(t *testing.T) {
	ddl, _, _, _ := codecCommon.NewLargeEvent4Test(t)
	encoder, err := NewEncoder(context.Background(), &codecCommon.Config{
		Protocol:          config.ProtocolOutboxJSON,
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		OutboxIDColumn:    "tu1",
		OutboxKeyColumn:   "tu2",
		OutboxValueColumn: "varcharT",
	})
	require.NoError(t, err)

	// outbox-json topics carry only INSERT payloads; checkpoint and DDL events
	// are intentionally suppressed (nil return signals the sink to skip them).
	checkpointMsg, err := encoder.EncodeCheckpointEvent(12345)
	require.NoError(t, err)
	require.Nil(t, checkpointMsg)

	ddlMsg, err := encoder.EncodeDDLEvent(ddl)
	require.NoError(t, err)
	require.Nil(t, ddlMsg)
}
