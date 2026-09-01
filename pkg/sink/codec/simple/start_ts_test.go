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

package simple

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestEncodeStartTsInSimpleJSON(t *testing.T) {
	ctx := context.Background()
	cfg := common.NewConfig(config.ProtocolSimple)
	cfg.SimpleIncludeStartTs = true
	enc, err := NewEncoder(cfg, nil)
	require.NoError(t, err)
	dec, err := NewDecoder(ctx, cfg, nil)
	require.NoError(t, err)

	ddlMessage, err := enc.EncodeDDLEvent(common.NewRoutedDDLEvent4Test())
	require.NoError(t, err)
	dec.AddKeyValue(ddlMessage.Key, ddlMessage.Value)
	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)
	require.NotNil(t, dec.NextDDLEvent())

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = 5
	require.NoError(t, enc.AppendRowChangedEvent(ctx, "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)

	var value map[string]any
	require.NoError(t, json.Unmarshal(messages[0].Value, &value))
	require.Equal(t, float64(5), value["startTs"])
	require.Contains(t, value, "commitTs")

	dec.AddKeyValue(messages[0].Key, messages[0].Value)
	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, uint64(5), decoded.GetStartTs())
}

func TestDecodeStartTsFallbackToCommitTs(t *testing.T) {
	ctx := context.Background()
	cfg := common.NewConfig(config.ProtocolSimple)
	enc, err := NewEncoder(cfg, nil)
	require.NoError(t, err)
	dec, err := NewDecoder(ctx, cfg, nil)
	require.NoError(t, err)

	ddlMessage, err := enc.EncodeDDLEvent(common.NewRoutedDDLEvent4Test())
	require.NoError(t, err)
	dec.AddKeyValue(ddlMessage.Key, ddlMessage.Value)
	_, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.NotNil(t, dec.NextDDLEvent())

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = 5
	require.NoError(t, enc.AppendRowChangedEvent(ctx, "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)

	var value map[string]any
	require.NoError(t, json.Unmarshal(messages[0].Value, &value))
	require.NotContains(t, value, "startTs")

	dec.AddKeyValue(messages[0].Key, messages[0].Value)
	_, hasNext = dec.HasNext()
	require.True(t, hasNext)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, decoded.GetCommitTs(), decoded.GetStartTs())
	require.NotEqual(t, uint64(5), decoded.GetStartTs())
}

func TestSimpleStartTsNotInDDLAndWatermark(t *testing.T) {
	ctx := context.Background()
	cfg := common.NewConfig(config.ProtocolSimple)
	cfg.SimpleIncludeStartTs = true
	enc, err := NewEncoder(cfg, nil)
	require.NoError(t, err)

	ddlMessage, err := enc.EncodeDDLEvent(common.NewRoutedDDLEvent4Test())
	require.NoError(t, err)
	require.NotContains(t, string(ddlMessage.Value), `"startTs"`)

	watermark, err := enc.EncodeCheckpointEvent(446266400629063682)
	require.NoError(t, err)
	require.NotContains(t, string(watermark.Value), `"startTs"`)

	dec, err := NewDecoder(ctx, cfg, nil)
	require.NoError(t, err)
	dec.AddKeyValue(watermark.Key, watermark.Value)
	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeResolved, messageType)
	require.Equal(t, uint64(446266400629063682), dec.NextResolvedEvent())
}
