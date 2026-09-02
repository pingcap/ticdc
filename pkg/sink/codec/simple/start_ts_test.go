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
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// 18-digit PD TSO above the float64 mantissa (2^53). json.Unmarshal into
// map[string]any would round this; UseNumber + ParseUint must keep every digit.
const (
	testStartTs  uint64 = 468651996400058379
	testCommitTs uint64 = 468651996400058400
)

func decodeSimpleJSON(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value map[string]any
	require.NoError(t, dec.Decode(&value))
	return value
}

func requireStartTs(t *testing.T, raw []byte, expected uint64) {
	t.Helper()
	value := decodeSimpleJSON(t, raw)
	num, ok := value["startTs"].(json.Number)
	require.True(t, ok, "startTs missing or not a JSON number: %v", value["startTs"])
	got, err := strconv.ParseUint(num.String(), 10, 64)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	require.Equal(t, strconv.FormatUint(expected, 10), num.String())
	require.Contains(t, string(raw), `"startTs":`+strconv.FormatUint(expected, 10))
}

func requireNoStartTs(t *testing.T, raw []byte) {
	t.Helper()
	value := decodeSimpleJSON(t, raw)
	_, exists := value["startTs"]
	require.False(t, exists, "startTs must be omitted, got %v", value["startTs"])
}

func newEncoderDecoder(t *testing.T, includeStartTs bool) (*Encoder, *Decoder) {
	t.Helper()
	ctx := context.Background()
	cfg := common.NewConfig(config.ProtocolSimple)
	cfg.SimpleIncludeStartTs = includeStartTs
	enc, err := NewEncoder(cfg, nil)
	require.NoError(t, err)
	dec, err := NewDecoder(ctx, cfg, nil)
	require.NoError(t, err)
	return enc.(*Encoder), dec.(*Decoder)
}

func registerTable(t *testing.T, enc *Encoder, dec *Decoder) {
	t.Helper()
	ddlMessage, err := enc.EncodeDDLEvent(common.NewRoutedDDLEvent4Test())
	require.NoError(t, err)
	requireNoStartTs(t, ddlMessage.Value)
	dec.AddKeyValue(ddlMessage.Key, ddlMessage.Value)
	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)
	require.NotNil(t, dec.NextDDLEvent())
}

func TestEncodeStartTsInSimpleJSON(t *testing.T) {
	enc, dec := newEncoderDecoder(t, true)
	registerTable(t, enc, dec)

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = testStartTs
	rowEvent.CommitTs = testCommitTs
	require.NoError(t, enc.AppendRowChangedEvent(context.Background(), "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)
	requireStartTs(t, messages[0].Value, testStartTs)

	value := decodeSimpleJSON(t, messages[0].Value)
	commit, ok := value["commitTs"].(json.Number)
	require.True(t, ok)
	gotCommit, err := strconv.ParseUint(commit.String(), 10, 64)
	require.NoError(t, err)
	require.Equal(t, testCommitTs, gotCommit)

	dec.AddKeyValue(messages[0].Key, messages[0].Value)
	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, testStartTs, decoded.GetStartTs())
	require.Equal(t, testCommitTs, decoded.GetCommitTs())
}

func TestEncodeStartTsOnInsertUpdateDelete(t *testing.T) {
	enc, _ := newEncoderDecoder(t, true)

	insert := common.NewRoutedRowEvent4Test()
	insert.StartTs = testStartTs
	insert.CommitTs = testCommitTs

	update := *insert
	update.Event.PreRow = insert.Event.Row

	del := *insert
	del.Event.PreRow = insert.Event.Row
	del.Event.Row = chunk.Row{}

	require.True(t, insert.IsInsert())
	require.True(t, update.IsUpdate())
	require.True(t, del.IsDelete())

	ctx := context.Background()
	require.NoError(t, enc.AppendRowChangedEvent(ctx, "", insert))
	require.NoError(t, enc.AppendRowChangedEvent(ctx, "", &update))
	require.NoError(t, enc.AppendRowChangedEvent(ctx, "", &del))
	messages := enc.Build()
	require.Len(t, messages, 3)
	for _, msg := range messages {
		requireStartTs(t, msg.Value, testStartTs)
	}

	types := make([]string, 0, 3)
	for _, msg := range messages {
		value := decodeSimpleJSON(t, msg.Value)
		types = append(types, value["type"].(string))
	}
	require.ElementsMatch(t, []string{
		string(DMLTypeInsert),
		string(DMLTypeUpdate),
		string(DMLTypeDelete),
	}, types)
}

func TestDecodeStartTsFallbackToCommitTs(t *testing.T) {
	enc, dec := newEncoderDecoder(t, false)
	registerTable(t, enc, dec)

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = testStartTs
	rowEvent.CommitTs = testCommitTs
	require.NoError(t, enc.AppendRowChangedEvent(context.Background(), "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)
	requireNoStartTs(t, messages[0].Value)

	dec.AddKeyValue(messages[0].Key, messages[0].Value)
	_, hasNext := dec.HasNext()
	require.True(t, hasNext)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, decoded.GetCommitTs(), decoded.GetStartTs())
	require.Equal(t, testCommitTs, decoded.GetStartTs())
	require.NotEqual(t, testStartTs, decoded.GetStartTs())
}

func TestDecodeZeroStartTsFallbackToCommitTs(t *testing.T) {
	enc, dec := newEncoderDecoder(t, true)
	registerTable(t, enc, dec)

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = 0
	rowEvent.CommitTs = testCommitTs
	require.NoError(t, enc.AppendRowChangedEvent(context.Background(), "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)
	// omitempty drops a zero startTs, so the wire format matches the pre-feature
	// message and the decoder must fall back to commitTs.
	requireNoStartTs(t, messages[0].Value)

	dec.AddKeyValue(messages[0].Key, messages[0].Value)
	_, hasNext := dec.HasNext()
	require.True(t, hasNext)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, testCommitTs, decoded.GetStartTs())
}

func TestDecodeExplicitZeroStartTsFallbackToCommitTs(t *testing.T) {
	enc, dec := newEncoderDecoder(t, true)
	registerTable(t, enc, dec)

	rowEvent := common.NewRoutedRowEvent4Test()
	rowEvent.StartTs = testStartTs
	rowEvent.CommitTs = testCommitTs
	require.NoError(t, enc.AppendRowChangedEvent(context.Background(), "", rowEvent))
	messages := enc.Build()
	require.Len(t, messages, 1)

	patched := bytes.Replace(
		messages[0].Value,
		[]byte(`"startTs":`+strconv.FormatUint(testStartTs, 10)),
		[]byte(`"startTs":0`),
		1,
	)
	require.NotEqual(t, messages[0].Value, patched)

	dec.AddKeyValue(messages[0].Key, patched)
	_, hasNext := dec.HasNext()
	require.True(t, hasNext)
	decoded := dec.NextDMLMessage().ToDMLEvent()
	require.Equal(t, testCommitTs, decoded.GetStartTs())
}

func TestSimpleStartTsNotInDDLWatermarkOrBootstrap(t *testing.T) {
	enc, dec := newEncoderDecoder(t, true)

	ddlMessage, err := enc.EncodeDDLEvent(common.NewRoutedDDLEvent4Test())
	require.NoError(t, err)
	requireNoStartTs(t, ddlMessage.Value)

	bootstrap := common.NewRoutedDDLEvent4Test()
	bootstrap.IsBootstrap = true
	bootstrapMessage, err := enc.EncodeDDLEvent(bootstrap)
	require.NoError(t, err)
	requireNoStartTs(t, bootstrapMessage.Value)
	value := decodeSimpleJSON(t, bootstrapMessage.Value)
	require.Equal(t, string(MessageTypeBootstrap), value["type"].(string))

	watermark, err := enc.EncodeCheckpointEvent(testCommitTs)
	require.NoError(t, err)
	requireNoStartTs(t, watermark.Value)

	dec.AddKeyValue(watermark.Key, watermark.Value)
	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeResolved, messageType)
	require.Equal(t, testCommitTs, dec.NextResolvedEvent())
}
