// Copyright 2022 PingCAP, Inc.
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

package canal

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONRowEventEncoder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, cfg)
	require.NoError(t, err)

	impl, ok := encoder.(*JSONRowEventEncoder)
	require.True(t, ok)
	require.NotNil(t, impl.config)
}

func TestCanalJSONRowEncoderDecodeBasic(t *testing.T) {
	ctx := context.Background()
	ddlEvent, insertEvent, _, _ := codecCommon.NewLargeEvent4Test(t)

	for _, enableTiDBExtension := range []bool{false, true} {
		cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
		cfg.EnableTiDBExtension = enableTiDBExtension

		encoder, err := NewJSONRowEventEncoder(ctx, cfg)
		require.NoError(t, err)

		decoder, err := NewDecoder(ctx, cfg, nil)
		require.NoError(t, err)

		ddlMsg, err := encoder.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)
		require.NotNil(t, ddlMsg)

		decoder.AddKeyValue(ddlMsg.Key, ddlMsg.Value)
		msgType, ok := decoder.HasNext()
		require.True(t, ok)
		require.Equal(t, codecCommon.MessageTypeDDL, msgType)
		require.NotNil(t, decoder.NextDDLEvent())

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent)
		require.NoError(t, err)

		msgs := encoder.Build()
		require.Len(t, msgs, 1)
		decoder.AddKeyValue(msgs[0].Key, msgs[0].Value)
		msgType, ok = decoder.HasNext()
		require.True(t, ok)
		require.Equal(t, codecCommon.MessageTypeRow, msgType)
		require.NotNil(t, decoder.NextDMLEvent())
	}
}
