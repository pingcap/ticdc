// Copyright 2020 PingCAP, Inc.
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

func TestCanalJSONRowEventEncoderBuildAndCallbacks(t *testing.T) {
	ctx := context.Background()
	_, insertEvent, updateEvent, _ := codecCommon.NewLargeEvent4Test(t)

	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(ctx, cfg)
	require.NoError(t, err)

	// Empty build should return nil.
	require.Nil(t, encoder.Build())

	count := 0
	insertEvent.Callback = func() { count += 1 }
	updateEvent.Callback = func() { count += 2 }

	require.NoError(t, encoder.AppendRowChangedEvent(ctx, "", insertEvent))
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, "", updateEvent))

	msgs := encoder.Build()
	require.Len(t, msgs, 2)

	require.NotNil(t, msgs[0].Callback)
	require.NotNil(t, msgs[1].Callback)
	require.Equal(t, 1, msgs[0].GetRowsCount())
	require.Equal(t, 1, msgs[1].GetRowsCount())

	msgs[0].Callback()
	require.Equal(t, 1, count)
	msgs[1].Callback()
	require.Equal(t, 3, count)

	// Build again should return nil (encoder already drained).
	require.Nil(t, encoder.Build())
}
