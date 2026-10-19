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

package v2

import (
	"bytes"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestDebeziumHandlingModesTOMLRoundTrip(t *testing.T) {
	cfg := DebeziumConfig{
		OutputOldValue:             true,
		DecimalHandlingMode:        util.AddressOf("string"),
		BigintUnsignedHandlingMode: util.AddressOf("string"),
		BinaryHandlingMode:         util.AddressOf("hex"),
	}
	var buf bytes.Buffer
	require.NoError(t, toml.NewEncoder(&buf).Encode(cfg))
	var internal config.DebeziumConfig
	meta, err := toml.Decode(buf.String(), &internal)
	require.NoError(t, err)
	require.Empty(t, meta.Undecoded())
	require.True(t, internal.OutputOldValue)
	require.Equal(t, "string", util.GetOrZero(internal.DecimalHandlingMode))
	require.Equal(t, "string", util.GetOrZero(internal.BigintUnsignedHandlingMode))
	require.Equal(t, "hex", util.GetOrZero(internal.BinaryHandlingMode))
}
