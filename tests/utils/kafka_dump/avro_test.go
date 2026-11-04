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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestAvroMessageDecoder(t *testing.T) {
	schema := `{"type":"record","name":"event","fields":[
		{"name":"source","type":{"type":"record","name":"source","fields":[{"name":"table","type":"string"}]}},
		{"name":"op","type":"string"},
		{"name":"id","type":"long"},
		{"name":"data","type":"bytes"},
		{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":65,"scale":30}}
	]}`
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/1" {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	defer server.Close()
	d := &avroMessageDecoder{registryURL: server.URL, client: server.Client(), codecs: make(map[uint32]*goavro.Codec)}
	codec, err := goavro.NewCodec(schema)
	require.NoError(t, err)
	amount, ok := new(big.Rat).SetString("12345678901234567890123456789012345.123456789012345678901234567890")
	require.True(t, ok)
	payload := map[string]any{
		"source": map[string]any{"table": "probe"}, "op": "c",
		"id": int64(9007199254740993), "data": []byte{0xfb, 0xff}, "amount": amount,
	}
	wire, err := codec.BinaryFromNative([]byte{0, 0, 0, 0, 1}, payload)
	require.NoError(t, err)
	for range 2 {
		message, err := d.decode(t.Context(), wire)
		require.NoError(t, err)
		require.Equal(t, payload, message["payload"])
		raw, err := json.Marshal(message)
		require.NoError(t, err)
		require.Equal(t, "probe", tableOf(raw))
		require.Contains(t, string(raw), `"id":9007199254740993`)
		require.Contains(t, string(raw), `"data":"+/8="`)
		require.Contains(t, string(raw), `"amount":"`+amount.RatString()+`"`)
	}
	require.Equal(t, int32(1), requests.Load())
	for _, invalid := range [][]byte{
		nil, {1, 0, 0, 0, 1}, {0, 0, 0, 0, 2}, {0, 0, 0, 0, 1}, append(append([]byte{}, wire...), 0),
	} {
		_, err := d.decode(t.Context(), invalid)
		require.Error(t, err)
	}
}
