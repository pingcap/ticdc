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

package avro

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/stretchr/testify/require"
)

func TestDecoderCodecCacheIsBounded(t *testing.T) {
	const schema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"
	codec, err := GenCodec(schema)
	require.NoError(t, err)
	decoder := NewDecoder(nil, 0, nil, "topic", nil).(*decoder)

	var firstID, secondID schemamanager.SchemaID
	for i := 1; i <= decoderCodecCacheSize; i++ {
		schemaID := schemamanager.NewConfluentSchemaID(i)
		decoder.codecs.Add(schemaID, codec)
		switch i {
		case 1:
			firstID = schemaID
		case 2:
			secondID = schemaID
		}
	}

	// Keep the first schema hot, so adding one more schema evicts the second one.
	_, ok := decoder.codecs.Get(firstID)
	require.True(t, ok)
	extraID := schemamanager.NewConfluentSchemaID(decoderCodecCacheSize + 1)
	decoder.codecs.Add(extraID, codec)

	require.Equal(t, decoderCodecCacheSize, decoder.codecs.Len())
	require.True(t, decoder.codecs.Contains(firstID))
	require.False(t, decoder.codecs.Contains(secondID))
}
