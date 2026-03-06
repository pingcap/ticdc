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
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestToSaramaHeaders(t *testing.T) {
	t.Parallel()

	headers := []common.MessageHeader{
		{Key: "Id", Value: []byte("123")},
		{Key: "event_type", Value: []byte("created")},
	}

	saramaHeaders := toSaramaHeaders(headers)
	require.Len(t, saramaHeaders, 2)
	require.Equal(t, "Id", string(saramaHeaders[0].Key))
	require.Equal(t, "123", string(saramaHeaders[0].Value))
	require.Equal(t, "event_type", string(saramaHeaders[1].Key))
	require.Equal(t, "created", string(saramaHeaders[1].Value))

	require.Nil(t, toSaramaHeaders(nil))
}
