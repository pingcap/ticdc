// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestSerializedMessagesSize(t *testing.T) {
	t.Parallel()

	msgs := []*common.Message{
		common.NewMsg([]byte("k1"), []byte("value-1")),
		common.NewMsg(nil, []byte("value-2")),
	}
	msgs[0].SetRowsCount(1)
	msgs[1].SetRowsCount(2)

	data, err := serializeMessages(msgs)
	require.NoError(t, err)
	require.Equal(t, serializedMessagesSize(msgs), len(data))
}
