// Copyright 2026 PingCAP, Inc.
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
	"encoding/binary"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeserializeMessagesRoundTrip(t *testing.T) {
	t.Parallel()

	msgs := []*common.Message{
		common.NewMsg([]byte("header"), []byte("value-1")),
		common.NewMsg(nil, []byte("value-2")),
	}
	msgs[0].SetRowsCount(1)
	msgs[1].SetRowsCount(2)

	data := serializeMessages(msgs)

	decoded, err := deserializeMessages(data)
	require.NoError(t, err)
	require.Len(t, decoded, 2)
	require.Equal(t, []byte("header"), decoded[0].Key)
	require.Equal(t, []byte("value-1"), decoded[0].Value)
	require.Equal(t, 1, decoded[0].GetRowsCount())
	require.Nil(t, decoded[1].Key)
	require.Equal(t, []byte("value-2"), decoded[1].Value)
	require.Equal(t, 2, decoded[1].GetRowsCount())
}

func TestDeserializeMessagesRejectsImpossibleCount(t *testing.T) {
	t.Parallel()

	// A payload that only contains the batch count cannot possibly hold one full
	// serialized message header, so deserializeMessages should reject it before
	// trying to allocate based on the claimed count.
	data := make([]byte, serializedMessageCountBytes)
	binary.LittleEndian.PutUint32(data, 1)

	decoded, err := deserializeMessages(data)
	require.Nil(t, decoded)
	require.Error(t, err)
	require.True(t, errors.ErrDecodeFailed.Equal(err))
	require.ErrorContains(t, err, "message count")
}
