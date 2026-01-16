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

package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMsgCopiesSlices(t *testing.T) {
	t.Parallel()
	key := []byte("key")
	value := []byte("value")
	msg := NewMsg(key, value)
	require.Equal(t, []byte("key"), msg.Key)
	require.Equal(t, []byte("value"), msg.Value)

	key[0] = 'x'
	value[0] = 'y'
	require.Equal(t, []byte("key"), msg.Key)
	require.Equal(t, []byte("value"), msg.Value)

	msg = NewMsg(nil, nil)
	require.Nil(t, msg.Key)
	require.Nil(t, msg.Value)
}

func TestMessageRowsCountAndLength(t *testing.T) {
	t.Parallel()

	msg := NewMsg([]byte("k"), []byte("v"))
	require.Equal(t, 0, msg.GetRowsCount())

	msg.IncRowsCount()
	require.Equal(t, 1, msg.GetRowsCount())

	msg.SetRowsCount(42)
	require.Equal(t, 42, msg.GetRowsCount())

	require.Equal(t, len(msg.Key)+len(msg.Value)+MaxRecordOverhead, msg.Length())
}

func TestPartitionKey(t *testing.T) {
	t.Parallel()

	msg := NewMsg(nil, nil)
	require.Equal(t, "", msg.GetPartitionKey())

	msg.SetPartitionKey("partition-1")
	require.Equal(t, "partition-1", msg.GetPartitionKey())
	require.NotNil(t, msg.PartitionKey)
}

func TestUnmarshalClaimCheckMessage(t *testing.T) {
	t.Parallel()

	origin := &ClaimCheckMessage{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	data, err := json.Marshal(origin)
	require.NoError(t, err)

	got, err := UnmarshalClaimCheckMessage(data)
	require.NoError(t, err)
	require.Equal(t, origin.Key, got.Key)
	require.Equal(t, origin.Value, got.Value)

	_, err = UnmarshalClaimCheckMessage([]byte("{"))
	require.Error(t, err)
}
