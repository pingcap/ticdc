// Copyright 2025 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestNotReusableEvent(t *testing.T) {
	// 1. Test new event
	dispatcherID := common.NewDispatcherID()
	event := NewNotReusableEvent(dispatcherID)
	require.Equal(t, event.GetType(), TypeNotReusableEvent)
	require.Equal(t, event.GetDispatcherID(), dispatcherID)

	// 2. Test encode and decode
	data, err := event.encode()
	require.NoError(t, err)
	reverseEvent := NotReusableEvent{}
	err = reverseEvent.decode(data)
	require.NoError(t, err)
	require.Equal(t, event, reverseEvent)

	// 3. Test Marshal and Unmarshal
	data, err = event.Marshal()
	require.NoError(t, err)
	reverseEvent = NotReusableEvent{}
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, event, reverseEvent)

	// 4. Test Other methods
	require.Equal(t, event.GetSize(), uint64(len(data)))
	require.Equal(t, event.GetDispatcherID(), dispatcherID)
	require.Equal(t, event.GetCommitTs(), common.Ts(0))
	require.Equal(t, event.GetStartTs(), common.Ts(0))
	require.Equal(t, event.IsPaused(), false)
}
