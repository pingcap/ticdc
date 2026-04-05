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

package txnutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv"
)

func TestNextScanLockKeyRetriesFullPageWithLiveLocks(t *testing.T) {
	locks := make([]*txnkv.Lock, scanLockLimit)
	for i := range locks {
		locks[i] = &txnkv.Lock{Key: []byte{byte(i % 251)}}
	}

	nextKey, shouldRetry := nextScanLockKey(locks, []byte("z"), 100)

	require.Nil(t, nextKey)
	require.True(t, shouldRetry)
}

func TestNextScanLockKeySkipsPastResolvedTailLock(t *testing.T) {
	locks := make([]*txnkv.Lock, scanLockLimit)
	for i := range locks {
		locks[i] = &txnkv.Lock{Key: []byte{byte(i % 251)}}
	}
	locks[len(locks)-1] = &txnkv.Lock{Key: []byte("tail-lock")}

	nextKey, shouldRetry := nextScanLockKey(locks, []byte("z"), 0)

	require.False(t, shouldRetry)
	require.Equal(t, append([]byte("tail-lock"), 0), nextKey)
}
