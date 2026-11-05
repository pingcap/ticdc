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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdvanceChangefeedEpoch(t *testing.T) {
	t.Parallel()

	epoch, err := AdvanceChangefeedEpoch(10, 8)
	require.NoError(t, err)
	require.Equal(t, uint64(10), epoch)

	epoch, err = AdvanceChangefeedEpoch(10, 12)
	require.NoError(t, err)
	require.Equal(t, uint64(13), epoch)

	_, err = AdvanceChangefeedEpoch(10, ^uint64(0))
	require.Error(t, err)
	require.ErrorContains(t, err, "changefeed epoch overflow")
}
