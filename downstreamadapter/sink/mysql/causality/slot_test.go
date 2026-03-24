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

package causality

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSlotByMask(t *testing.T) {
	t.Parallel()

	// numSlots=16 => mask=15
	require.Equal(t, uint64(11), getSlotByMask(11, 15))
	require.Equal(t, uint64(14), getSlotByMask(14, 15))
	require.Equal(t, uint64(3), getSlotByMask(19, 15))
}

func TestGetSlotByModulo(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(5), getSlotByModulo(11, 6))
	require.Equal(t, uint64(2), getSlotByModulo(14, 6))
	require.Equal(t, uint64(1), getSlotByModulo(19, 6))
}

func TestNewSlotIndexFunc(t *testing.T) {
	t.Parallel()

	powerOfTwoSlots := newSlots(16)
	require.Equal(t, uint64(3), powerOfTwoSlots.getSlot(19))
	require.Equal(t, uint64(15), powerOfTwoSlots.getSlot(31))

	nonPowerOfTwoSlots := newSlots(6)
	require.Equal(t, uint64(1), nonPowerOfTwoSlots.getSlot(19))
	require.Equal(t, uint64(5), nonPowerOfTwoSlots.getSlot(11))
}
