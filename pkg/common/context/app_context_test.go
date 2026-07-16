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

package context

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncryptionManagerServiceName(t *testing.T) {
	GetGlobalContext().serviceMap.Delete(EncryptionManager)
	t.Cleanup(func() {
		GetGlobalContext().serviceMap.Delete(EncryptionManager)
	})

	SetService(EncryptionManager, 42)

	value, ok := TryGetService[int](EncryptionManager)
	require.True(t, ok)
	require.Equal(t, 42, value)
}
