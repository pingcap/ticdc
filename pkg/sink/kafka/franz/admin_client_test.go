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

package franz

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBrokerConfigControllerNotAvailableUsesDedicatedError(t *testing.T) {
	t.Parallel()

	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	dir := filepath.Dir(currentFile)

	source, err := os.ReadFile(filepath.Join(dir, "admin_client.go"))
	require.NoError(t, err)
	require.NotContains(t, string(source), `ErrKafkaInvalidConfig.GenWithStack("kafka controller is not available")`)
	require.Contains(t, string(source), "ErrKafkaControllerNotAvailable")
}
