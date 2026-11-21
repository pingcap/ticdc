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

package factory

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreToDefaultPathUsesPrivatePermissions(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	credentialDir := filepath.Join(homeDir, ".ticdc")
	require.NoError(t, os.Mkdir(credentialDir, 0o755))
	credentialFile := filepath.Join(credentialDir, "credentials")
	require.NoError(t, os.WriteFile(credentialFile, []byte("old"), 0o644))

	auth := &ClientAuth{User: "user", Password: "secret"}
	require.NoError(t, auth.StoreToDefaultPath())

	dirInfo, err := os.Stat(credentialDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	fileInfo, err := os.Stat(credentialFile)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm())

	stored, err := os.ReadFile(credentialFile)
	require.NoError(t, err)
	require.Contains(t, string(stored), `ticdc_password = "secret"`)
}
