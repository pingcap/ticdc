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

package encryption

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

type mockMetaManager struct {
	currentKey    []byte
	currentKeyID  string
	version       byte
	currentKeyErr error
}

func (m *mockMetaManager) IsEncryptionEnabled(ctx context.Context, keyspaceID uint32) bool {
	return true
}

func (m *mockMetaManager) GetCurrentDataKey(ctx context.Context, keyspaceID uint32) ([]byte, string, byte, error) {
	return m.currentKey, m.currentKeyID, m.version, m.currentKeyErr
}

func (m *mockMetaManager) GetDataKey(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, error) {
	return nil, nil
}

func (m *mockMetaManager) Start(ctx context.Context) error { return nil }
func (m *mockMetaManager) Stop()                           {}

func setAllowDegradeOnError(t *testing.T, allow bool) func() {
	t.Helper()
	original := config.GetGlobalServerConfig().Clone()
	updated := original.Clone()
	updated.Debug.Encryption.AllowDegradeOnError = allow
	config.StoreGlobalServerConfig(updated)
	return func() {
		config.StoreGlobalServerConfig(original)
	}
}

func TestEncryptDataAllowDegradeOnError(t *testing.T) {
	restore := setAllowDegradeOnError(t, true)
	defer restore()

	meta := &mockMetaManager{
		currentKeyErr: errors.New("boom"),
	}
	manager := NewEncryptionManager(meta)
	input := []byte("payload")

	output, err := manager.EncryptData(context.Background(), 1, input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestEncryptDataDisallowDegradeOnError(t *testing.T) {
	restore := setAllowDegradeOnError(t, false)
	defer restore()

	meta := &mockMetaManager{
		currentKeyErr: errors.New("boom"),
	}
	manager := NewEncryptionManager(meta)
	_, err := manager.EncryptData(context.Background(), 1, []byte("payload"))
	require.Error(t, err)
}

func TestEncryptDataDisabledSkipsEncryption(t *testing.T) {
	restore := setAllowDegradeOnError(t, false)
	defer restore()

	meta := &mockMetaManager{}
	manager := NewEncryptionManager(meta)
	input := []byte("payload")

	output, err := manager.EncryptData(context.Background(), 1, input)
	require.NoError(t, err)
	require.Equal(t, input, output)
}
