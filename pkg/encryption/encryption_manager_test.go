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
	"bytes"
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
	dataKeys      map[string][]byte
}

func (m *mockMetaManager) IsEncryptionEnabled(ctx context.Context, keyspaceID uint32) bool {
	return true
}

func (m *mockMetaManager) GetCurrentDataKey(ctx context.Context, keyspaceID uint32) ([]byte, string, byte, error) {
	return m.currentKey, m.currentKeyID, m.version, m.currentKeyErr
}

func (m *mockMetaManager) GetDataKey(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, error) {
	if m.dataKeys == nil {
		if m.currentKeyID == dataKeyID && len(m.currentKey) > 0 {
			return m.currentKey, nil
		}
		return nil, errors.New("data key not found")
	}
	key, ok := m.dataKeys[dataKeyID]
	if !ok {
		return nil, errors.New("data key not found")
	}
	return key, nil
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

func TestEncryptDecryptRoundTrip(t *testing.T) {
	restore := setAllowDegradeOnError(t, false)
	defer restore()

	key := bytes.Repeat([]byte{0x11}, 32)
	meta := &mockMetaManager{
		currentKey:   key,
		currentKeyID: "K01",
		version:      0x01,
	}
	manager := NewEncryptionManager(meta)

	input := []byte("round-trip-payload")
	encrypted, err := manager.EncryptData(context.Background(), 1, input)
	require.NoError(t, err)
	require.NotEqual(t, input, encrypted)
	require.True(t, IsEncrypted(encrypted))

	decrypted, err := manager.DecryptData(context.Background(), 1, encrypted)
	require.NoError(t, err)
	require.Equal(t, input, decrypted)
}

func TestEncryptDecryptRoundTripWithAES128Key(t *testing.T) {
	restore := setAllowDegradeOnError(t, false)
	defer restore()

	key := bytes.Repeat([]byte{0x22}, 16)
	meta := &mockMetaManager{
		currentKey:   key,
		currentKeyID: "K02",
		version:      0x01,
	}
	manager := NewEncryptionManager(meta)

	input := []byte("round-trip-with-16-byte-key")
	encrypted, err := manager.EncryptData(context.Background(), 1, input)
	require.NoError(t, err)
	require.NotEqual(t, input, encrypted)
	require.True(t, IsEncrypted(encrypted))

	decrypted, err := manager.DecryptData(context.Background(), 1, encrypted)
	require.NoError(t, err)
	require.Equal(t, input, decrypted)
}
