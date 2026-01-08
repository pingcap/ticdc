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
	"crypto/aes"
	"crypto/cipher"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/encryption/kms"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// EncryptionMetaManager manages encryption metadata for keyspaces
type EncryptionMetaManager interface {
	// IsEncryptionEnabled checks if encryption is enabled for a keyspace
	IsEncryptionEnabled(ctx context.Context, keyspaceID uint32) bool

	// GetCurrentDataKey gets the current data key for a keyspace
	GetCurrentDataKey(ctx context.Context, keyspaceID uint32) ([]byte, EncryptionAlgorithm, error)

	// GetDataKeyWithAlgorithm gets a data key by ID with its encryption algorithm
	GetDataKeyWithAlgorithm(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, EncryptionAlgorithm, error)

	// GetCurrentDataKeyID gets the current data key ID for a keyspace
	GetCurrentDataKeyID(ctx context.Context, keyspaceID uint32) (string, error)

	// Start starts the background refresh goroutine
	Start(ctx context.Context) error

	// Stop stops the background refresh goroutine
	Stop()
}

type encryptionMetaManager struct {
	tikvClient TiKVEncryptionClient
	kmsClient  kms.KMSClient

	// Cache for encryption metadata
	metaCache map[uint32]*cachedMeta
	metaMu    sync.RWMutex

	// Cache for decrypted data keys
	dataKeyCache map[uint32]map[string]*cachedDataKey
	dataKeyMu    sync.RWMutex

	// TTL for cache refresh
	ttl time.Duration

	// Background refresh
	refreshInterval time.Duration
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

type cachedMeta struct {
	meta      *KeyspaceEncryptionMeta
	timestamp time.Time
}

type cachedDataKey struct {
	key       []byte
	algorithm EncryptionAlgorithm
	timestamp time.Time
}

// NewEncryptionMetaManager creates a new encryption meta manager
func NewEncryptionMetaManager(tikvClient TiKVEncryptionClient, kmsClient kms.KMSClient) EncryptionMetaManager {
	return &encryptionMetaManager{
		tikvClient:      tikvClient,
		kmsClient:       kmsClient,
		metaCache:       make(map[uint32]*cachedMeta),
		dataKeyCache:    make(map[uint32]map[string]*cachedDataKey),
		ttl:             1 * time.Hour, // Default TTL: 1 hour
		refreshInterval: 1 * time.Hour, // Default refresh interval: 1 hour
		stopCh:          make(chan struct{}),
	}
}

// IsEncryptionEnabled checks if encryption is enabled for a keyspace
func (m *encryptionMetaManager) IsEncryptionEnabled(ctx context.Context, keyspaceID uint32) bool {
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		// If we can't get meta, encryption is not enabled
		return false
	}
	return meta != nil && meta.Enabled
}

// GetCurrentDataKey gets the current data key for a keyspace
func (m *encryptionMetaManager) GetCurrentDataKey(ctx context.Context, keyspaceID uint32) ([]byte, EncryptionAlgorithm, error) {
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		return nil, "", err
	}

	if meta == nil || !meta.Enabled {
		return nil, "", nil
	}

	if meta.CurrentDataKeyID == "" {
		return nil, "", cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key ID is empty")
	}

	return m.GetDataKeyWithAlgorithm(ctx, keyspaceID, meta.CurrentDataKeyID)
}

// GetCurrentDataKeyID gets the current data key ID for a keyspace
func (m *encryptionMetaManager) GetCurrentDataKeyID(ctx context.Context, keyspaceID uint32) (string, error) {
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		return "", err
	}

	if meta == nil || !meta.Enabled {
		return "", cerrors.ErrDataKeyNotFound.GenWithStackByArgs("encryption not enabled")
	}

	if meta.CurrentDataKeyID == "" {
		return "", cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key ID is empty")
	}

	return meta.CurrentDataKeyID, nil
}

// GetDataKeyWithAlgorithm gets a data key by ID with its encryption algorithm
func (m *encryptionMetaManager) GetDataKeyWithAlgorithm(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, EncryptionAlgorithm, error) {
	// Check cache first
	m.dataKeyMu.RLock()
	if keyspaceCache, ok := m.dataKeyCache[keyspaceID]; ok {
		if cached, ok := keyspaceCache[dataKeyID]; ok {
			// Check if cache is still valid
			if time.Since(cached.timestamp) < m.ttl {
				key := make([]byte, len(cached.key))
				copy(key, cached.key)
				m.dataKeyMu.RUnlock()
				return key, cached.algorithm, nil
			}
		}
	}
	m.dataKeyMu.RUnlock()

	// Get meta to find the data key
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		return nil, "", err
	}

	if meta == nil || !meta.Enabled {
		return nil, "", cerrors.ErrDataKeyNotFound.GenWithStackByArgs("encryption not enabled")
	}

	dataKey, ok := meta.DataKeyMap[dataKeyID]
	if !ok {
		return nil, "", cerrors.ErrDataKeyNotFound.GenWithStackByArgs("data key not found: " + dataKeyID)
	}

	// Decrypt the data key using master key
	plaintextKey, err := m.decryptDataKey(ctx, meta.MasterKey, dataKey.Ciphertext)
	if err != nil {
		return nil, "", err
	}

	// Cache the decrypted key
	m.dataKeyMu.Lock()
	if m.dataKeyCache[keyspaceID] == nil {
		m.dataKeyCache[keyspaceID] = make(map[string]*cachedDataKey)
	}
	m.dataKeyCache[keyspaceID][dataKeyID] = &cachedDataKey{
		key:       plaintextKey,
		algorithm: dataKey.EncryptionAlgorithm,
		timestamp: time.Now(),
	}
	m.dataKeyMu.Unlock()

	return plaintextKey, dataKey.EncryptionAlgorithm, nil
}

// getMeta gets encryption metadata, with caching
func (m *encryptionMetaManager) getMeta(ctx context.Context, keyspaceID uint32) (*KeyspaceEncryptionMeta, error) {
	// Check cache first
	m.metaMu.RLock()
	if cached, ok := m.metaCache[keyspaceID]; ok {
		// Check if cache is still valid
		if time.Since(cached.timestamp) < m.ttl {
			meta := cached.meta
			m.metaMu.RUnlock()
			return meta, nil
		}
	}
	m.metaMu.RUnlock()

	// Fetch from TiKV
	meta, err := m.tikvClient.GetKeyspaceEncryptionMeta(ctx, keyspaceID)
	if err != nil {
		// If we get ErrEncryptionMetaNotFound, cache nil to avoid repeated lookups
		if cerrors.ErrEncryptionMetaNotFound.Equal(err) {
			m.metaMu.Lock()
			m.metaCache[keyspaceID] = &cachedMeta{
				meta:      nil,
				timestamp: time.Now(),
			}
			m.metaMu.Unlock()
			return nil, nil
		}
		return nil, err
	}

	// Cache the result (including nil if enabled=false)
	m.metaMu.Lock()
	m.metaCache[keyspaceID] = &cachedMeta{
		meta:      meta,
		timestamp: time.Now(),
	}
	m.metaMu.Unlock()

	// If enabled=false, return nil
	if meta != nil && !meta.Enabled {
		return nil, nil
	}

	return meta, nil
}

// decryptDataKey decrypts a data key using the master key
func (m *encryptionMetaManager) decryptDataKey(ctx context.Context, masterKey *MasterKey, dataKeyCiphertext []byte) ([]byte, error) {
	if masterKey == nil {
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("master key is nil")
	}

	// Decrypt master key from KMS
	masterKeyPlaintext, err := m.kmsClient.DecryptMasterKey(
		ctx,
		masterKey.Ciphertext,
		masterKey.CMEKID,
		masterKey.Vendor.String(),
		masterKey.Region,
	)
	if err != nil {
		return nil, cerrors.ErrDecodeFailed.Wrap(err)
	}

	// Decrypt data key using master key (AES-256-CTR)
	block, err := aes.NewCipher(masterKeyPlaintext)
	if err != nil {
		return nil, cerrors.ErrDecodeFailed.Wrap(err)
	}

	if len(dataKeyCiphertext) < aes.BlockSize {
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("data key ciphertext too short")
	}

	// Extract IV from the beginning
	iv := dataKeyCiphertext[:aes.BlockSize]
	encryptedData := dataKeyCiphertext[aes.BlockSize:]

	// Decrypt using CTR mode
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(encryptedData))
	stream.XORKeyStream(plaintext, encryptedData)

	return plaintext, nil
}

// Start starts the background refresh goroutine
func (m *encryptionMetaManager) Start(ctx context.Context) error {
	m.wg.Add(1)
	go m.refreshLoop(ctx)
	return nil
}

// Stop stops the background refresh goroutine
func (m *encryptionMetaManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// refreshLoop periodically refreshes encryption metadata
func (m *encryptionMetaManager) refreshLoop(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.refreshAll(ctx)
		}
	}
}

// refreshAll refreshes all cached encryption metadata
func (m *encryptionMetaManager) refreshAll(ctx context.Context) {
	m.metaMu.RLock()
	keyspaceIDs := make([]uint32, 0, len(m.metaCache))
	for keyspaceID := range m.metaCache {
		keyspaceIDs = append(keyspaceIDs, keyspaceID)
	}
	m.metaMu.RUnlock()

	for _, keyspaceID := range keyspaceIDs {
		// Force refresh by getting meta again
		_, err := m.getMeta(ctx, keyspaceID)
		if err != nil {
			log.Warn("failed to refresh encryption meta",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Error(err))
		}
	}
}
