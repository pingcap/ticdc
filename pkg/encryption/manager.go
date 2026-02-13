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

	// GetCurrentDataKey gets the current data key for a keyspace.
	// It returns the plaintext data key, the 3-byte data key ID used in the encryption header,
	// and the encryption format version (derived from current.data_key_id & 0xFF).
	//
	// Returning key and key ID together avoids mismatches if TiKV rotates keys between calls.
	GetCurrentDataKey(ctx context.Context, keyspaceID uint32) (dataKey []byte, dataKeyID string, version byte, err error)

	// GetDataKey gets a data key by ID.
	GetDataKey(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, error)

	// Start starts the background refresh goroutine
	Start(ctx context.Context) error

	// Stop stops the background refresh goroutine
	Stop()
}

type encryptionMetaManager struct {
	tikvClient TiKVEncryptionClient
	kmsClient  kms.KMSClient

	metaCache    map[uint32]*cachedMeta
	metaMu       sync.RWMutex
	dataKeyCache map[uint32]map[string]*cachedDataKey
	dataKeyMu    sync.RWMutex

	ttl             time.Duration
	refreshInterval time.Duration
	stopCh          chan struct{}
	stopOnce        sync.Once
	wg              sync.WaitGroup
}

type cachedMeta struct {
	meta      *EncryptionMeta
	timestamp time.Time
}

type cachedDataKey struct {
	key       []byte
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
		log.Warn("failed to get encryption meta",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		// If we can't get meta, encryption is not enabled
		return false
	}
	return meta != nil
}

// GetCurrentDataKey gets the current data key for a keyspace
func (m *encryptionMetaManager) GetCurrentDataKey(ctx context.Context, keyspaceID uint32) ([]byte, string, byte, error) {
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		log.Warn("failed to get encryption meta for current data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, "", 0, err
	}

	if meta == nil {
		return nil, "", 0, nil
	}

	if meta.Current == nil || meta.Current.DataKeyId == 0 {
		log.Warn("encryption meta current data key ID is empty",
			zap.Uint32("keyspaceID", keyspaceID))
		return nil, "", 0, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key ID is empty")
	}

	currentKeyID, err := encodeDataKeyID24BE(meta.Current.DataKeyId)
	if err != nil {
		log.Warn("failed to encode current data key ID",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", meta.Current.DataKeyId),
			zap.Error(err))
		return nil, "", 0, err
	}

	version := byte(meta.Current.DataKeyId & 0xFF)
	if version == VersionUnencrypted {
		log.Warn("invalid encryption meta version derived from current data key ID",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", meta.Current.DataKeyId))
		return nil, "", 0, cerrors.ErrEncryptionFailed.GenWithStackByArgs("version must be non-zero")
	}

	// Check cache first.
	m.dataKeyMu.RLock()
	if keyspaceCache, ok := m.dataKeyCache[keyspaceID]; ok {
		if cached, ok := keyspaceCache[currentKeyID]; ok {
			if time.Since(cached.timestamp) < m.ttl {
				key := make([]byte, len(cached.key))
				copy(key, cached.key)
				m.dataKeyMu.RUnlock()
				return key, currentKeyID, version, nil
			}
		}
	}
	m.dataKeyMu.RUnlock()

	dataKey, ok := meta.DataKeys[meta.Current.DataKeyId]
	if !ok {
		log.Warn("current data key not found in encryption meta",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", meta.Current.DataKeyId))
		return nil, "", 0, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key not found")
	}

	plaintextKey, err := m.decryptDataKey(ctx, meta.MasterKey, dataKey.Ciphertext)
	if err != nil {
		log.Warn("failed to decrypt current data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", meta.Current.DataKeyId),
			zap.Binary("dataKeyIDBytes", []byte(currentKeyID)),
			zap.String("kmsVendor", safeKMSVendor(meta.MasterKey)),
			zap.String("cmekID", safeCMEKID(meta.MasterKey)),
			zap.Error(err))
		return nil, "", 0, err
	}

	m.dataKeyMu.Lock()
	if m.dataKeyCache[keyspaceID] == nil {
		m.dataKeyCache[keyspaceID] = make(map[string]*cachedDataKey)
	}
	m.dataKeyCache[keyspaceID][currentKeyID] = &cachedDataKey{
		key:       plaintextKey,
		timestamp: time.Now(),
	}
	m.dataKeyMu.Unlock()

	return plaintextKey, currentKeyID, version, nil
}

// GetDataKey gets a data key by ID.
func (m *encryptionMetaManager) GetDataKey(ctx context.Context, keyspaceID uint32, dataKeyID string) ([]byte, error) {
	// Check cache first
	m.dataKeyMu.RLock()
	if keyspaceCache, ok := m.dataKeyCache[keyspaceID]; ok {
		if cached, ok := keyspaceCache[dataKeyID]; ok {
			// Check if cache is still valid
			if time.Since(cached.timestamp) < m.ttl {
				key := make([]byte, len(cached.key))
				copy(key, cached.key)
				m.dataKeyMu.RUnlock()
				return key, nil
			}
		}
	}
	m.dataKeyMu.RUnlock()

	// Get meta to find the data key
	meta, err := m.getMeta(ctx, keyspaceID)
	if err != nil {
		log.Warn("failed to get encryption meta for data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, err
	}

	if meta == nil {
		log.Warn("encryption not enabled when looking up data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Binary("dataKeyID", []byte(dataKeyID)))
		return nil, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("encryption not enabled")
	}

	id, err := decodeDataKeyID24BE(dataKeyID)
	if err != nil {
		log.Warn("failed to decode data key ID",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, err
	}

	dataKey, ok := meta.DataKeys[id]
	if !ok {
		log.Warn("data key not found in encryption meta",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", id),
			zap.Binary("dataKeyIDBytes", []byte(dataKeyID)))
		return nil, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("data key not found: " + dataKeyID)
	}

	// Decrypt the data key using master key
	plaintextKey, err := m.decryptDataKey(ctx, meta.MasterKey, dataKey.Ciphertext)
	if err != nil {
		log.Warn("failed to decrypt data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint32("dataKeyID", id),
			zap.Binary("dataKeyIDBytes", []byte(dataKeyID)),
			zap.String("kmsVendor", safeKMSVendor(meta.MasterKey)),
			zap.String("cmekID", safeCMEKID(meta.MasterKey)),
			zap.Error(err))
		return nil, err
	}

	// Cache the decrypted key
	m.dataKeyMu.Lock()
	if m.dataKeyCache[keyspaceID] == nil {
		m.dataKeyCache[keyspaceID] = make(map[string]*cachedDataKey)
	}
	m.dataKeyCache[keyspaceID][dataKeyID] = &cachedDataKey{
		key:       plaintextKey,
		timestamp: time.Now(),
	}
	m.dataKeyMu.Unlock()

	return plaintextKey, nil
}

// getMeta gets encryption metadata, with caching
func (m *encryptionMetaManager) getMeta(ctx context.Context, keyspaceID uint32) (*EncryptionMeta, error) {
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
			log.Debug("encryption meta not found",
				zap.Uint32("keyspaceID", keyspaceID))
			m.metaMu.Lock()
			m.metaCache[keyspaceID] = &cachedMeta{
				meta:      nil,
				timestamp: time.Now(),
			}
			m.metaMu.Unlock()
			return nil, nil
		}
		log.Warn("failed to fetch encryption meta from TiKV",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, err
	}

	// Cache the result (including nil if enabled=false)
	m.metaMu.Lock()
	m.metaCache[keyspaceID] = &cachedMeta{
		meta:      meta,
		timestamp: time.Now(),
	}
	m.metaMu.Unlock()

	return meta, nil
}

// decryptDataKey decrypts a data key using the master key
func (m *encryptionMetaManager) decryptDataKey(ctx context.Context, masterKey *MasterKey, dataKeyCiphertext []byte) ([]byte, error) {
	if masterKey == nil {
		log.Warn("failed to decrypt data key: master key is nil")
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("master key is nil")
	}

	// Decrypt master key from KMS
	masterKeyPlaintext, err := m.kmsClient.DecryptMasterKey(
		ctx,
		masterKey.Ciphertext,
		masterKey.CmekId,
		masterKey.Vendor,
		masterKey.Region,
		masterKey.Endpoint,
	)
	if err != nil {
		log.Warn("failed to decrypt master key via KMS",
			zap.String("kmsVendor", masterKey.Vendor),
			zap.String("cmekID", masterKey.CmekId),
			zap.String("region", masterKey.Region),
			zap.String("endpoint", masterKey.Endpoint),
			zap.Error(err))
		return nil, cerrors.ErrDecodeFailed.Wrap(err)
	}

	if len(masterKeyPlaintext) != 32 {
		log.Warn("invalid master key plaintext length",
			zap.Int("length", len(masterKeyPlaintext)))
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("master key plaintext must be 32 bytes")
	}

	// Decrypt data key using master key (AES-256-CTR)
	block, err := aes.NewCipher(masterKeyPlaintext)
	if err != nil {
		log.Warn("failed to initialize AES cipher for data key decryption",
			zap.Error(err))
		return nil, cerrors.ErrDecodeFailed.Wrap(err)
	}

	if len(dataKeyCiphertext) != 32 {
		log.Warn("invalid data key ciphertext length",
			zap.Int("length", len(dataKeyCiphertext)))
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("data key ciphertext must be 32 bytes")
	}

	// The ciphertext is encrypted using AES-256-CTR with a zero IV.
	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(dataKeyCiphertext))
	stream.XORKeyStream(plaintext, dataKeyCiphertext)

	return plaintext, nil
}

func safeKMSVendor(masterKey *MasterKey) string {
	if masterKey == nil {
		return ""
	}
	return masterKey.Vendor
}

func safeCMEKID(masterKey *MasterKey) string {
	if masterKey == nil {
		return ""
	}
	return masterKey.CmekId
}

// Start starts the background refresh goroutine
func (m *encryptionMetaManager) Start(ctx context.Context) error {
	m.wg.Add(1)
	go m.refreshLoop(ctx)
	return nil
}

func (m *encryptionMetaManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()
}

func (m *encryptionMetaManager) Close() {
	m.Stop()
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

func (m *encryptionMetaManager) refreshAll(ctx context.Context) {
	m.metaMu.RLock()
	keyspaceIDs := make([]uint32, 0, len(m.metaCache))
	for keyspaceID := range m.metaCache {
		keyspaceIDs = append(keyspaceIDs, keyspaceID)
	}
	m.metaMu.RUnlock()

	for _, keyspaceID := range keyspaceIDs {
		m.metaMu.Lock()
		delete(m.metaCache, keyspaceID)
		m.metaMu.Unlock()

		_, err := m.getMeta(ctx, keyspaceID)
		if err != nil {
			log.Warn("failed to refresh encryption meta",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Error(err))
		}
	}
}
