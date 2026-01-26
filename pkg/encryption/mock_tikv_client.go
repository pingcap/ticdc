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
	"crypto/rand"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/encryption/kms"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// TiKVEncryptionClient is the interface for getting encryption metadata from TiKV
type TiKVEncryptionClient interface {
	// GetKeyspaceEncryptionMeta gets the encryption metadata for a keyspace
	GetKeyspaceEncryptionMeta(ctx context.Context, keyspaceID uint32) (*KeyspaceEncryptionMeta, error)
}

// MockTiKVClient is a mock implementation of TiKVEncryptionClient for development and testing
type MockTiKVClient struct {
	// metaMap stores mock encryption metadata by keyspace ID
	metaMap map[uint32]*KeyspaceEncryptionMeta
	// notFoundKeyspaces stores keyspace IDs that should return ErrEncryptionMetaNotFound
	notFoundKeyspaces map[uint32]bool
}

// NewMockTiKVClient creates a new mock TiKV client
func NewMockTiKVClient() *MockTiKVClient {
	client := &MockTiKVClient{
		metaMap:           make(map[uint32]*KeyspaceEncryptionMeta),
		notFoundKeyspaces: make(map[uint32]bool),
	}

	// Initialize with some default mock data for testing
	client.initDefaultMockData()

	return client
}

// initDefaultMockData initializes default mock encryption metadata
func (c *MockTiKVClient) initDefaultMockData() {
	// Create a mock keyspace with encryption enabled
	// Data key IDs must be exactly 3 bytes to match the header format
	mockDataKeyID1 := "K01"
	mockDataKeyID2 := "K02"

	// Generate mock master key plaintext (32 bytes for AES-256)
	masterKeyPlaintext := make([]byte, 32)
	if _, err := rand.Read(masterKeyPlaintext); err != nil {
		log.Panic("failed to generate random master key plaintext", zap.Error(err))
	}

	// Generate mock data key plaintext (32 bytes for AES-256)
	dataKey1Plaintext := make([]byte, 32)
	if _, err := rand.Read(dataKey1Plaintext); err != nil {
		log.Panic("failed to generate random data key plaintext", zap.Error(err))
	}
	dataKey2Plaintext := make([]byte, 32)
	if _, err := rand.Read(dataKey2Plaintext); err != nil {
		log.Panic("failed to generate random data key plaintext", zap.Error(err))
	}

	// Encrypt data keys using master key (AES-256-CTR with zero IV)
	block, err := aes.NewCipher(masterKeyPlaintext)
	if err != nil {
		log.Panic("failed to create AES cipher for data key encryption", zap.Error(err))
	}
	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCTR(block, iv)

	dataKey1Ciphertext := make([]byte, len(dataKey1Plaintext))
	stream.XORKeyStream(dataKey1Ciphertext, dataKey1Plaintext)

	// Reset stream by creating a new CTR stream to ensure deterministic encryption per key.
	stream = cipher.NewCTR(block, iv)
	dataKey2Ciphertext := make([]byte, len(dataKey2Plaintext))
	stream.XORKeyStream(dataKey2Ciphertext, dataKey2Plaintext)

	// Encrypt master key plaintext via mock KMS to generate a realistic ciphertext.
	kmsClient := kms.NewMockKMSClient()
	masterKeyCiphertext, err := kmsClient.EncryptMasterKey(masterKeyPlaintext)
	if err != nil {
		log.Panic("failed to encrypt master key via mock KMS", zap.Error(err))
	}

	meta := &KeyspaceEncryptionMeta{
		Enabled: true,
		Version: 0x01, // Encryption format version from TiKV
		MasterKey: &MasterKey{
			Vendor:     KMSVendorAWS,
			CMEKID:     "foobar1",
			Region:     "us-west-1",
			Ciphertext: masterKeyCiphertext,
		},
		CurrentDataKeyID: mockDataKeyID2,
		DataKeyMap: map[string]*DataKey{
			mockDataKeyID1: {
				Ciphertext:          dataKey1Ciphertext,
				EncryptionAlgorithm: AES256CTR,
			},
			mockDataKeyID2: {
				Ciphertext:          dataKey2Ciphertext,
				EncryptionAlgorithm: AES256CTR,
			},
		},
	}

	// Use keyspace ID 1 as default enabled keyspace
	c.metaMap[1] = meta

	// Create a mock keyspace with encryption disabled
	disabledMeta := &KeyspaceEncryptionMeta{
		Enabled: false,
	}
	c.metaMap[2] = disabledMeta
}

// GetKeyspaceEncryptionMeta gets the encryption metadata for a keyspace
func (c *MockTiKVClient) GetKeyspaceEncryptionMeta(ctx context.Context, keyspaceID uint32) (*KeyspaceEncryptionMeta, error) {
	// Check if this keyspace should return not found error
	if c.notFoundKeyspaces[keyspaceID] {
		log.Debug("mock TiKV client: encryption meta not found",
			zap.Uint32("keyspaceID", keyspaceID))
		return nil, cerrors.ErrEncryptionMetaNotFound
	}

	// Return mock metadata if available
	if meta, ok := c.metaMap[keyspaceID]; ok {
		log.Debug("mock TiKV client: returning encryption meta",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Bool("enabled", meta.Enabled))
		return meta, nil
	}

	// Default behavior: return not found for unknown keyspaces
	// This simulates classic architecture or unconfigured encryption
	log.Debug("mock TiKV client: encryption meta not found (unknown keyspace)",
		zap.Uint32("keyspaceID", keyspaceID))
	return nil, cerrors.ErrEncryptionMetaNotFound
}

// SetKeyspaceMeta sets mock encryption metadata for a keyspace (for testing)
func (c *MockTiKVClient) SetKeyspaceMeta(keyspaceID uint32, meta *KeyspaceEncryptionMeta) {
	c.metaMap[keyspaceID] = meta
}

// SetKeyspaceNotFound sets a keyspace to return cerrors.ErrEncryptionMetaNotFound (for testing)
func (c *MockTiKVClient) SetKeyspaceNotFound(keyspaceID uint32) {
	c.notFoundKeyspaces[keyspaceID] = true
}

// ClearKeyspaceNotFound clears the not found flag for a keyspace (for testing)
func (c *MockTiKVClient) ClearKeyspaceNotFound(keyspaceID uint32) {
	delete(c.notFoundKeyspaces, keyspaceID)
}

// GetKeyspaceMeta gets the stored mock metadata (for testing)
func (c *MockTiKVClient) GetKeyspaceMeta(keyspaceID uint32) (*KeyspaceEncryptionMeta, bool) {
	meta, ok := c.metaMap[keyspaceID]
	return meta, ok
}
