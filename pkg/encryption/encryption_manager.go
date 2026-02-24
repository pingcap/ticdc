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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// EncryptionManager is the main interface for encryption/decryption operations
type EncryptionManager interface {
	// EncryptData encrypts data for a keyspace
	// Returns encrypted data with header, or original data if encryption is not enabled
	EncryptData(ctx context.Context, keyspaceID uint32, data []byte) ([]byte, error)

	// DecryptData decrypts data for a keyspace
	// Automatically detects if data is encrypted and handles accordingly
	DecryptData(ctx context.Context, keyspaceID uint32, encryptedData []byte) ([]byte, error)
}

type encryptionManager struct {
	metaManager EncryptionMetaManager
}

// NewEncryptionManager creates a new encryption manager
func NewEncryptionManager(metaManager EncryptionMetaManager) EncryptionManager {
	return &encryptionManager{
		metaManager: metaManager,
	}
}

// EncryptData encrypts data for a keyspace
func (m *encryptionManager) EncryptData(ctx context.Context, keyspaceID uint32, data []byte) ([]byte, error) {
	allowDegrade := true
	serverCfg := config.GetGlobalServerConfig()
	if serverCfg != nil && serverCfg.Debug != nil && serverCfg.Debug.Encryption != nil {
		allowDegrade = serverCfg.Debug.Encryption.AllowDegradeOnError
	}

	// Get current data key, key ID and version together to avoid mismatch when keys rotate.
	dataKey, currentDataKeyID, version, err := m.metaManager.GetCurrentDataKey(ctx, keyspaceID)
	if err != nil {
		if allowDegrade {
			log.Warn("failed to get current data key, degrade to plaintext",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Error(err))
			return data, nil
		}
		log.Error("failed to get current data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		log.Debug("encryption not enabled for keyspace",
			zap.Uint32("keyspaceID", keyspaceID))
		return data, nil
	}

	cipherImpl := NewAES256CTRCipher()

	// Generate IV
	iv, err := GenerateIV(cipherImpl.IVSize())
	if err != nil {
		log.Error("failed to generate IV",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Encrypt data
	encryptedData, err := cipherImpl.Encrypt(data, dataKey, iv)
	if err != nil {
		log.Error("failed to encrypt data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Prepend IV to encrypted data
	encryptedWithIV := make([]byte, len(iv)+len(encryptedData))
	copy(encryptedWithIV, iv)
	copy(encryptedWithIV[len(iv):], encryptedData)

	// Encode with encryption header
	result, err := EncodeEncryptedData(encryptedWithIV, version, currentDataKeyID)
	if err != nil {
		log.Error("failed to encode encrypted data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(currentDataKeyID)),
			zap.Error(err))
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	log.Debug("data encrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("dataKeyID", currentDataKeyID),
		zap.Int("originalSize", len(data)),
		zap.Int("encryptedSize", len(result)))

	return result, nil
}

// DecryptData decrypts data for a keyspace
func (m *encryptionManager) DecryptData(ctx context.Context, keyspaceID uint32, encryptedData []byte) ([]byte, error) {
	// Check if data is encrypted
	if !IsEncrypted(encryptedData) {
		// Data is not encrypted, return as-is (backward compatibility)
		log.Debug("data is not encrypted",
			zap.Uint32("keyspaceID", keyspaceID))
		return encryptedData, nil
	}

	// Decode encryption header
	version, dataKeyID, dataWithIV, err := DecodeEncryptedData(encryptedData)
	if err != nil {
		log.Warn("failed to decode encrypted data header",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Int("encryptedSize", len(encryptedData)),
			zap.Error(err))
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	if version == VersionUnencrypted {
		// Should not happen if IsEncrypted returned true, but handle it anyway
		return dataWithIV, nil
	}

	dataKey, err := m.metaManager.GetDataKey(ctx, keyspaceID, dataKeyID)
	if err != nil {
		log.Warn("failed to get data key for decryption",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		log.Warn("data key is empty for decryption",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)))
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("data key is empty")
	}

	cipherImpl := NewAES256CTRCipher()

	// Extract IV from the beginning of data
	ivSize := cipherImpl.IVSize()
	if len(dataWithIV) < ivSize {
		log.Warn("encrypted data too short for IV",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Int("dataWithIVSize", len(dataWithIV)),
			zap.Int("expectedIVSize", ivSize))
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("data too short for IV")
	}

	iv := dataWithIV[:ivSize]
	encryptedDataOnly := dataWithIV[ivSize:]

	// Decrypt data
	plaintext, err := cipherImpl.Decrypt(encryptedDataOnly, dataKey, iv)
	if err != nil {
		log.Warn("failed to decrypt data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	log.Debug("data decrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("dataKeyID", dataKeyID),
		zap.Int("encryptedSize", len(encryptedData)),
		zap.Int("plaintextSize", len(plaintext)))

	return plaintext, nil
}
