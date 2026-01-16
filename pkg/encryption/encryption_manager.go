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

	// Get current data key and algorithm
	dataKey, algorithm, err := m.metaManager.GetCurrentDataKey(ctx, keyspaceID)
	if err != nil {
		if allowDegrade {
			log.Warn("failed to get current data key, degrade to plaintext",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Error(err))
			return data, nil
		}
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		if algorithm == "" {
			log.Debug("encryption not enabled for keyspace",
				zap.Uint32("keyspaceID", keyspaceID))
			return data, nil
		}
		if allowDegrade {
			// No data key available
			log.Warn("data key is empty, degrade to plaintext",
				zap.Uint32("keyspaceID", keyspaceID))
			return data, nil
		}
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("data key is empty")
	}

	// Get cipher for the specified algorithm
	cipher, err := GetCipher(algorithm)
	if err != nil {
		log.Error("unsupported encryption algorithm",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("algorithm", string(algorithm)),
			zap.Error(err))
		return nil, err
	}

	// Generate IV
	iv, err := GenerateIV(cipher.IVSize())
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Encrypt data
	encryptedData, err := cipher.Encrypt(data, dataKey, iv)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Prepend IV to encrypted data
	encryptedWithIV := make([]byte, len(iv)+len(encryptedData))
	copy(encryptedWithIV, iv)
	copy(encryptedWithIV[len(iv):], encryptedData)

	// Get current data key ID
	currentDataKeyID, err := m.metaManager.GetCurrentDataKeyID(ctx, keyspaceID)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Get encryption version from metadata
	version, err := m.metaManager.GetEncryptionVersion(ctx, keyspaceID)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Encode with encryption header
	result, err := EncodeEncryptedData(encryptedWithIV, version, currentDataKeyID)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	log.Debug("data encrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("algorithm", string(algorithm)),
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
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	if version == VersionUnencrypted {
		// Should not happen if IsEncrypted returned true, but handle it anyway
		return dataWithIV, nil
	}

	// Get data key and algorithm
	dataKey, algorithm, err := m.metaManager.GetDataKeyWithAlgorithm(ctx, keyspaceID, dataKeyID)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("data key is empty")
	}

	// Get cipher for the specified algorithm
	cipher, err := GetCipher(algorithm)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	// Extract IV from the beginning of data
	ivSize := cipher.IVSize()
	if len(dataWithIV) < ivSize {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("data too short for IV")
	}

	iv := dataWithIV[:ivSize]
	encryptedDataOnly := dataWithIV[ivSize:]

	// Decrypt data
	plaintext, err := cipher.Decrypt(encryptedDataOnly, dataKey, iv)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	log.Debug("data decrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("algorithm", string(algorithm)),
		zap.String("dataKeyID", dataKeyID),
		zap.Int("encryptedSize", len(encryptedData)),
		zap.Int("plaintextSize", len(plaintext)))

	return plaintext, nil
}
