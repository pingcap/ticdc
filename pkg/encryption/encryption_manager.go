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

package encryption

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// EncryptionManager is the main interface for encryption/decryption operations
type EncryptionManager interface {
	// EncryptData encrypts data for a keyspace
	// Returns a value that has passed through the encryption layer and is wrapped
	// with a 4-byte header. The payload is encrypted when a data key is available,
	// or left plaintext with a version-0 header otherwise.
	EncryptData(ctx context.Context, keyspaceID uint32, data []byte) ([]byte, error)

	// DecryptData decrypts data for a keyspace
	// The caller must ensure the key marks this value as passing through the
	// encryption layer, so the value must carry the 4-byte encryption header.
	DecryptData(ctx context.Context, keyspaceID uint32, layerData []byte) ([]byte, error)
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
	if serverCfg != nil && serverCfg.Encryption != nil {
		allowDegrade = serverCfg.Encryption.AllowDegradeOnError
	}

	// Get current data key, key ID and version together to avoid mismatch when keys rotate.
	dataKey, currentDataKeyID, version, err := m.metaManager.GetCurrentDataKey(ctx, keyspaceID)
	if err != nil {
		if allowDegrade {
			log.Warn("failed to get current data key, degrade to plaintext",
				zap.Uint32("keyspaceID", keyspaceID),
				zap.Error(err))
			return EncodeUnencryptedData(data), nil
		}
		log.Error("failed to get current data key",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, errors.ErrEncryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		log.Debug("encryption not enabled for keyspace",
			zap.Uint32("keyspaceID", keyspaceID))
		return EncodeUnencryptedData(data), nil
	}

	cipherImpl := NewAES256CTRCipher()

	// Generate IV
	iv, err := GenerateIV(cipherImpl.IVSize())
	if err != nil {
		log.Error("failed to generate IV",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, errors.ErrEncryptionFailed.Wrap(err)
	}

	// Encrypt data
	encryptedData, err := cipherImpl.Encrypt(data, dataKey, iv)
	if err != nil {
		log.Error("failed to encrypt data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
		return nil, errors.ErrEncryptionFailed.Wrap(err)
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
		return nil, errors.ErrEncryptionFailed.Wrap(err)
	}

	log.Debug("data encrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("dataKeyID", currentDataKeyID),
		zap.Int("originalSize", len(data)),
		zap.Int("encryptedSize", len(result)))

	return result, nil
}

func decodeEncryptionLayerData(layerData []byte) (byte, string, []byte, error) {
	version, dataKeyID, payload, err := DecodeEncryptedData(layerData)
	if err != nil {
		return 0, "", nil, err
	}

	if version == VersionUnencrypted {
		if !dataKeyIDIsZero(layerData) {
			return 0, "", nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("invalid unencrypted data header")
		}
		return version, "", payload, nil
	}

	if dataKeyIDIsZero(layerData) {
		return 0, "", nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("invalid encrypted data header")
	}

	return version, dataKeyID, payload, nil
}

// DecryptData decrypts data for a keyspace
func (m *encryptionManager) DecryptData(ctx context.Context, keyspaceID uint32, layerData []byte) ([]byte, error) {
	version, dataKeyID, payload, err := decodeEncryptionLayerData(layerData)
	if err != nil {
		log.Warn("failed to decode encryption layer data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Int("valueSize", len(layerData)),
			zap.Error(err))
		return nil, errors.ErrDecryptionFailed.Wrap(err)
	}

	if version == VersionUnencrypted {
		return payload, nil
	}

	dataKey, err := m.metaManager.GetDataKey(ctx, keyspaceID, dataKeyID)
	if err != nil {
		log.Warn("failed to get data key for decryption",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, errors.ErrDecryptionFailed.Wrap(err)
	}

	if len(dataKey) == 0 {
		log.Warn("data key is empty for decryption",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)))
		return nil, errors.ErrDecryptionFailed.GenWithStackByArgs("data key is empty")
	}

	cipherImpl := NewAES256CTRCipher()

	// Extract IV from the beginning of data
	ivSize := cipherImpl.IVSize()
	if len(payload) < ivSize {
		log.Warn("encrypted data too short for IV",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Int("dataWithIVSize", len(payload)),
			zap.Int("expectedIVSize", ivSize))
		return nil, errors.ErrDecryptionFailed.GenWithStackByArgs("data too short for IV")
	}

	iv := payload[:ivSize]
	encryptedDataOnly := payload[ivSize:]

	// Decrypt data
	plaintext, err := cipherImpl.Decrypt(encryptedDataOnly, dataKey, iv)
	if err != nil {
		log.Warn("failed to decrypt data",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Uint8("version", version),
			zap.Binary("dataKeyID", []byte(dataKeyID)),
			zap.Error(err))
		return nil, errors.ErrDecryptionFailed.Wrap(err)
	}

	log.Debug("data decrypted successfully",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.String("dataKeyID", dataKeyID),
		zap.Int("encryptedSize", len(layerData)),
		zap.Int("plaintextSize", len(plaintext)))

	return plaintext, nil
}
