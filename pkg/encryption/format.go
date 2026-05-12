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
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	// EncryptionHeaderSize is the size of encryption header (4 bytes)
	// Format: [version(1 byte)][dataKeyID(3 bytes)]
	EncryptionHeaderSize = 4

	// VersionUnencrypted indicates data is not encrypted
	VersionUnencrypted byte = 0x00
)

// EncryptionHeader represents the 4-byte encryption header
// Format: [version(1 byte)][dataKeyID(3 bytes)]
type EncryptionHeader struct {
	Version   byte
	DataKeyID [3]byte
}

// EncodeEncryptedData encodes data with encryption header
// Format: [version(1)][dataKeyID(3)][encryptedData]
// The version byte comes from the encryption metadata obtained from TiKV
func EncodeEncryptedData(data []byte, version byte, dataKeyID string) ([]byte, error) {
	if len(dataKeyID) != 3 {
		return nil, cerrors.ErrInvalidDataKeyID.GenWithStackByArgs("data key ID must be 3 bytes")
	}

	if version == VersionUnencrypted {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("version cannot be 0 for encrypted data")
	}

	result := make([]byte, EncryptionHeaderSize+len(data))
	result[0] = version
	copy(result[1:4], dataKeyID)
	copy(result[4:], data)

	return result, nil
}

// DecodeEncryptedData decodes data and extracts encryption header
// Returns: (version, dataKeyID, encryptedData, error)
func DecodeEncryptedData(data []byte) (byte, string, []byte, error) {
	if len(data) < EncryptionHeaderSize {
		return 0, "", nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("data too short for encryption header")
	}

	version := data[0]
	var dataKeyID [3]byte
	copy(dataKeyID[:], data[1:4])
	encryptedData := data[4:]

	return version, string(dataKeyID[:]), encryptedData, nil
}

func dataKeyIDIsZero(data []byte) bool {
	return data[1] == 0 && data[2] == 0 && data[3] == 0
}

func hasEncryptedHeader(data []byte) bool {
	return data[0] != VersionUnencrypted && !dataKeyIDIsZero(data)
}

// IsEncrypted checks if data is encrypted by examining the version byte
// Data is considered encrypted if version != 0 (VersionUnencrypted) and the
// 3-byte data key ID is non-zero.
func IsEncrypted(data []byte) bool {
	if len(data) < EncryptionHeaderSize {
		return false
	}
	return hasEncryptedHeader(data)
}

// HasUnencryptedHeader checks whether data uses the 4-byte plaintext wrapper.
func HasUnencryptedHeader(data []byte) bool {
	if len(data) < EncryptionHeaderSize {
		return false
	}
	return data[0] == VersionUnencrypted && dataKeyIDIsZero(data)
}

// IsEncryptedWithVersion checks if data is encrypted with a specific version
// This is useful when you know the expected version from TiKV metadata
func IsEncryptedWithVersion(data []byte, expectedVersion byte) bool {
	if len(data) < EncryptionHeaderSize {
		return false
	}
	return hasEncryptedHeader(data) && data[0] == expectedVersion
}

// GetVersion extracts the version byte from data
// Returns 0 if data is too short
func GetVersion(data []byte) byte {
	if len(data) < EncryptionHeaderSize {
		return 0
	}
	return data[0]
}

// EncodeUnencryptedData encodes unencrypted data with version=0 header
// This creates a unified format where all new data has the 4-byte header
func EncodeUnencryptedData(data []byte) []byte {
	result := make([]byte, EncryptionHeaderSize+len(data))
	result[0] = VersionUnencrypted
	// DataKeyID is zero for unencrypted data (3 bytes)
	result[1] = 0
	result[2] = 0
	result[3] = 0
	copy(result[4:], data)
	return result
}

// DecodeUnencryptedData decodes unencrypted data by removing the 4-byte plaintext header.
// Callers must guarantee the value is marked as passing through the encryption layer
// and uses the unencrypted header format.
func DecodeUnencryptedData(data []byte) ([]byte, error) {
	if !HasUnencryptedHeader(data) {
		log.Panic("unexpected data without unencrypted header",
			zap.Int("dataLen", len(data)))
	}
	return data[EncryptionHeaderSize:], nil
}

// ExtractDataKeyID extracts the data key ID from encrypted data
func ExtractDataKeyID(data []byte) (string, error) {
	if len(data) < EncryptionHeaderSize {
		return "", cerrors.ErrDecodeFailed.GenWithStackByArgs("data too short")
	}

	// Only extract key ID from data that definitely looks like new-format encrypted:
	// - version != 0 (encrypted data has non-zero version)
	// - data key ID is non-zero
	if hasEncryptedHeader(data) {
		var keyID [3]byte
		copy(keyID[:], data[1:4])
		return string(keyID[:]), nil
	}

	// Otherwise, this is not encrypted data (legacy data or new-format unencrypted)
	return "", cerrors.ErrDecodeFailed.GenWithStackByArgs("data is not encrypted")
}
