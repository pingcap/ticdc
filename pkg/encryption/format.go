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
	cerrors "github.com/pingcap/ticdc/pkg/errors"
)

const (
	// EncryptionHeaderSize is the size of encryption header (4 bytes)
	EncryptionHeaderSize = 4

	// VersionUnencrypted is the version for unencrypted data
	VersionUnencrypted byte = 0

	// VersionEncrypted is the minimum version for encrypted data
	VersionEncrypted byte = 1
)

// EncryptionHeader represents the 4-byte encryption header
// Format: [version(1 byte)][dataKeyID(3 bytes)]
type EncryptionHeader struct {
	Version   byte
	DataKeyID [3]byte
}

// EncodeEncryptedData encodes data with encryption header
// Format: [version(1)][dataKeyID(3)][encryptedData]
func EncodeEncryptedData(data []byte, dataKeyID string) ([]byte, error) {
	if len(dataKeyID) != 3 {
		return nil, cerrors.ErrInvalidDataKeyID.GenWithStackByArgs("data key ID must be 3 bytes")
	}

	header := EncryptionHeader{
		Version: VersionEncrypted,
	}
	copy(header.DataKeyID[:], dataKeyID)

	result := make([]byte, EncryptionHeaderSize+len(data))
	result[0] = header.Version
	copy(result[1:4], header.DataKeyID[:])
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

// IsEncrypted checks if data is encrypted by examining the version field
func IsEncrypted(data []byte) bool {
	if len(data) < EncryptionHeaderSize {
		return false
	}
	return data[0] != VersionUnencrypted
}

// EncodeUnencryptedData encodes unencrypted data with version=0 header (optional)
// This is used for unified format, but may not be necessary for backward compatibility
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

// DecodeUnencryptedData decodes unencrypted data (removes header if present)
func DecodeUnencryptedData(data []byte) ([]byte, error) {
	if len(data) < EncryptionHeaderSize {
		// No header, return as-is (backward compatibility)
		return data, nil
	}

	version := data[0]
	if version == VersionUnencrypted {
		// Remove header
		return data[4:], nil
	}

	// If version != 0, this might be encrypted data
	return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("data appears to be encrypted")
}

// ExtractDataKeyID extracts the data key ID from encrypted data
func ExtractDataKeyID(data []byte) (string, error) {
	if len(data) < EncryptionHeaderSize {
		return "", cerrors.ErrDecodeFailed.GenWithStackByArgs("data too short")
	}

	var dataKeyID [3]byte
	copy(dataKeyID[:], data[1:4])
	return string(dataKeyID[:]), nil
}
