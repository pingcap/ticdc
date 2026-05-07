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
	"testing"

	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestEncodeEncryptedDataInvalidKey(t *testing.T) {
	// Key ID must be exactly 3 bytes
	_, err := EncodeEncryptedData([]byte("payload"), 0x01, "ab")
	require.Error(t, err)
	require.True(t, cerrors.ErrInvalidDataKeyID.Equal(err))

	_, err = EncodeEncryptedData([]byte("payload"), 0x01, "abcd")
	require.Error(t, err)
	require.True(t, cerrors.ErrInvalidDataKeyID.Equal(err))
}

func TestEncodeEncryptedDataInvalidVersion(t *testing.T) {
	// Version cannot be 0 for encrypted data
	_, err := EncodeEncryptedData([]byte("payload"), VersionUnencrypted, "abc")
	require.Error(t, err)
}

func TestEncodeDecodeEncryptedData(t *testing.T) {
	data := []byte("payload")
	keyID := "abc"
	version := byte(0x01)

	encoded, err := EncodeEncryptedData(data, version, keyID)
	require.NoError(t, err)
	require.True(t, IsEncrypted(encoded))

	// Verify version byte is set correctly
	require.Equal(t, version, encoded[0])

	decodedVersion, decodedKeyID, body, err := DecodeEncryptedData(encoded)
	require.NoError(t, err)
	require.Equal(t, version, decodedVersion)
	require.Equal(t, keyID, decodedKeyID)
	require.Equal(t, data, body)
}

func TestEncodeDecodeWithDifferentVersions(t *testing.T) {
	data := []byte("payload")

	// Test with different version values that might come from TiKV
	versions := []byte{0x01, 0x02, 0x10, 0xFF}

	for _, version := range versions {
		keyID := string([]byte{0xAA, 0xBB, version ^ 0xFF})
		encoded, err := EncodeEncryptedData(data, version, keyID)
		require.NoError(t, err)
		require.True(t, IsEncrypted(encoded))
		require.Equal(t, version, GetVersion(encoded))

		decodedVersion, decodedKeyID, body, err := DecodeEncryptedData(encoded)
		require.NoError(t, err)
		require.Equal(t, version, decodedVersion)
		require.Equal(t, keyID, decodedKeyID)
		require.Equal(t, data, body)
	}
}

func TestEncodeUnencryptedData(t *testing.T) {
	raw := []byte("plain")
	encoded := EncodeUnencryptedData(raw)

	// Unencrypted data with header should NOT be detected as encrypted
	// because the version byte is VersionUnencrypted (0x00)
	require.False(t, IsEncrypted(encoded))
	require.Equal(t, VersionUnencrypted, encoded[0])

	decoded, err := DecodeUnencryptedData(encoded)
	require.NoError(t, err)
	require.Equal(t, raw, decoded)
}

func TestIsEncryptedWithLegacyData(t *testing.T) {
	// Legacy unencrypted data (no header) should not be detected as encrypted
	// because it's too short for the header
	shortData := []byte("abc")
	require.False(t, IsEncrypted(shortData))

	// Data with version=0 (first byte is 0x00) is not encrypted
	unencryptedWithHeader := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	require.False(t, IsEncrypted(unencryptedWithHeader))
}

func TestIsEncryptedWithVersionByte(t *testing.T) {
	// Data with a non-zero version and non-zero data key ID should be detected as encrypted.
	encryptedData := []byte{0x01, 'a', 'b', 'c', 'd', 'a', 't', 'a'}
	require.True(t, IsEncrypted(encryptedData))

	// Version and data key ID are independent fields.
	for _, v := range []byte{0x01, 0x02, 0x10, 0xFF} {
		data := []byte{v, 'a', 'b', v ^ 0xFF, 'd', 'a', 't', 'a'}
		require.True(t, IsEncrypted(data))
	}

	// Data too short should not be detected as encrypted
	shortData := []byte{0x01, 'a', 'b'}
	require.False(t, IsEncrypted(shortData))
}

func TestIsEncryptedWithVersion(t *testing.T) {
	data := []byte{0x05, 'a', 'b', 'c', 'd', 'a', 't', 'a'}

	// Should match when version matches
	require.True(t, IsEncryptedWithVersion(data, 0x05))

	// Should not match when version doesn't match
	require.False(t, IsEncryptedWithVersion(data, 0x01))
	require.False(t, IsEncryptedWithVersion(data, 0x00))
}

func TestGetVersion(t *testing.T) {
	// Normal data
	data := []byte{0x05, 'a', 'b', 'c', 'd', 'a', 't', 'a'}
	require.Equal(t, byte(0x05), GetVersion(data))

	// Short data returns 0
	shortData := []byte{0x05, 'a', 'b'}
	require.Equal(t, byte(0x00), GetVersion(shortData))
}

func TestDecodeUnencryptedDataPanicsWithoutUnencryptedHeader(t *testing.T) {
	legacyData := []byte("legacy")
	require.Panics(t, func() {
		_, _ = DecodeUnencryptedData(legacyData)
	})

	legacyData2 := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	require.Panics(t, func() {
		_, _ = DecodeUnencryptedData(legacyData2)
	})
}

func TestDecodeUnencryptedDataPanicsWithEncryptedData(t *testing.T) {
	encryptedData := []byte{0x01, 'a', 'b', 'c', 'd', 'a', 't', 'a'}
	require.Panics(t, func() {
		_, _ = DecodeUnencryptedData(encryptedData)
	})
}

func TestExtractDataKeyID(t *testing.T) {
	data := []byte("payload")
	keyID := "xyz"
	version := byte(0x01)

	encoded, err := EncodeEncryptedData(data, version, keyID)
	require.NoError(t, err)

	extractedKeyID, err := ExtractDataKeyID(encoded)
	require.NoError(t, err)
	require.Equal(t, keyID, extractedKeyID)
}

func TestExtractDataKeyIDFromUnencryptedData(t *testing.T) {
	// Trying to extract key ID from unencrypted data should return error
	unencryptedData := EncodeUnencryptedData([]byte("plain"))
	_, err := ExtractDataKeyID(unencryptedData)
	require.Error(t, err)

	// Trying to extract key ID from legacy data that is too short should return error
	legacyData := []byte("abc") // 3 bytes < 4
	_, err = ExtractDataKeyID(legacyData)
	require.Error(t, err)

	// Legacy data with non-zero bytes in positions 1-3 should also return error
	// (This can't be confused with new-format encrypted data which has non-zero key ID)
	legacyData2 := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	_, err = ExtractDataKeyID(legacyData2)
	require.Error(t, err)
}
