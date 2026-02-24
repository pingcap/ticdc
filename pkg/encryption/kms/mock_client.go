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

package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// KMSClient is the interface for Key Management Service operations
type KMSClient interface {
	// DecryptMasterKey decrypts the master key using the specified KMS parameters
	DecryptMasterKey(ctx context.Context, ciphertext []byte, keyID string, vendor string, region string, endpoint string) ([]byte, error)
}

// MockKMSClient is a mock implementation of KMSClient for development and testing
// It uses an in-memory mock key for decryption
type MockKMSClient struct {
	// mockKey is a fixed key used for mock decryption (32 bytes for AES-256)
	mockKey []byte
}

// NewMockKMSClient creates a new mock KMS client
func NewMockKMSClient() *MockKMSClient {
	// Generate a fixed mock key for testing
	// In a real implementation, this would be retrieved from KMS
	mockKey := make([]byte, 32)
	// Use a deterministic key for testing (in production, this would come from KMS)
	for i := range mockKey {
		mockKey[i] = byte(i % 256)
	}

	return &MockKMSClient{
		mockKey: mockKey,
	}
}

// DecryptMasterKey decrypts the master key using mock KMS
// In a real implementation, this would call the actual KMS service
func (c *MockKMSClient) DecryptMasterKey(ctx context.Context, ciphertext []byte, keyID string, vendor string, region string, endpoint string) ([]byte, error) {
	if len(ciphertext) < aes.BlockSize {
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("ciphertext too short")
	}

	log.Debug("mock KMS client: decrypting master key",
		zap.String("keyID", keyID),
		zap.String("vendor", vendor),
		zap.String("region", region),
		zap.Int("ciphertextLen", len(ciphertext)))

	// In mock implementation, we use AES-256-CTR to decrypt
	// The ciphertext is encrypted with the mock key
	block, err := aes.NewCipher(c.mockKey)
	if err != nil {
		return nil, cerrors.ErrDecodeFailed.Wrap(err)
	}

	// Extract IV from the beginning of ciphertext
	iv := ciphertext[:aes.BlockSize]
	encryptedData := ciphertext[aes.BlockSize:]

	// Decrypt using CTR mode
	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(encryptedData))
	stream.XORKeyStream(plaintext, encryptedData)

	return plaintext, nil
}

// EncryptMasterKey encrypts a master key using mock KMS (for testing)
// This is used to generate mock ciphertext
func (c *MockKMSClient) EncryptMasterKey(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.mockKey)
	if err != nil {
		return nil, cerrors.ErrEncodeFailed.Wrap(err)
	}

	// Generate random IV
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, cerrors.ErrEncodeFailed.Wrap(err)
	}

	// Encrypt using CTR mode
	stream := cipher.NewCTR(block, iv)
	ciphertext := make([]byte, len(plaintext))
	stream.XORKeyStream(ciphertext, plaintext)

	// Prepend IV to ciphertext
	result := make([]byte, aes.BlockSize+len(ciphertext))
	copy(result[:aes.BlockSize], iv)
	copy(result[aes.BlockSize:], ciphertext)

	return result, nil
}
