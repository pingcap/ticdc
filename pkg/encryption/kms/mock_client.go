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

	cerrors "github.com/pingcap/ticdc/pkg/errors"
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
