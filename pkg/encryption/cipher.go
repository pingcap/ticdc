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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	cerrors "github.com/pingcap/ticdc/pkg/errors"
)

// Cipher is the interface for encryption/decryption operations
type Cipher interface {
	// Encrypt encrypts data using the provided key and IV
	Encrypt(data, key, iv []byte) ([]byte, error)

	// Decrypt decrypts data using the provided key and IV
	Decrypt(data, key, iv []byte) ([]byte, error)

	// IVSize returns the required IV size in bytes
	IVSize() int
}

// AES256CTRCipher implements AES-256-CTR encryption
type AES256CTRCipher struct{}

// NewAES256CTRCipher creates a new AES-256-CTR cipher
func NewAES256CTRCipher() *AES256CTRCipher {
	return &AES256CTRCipher{}
}

// IVSize returns the IV size for AES-256-CTR (16 bytes)
func (c *AES256CTRCipher) IVSize() int {
	return aes.BlockSize
}

// Encrypt encrypts data using AES-256-CTR
func (c *AES256CTRCipher) Encrypt(data, key, iv []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("key must be 32 bytes for AES-256")
	}
	if len(iv) != c.IVSize() {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("IV must be 16 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	stream := cipher.NewCTR(block, iv)
	ciphertext := make([]byte, len(data))
	stream.XORKeyStream(ciphertext, data)

	return ciphertext, nil
}

// Decrypt decrypts data using AES-256-CTR
func (c *AES256CTRCipher) Decrypt(data, key, iv []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("key must be 32 bytes for AES-256")
	}
	if len(iv) != c.IVSize() {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("IV must be 16 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	stream := cipher.NewCTR(block, iv)
	plaintext := make([]byte, len(data))
	stream.XORKeyStream(plaintext, data)

	return plaintext, nil
}

// GenerateIV generates a random IV of the specified size
func GenerateIV(size int) ([]byte, error) {
	iv := make([]byte, size)
	if _, err := rand.Read(iv); err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}
	return iv, nil
}
