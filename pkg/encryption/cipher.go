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
	if len(iv) != aes.BlockSize {
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
	if len(iv) != aes.BlockSize {
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

// AES256GCMCipher implements AES-256-GCM encryption
type AES256GCMCipher struct{}

// NewAES256GCMCipher creates a new AES-256-GCM cipher
func NewAES256GCMCipher() *AES256GCMCipher {
	return &AES256GCMCipher{}
}

func (c *AES256GCMCipher) IVSize() int {
	return 12
}

// Encrypt encrypts data using AES-256-GCM
func (c *AES256GCMCipher) Encrypt(data, key, iv []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("key must be 32 bytes for AES-256")
	}
	if len(iv) < 12 {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("IV must be at least 12 bytes for GCM")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	// Use first 12 bytes of IV as nonce for GCM
	nonce := iv[:12]

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}

	ciphertext := aesGCM.Seal(nil, nonce, data, nil)
	return ciphertext, nil
}

// Decrypt decrypts data using AES-256-GCM
func (c *AES256GCMCipher) Decrypt(data, key, iv []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("key must be 32 bytes for AES-256")
	}
	if len(iv) < 12 {
		return nil, cerrors.ErrDecryptionFailed.GenWithStackByArgs("IV must be at least 12 bytes for GCM")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	// Use first 12 bytes of IV as nonce for GCM
	nonce := iv[:12]

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	plaintext, err := aesGCM.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, cerrors.ErrDecryptionFailed.Wrap(err)
	}

	return plaintext, nil
}

// GetCipher returns a Cipher implementation for the given algorithm
func GetCipher(algorithm EncryptionAlgorithm) (Cipher, error) {
	switch algorithm {
	case AES256CTR:
		return NewAES256CTRCipher(), nil
	case AES256GCM:
		return NewAES256GCMCipher(), nil
	default:
		return nil, cerrors.ErrUnsupportedEncryptionAlgorithm.GenWithStackByArgs("algorithm: " + string(algorithm))
	}
}

// GenerateIV generates a random IV of the specified size
func GenerateIV(size int) ([]byte, error) {
	iv := make([]byte, size)
	if _, err := rand.Read(iv); err != nil {
		return nil, cerrors.ErrEncryptionFailed.Wrap(err)
	}
	return iv, nil
}
