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

	"github.com/stretchr/testify/require"
)

func TestAES256CTREncryptDecrypt(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef") // 32 bytes
	iv := []byte("1234567890abcdef")                  // 16 bytes
	plain := []byte("hello world")

	cipherImpl := NewAES256CTRCipher()
	encrypted, err := cipherImpl.Encrypt(plain, key, iv)
	require.NoError(t, err)
	require.NotEqual(t, plain, encrypted)

	decrypted, err := cipherImpl.Decrypt(encrypted, key, iv)
	require.NoError(t, err)
	require.Equal(t, plain, decrypted)
}
