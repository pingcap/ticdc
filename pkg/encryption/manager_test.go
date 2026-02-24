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
	"crypto/aes"
	"crypto/cipher"
	"testing"

	"github.com/pingcap/ticdc/pkg/encryption/kms"
	"github.com/stretchr/testify/require"
)

type staticKMSClient struct {
	plaintext []byte
}

func (c *staticKMSClient) DecryptMasterKey(ctx context.Context, ciphertext []byte, keyID string, vendor string, region string, endpoint string) ([]byte, error) {
	return c.plaintext, nil
}

var _ kms.KMSClient = (*staticKMSClient)(nil)

type staticTiKVEncryptionClient struct {
	meta *EncryptionMeta
	err  error
}

func (c *staticTiKVEncryptionClient) GetKeyspaceEncryptionMeta(ctx context.Context, keyspaceID uint32) (*EncryptionMeta, error) {
	return c.meta, c.err
}

func TestEncryptionMetaManagerDecryptDataKeyUsesZeroIV(t *testing.T) {
	t.Parallel()

	masterKeyPlaintext := make([]byte, 32)
	for i := range masterKeyPlaintext {
		masterKeyPlaintext[i] = byte(i + 1)
	}

	dataKeyPlaintext := make([]byte, 32)
	for i := range dataKeyPlaintext {
		dataKeyPlaintext[i] = byte(0xA0 + i)
	}

	block, err := aes.NewCipher(masterKeyPlaintext)
	require.NoError(t, err)
	iv := make([]byte, aes.BlockSize)
	stream := cipher.NewCTR(block, iv)
	dataKeyCiphertext := make([]byte, len(dataKeyPlaintext))
	stream.XORKeyStream(dataKeyCiphertext, dataKeyPlaintext)

	dataKeyID := uint32(0x4b3031) // "K01"

	meta := &EncryptionMeta{
		KeyspaceId: 1,
		Current: &EncryptionEpoch{
			FileId:    1,
			DataKeyId: dataKeyID,
			CreatedAt: 0,
		},
		MasterKey: &MasterKey{
			Vendor:     "aws-kms",
			CmekId:     "cmek-1",
			Region:     "us-west-1",
			Ciphertext: []byte{1, 2, 3},
		},
		DataKeys: map[uint32]*DataKey{
			dataKeyID: {Ciphertext: dataKeyCiphertext},
		},
	}

	tikvClient := &staticTiKVEncryptionClient{meta: meta}
	kmsClient := &staticKMSClient{plaintext: masterKeyPlaintext}
	mgr := NewEncryptionMetaManager(tikvClient, kmsClient)

	gotKey, err := mgr.GetDataKey(context.Background(), 1, "K01")
	require.NoError(t, err)
	require.Equal(t, dataKeyPlaintext, gotKey)
}
