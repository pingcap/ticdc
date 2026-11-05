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

package eventstore

import (
	"encoding/hex"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestEventStoreKeyFormatGolden(t *testing.T) {
	t.Parallel()

	const (
		uniqueID    = uint64(0x0102030405060708)
		tableID     = int64(0x1112131415161718)
		txnCommitTs = uint64(0x2122232425262728)
		txnStartTs  = uint64(0x3132333435363738)
	)
	event := &common.RawKVEntry{
		OpType:  common.OpTypePut,
		CRTs:    txnCommitTs,
		StartTs: txnStartTs,
		Key:     []byte{0x41, 0x42},
	}

	key := EncodeKey(uniqueID, tableID, event, CompressionZSTD)
	expectedKey := mustDecodeHex(t, "0102030405060708111213141516171821222324252627283132333435363738030100000000000000004142")
	require.Equal(t, expectedKey, key)
	require.Equal(t, len(expectedKey), encodedKeyLen(event))

	require.Equal(t, 8, encodedKeyUniqueIDLen)
	require.Equal(t, 8, encodedKeyTableIDLen)
	require.Equal(t, 8, encodedKeyTxnCommitTsLen)
	require.Equal(t, 8, encodedKeyTxnStartTsLen)
	require.Equal(t, 1, encodedKeyDMLOrderLen)
	require.Equal(t, 1, encodedKeyCompressionLen)
	require.Equal(t, 8, encodedKeyMaskLen)

	require.Equal(t, 0, encodedKeyUniqueIDOffset)
	require.Equal(t, 8, encodedKeyTableIDOffset)
	require.Equal(t, 16, encodedKeyTxnCommitTsOffset)
	require.Equal(t, 24, encodedKeyTxnStartTsOffset)
	require.Equal(t, 32, encodedKeyDMLOrderOffset)
	require.Equal(t, 32, encodedKeyAttributesOffset)
	require.Equal(t, 33, encodedKeyCompressionOffset)
	require.Equal(t, 34, encodedKeyMaskOffset)
	require.Equal(t, 42, encodedKeyAttributesEnd)
	require.Equal(t, encodedKeyTableIDOffset, encodedKeyUniqueIDOffset+encodedKeyUniqueIDLen)
	require.Equal(t, encodedKeyTxnCommitTsOffset, encodedKeyTableIDOffset+encodedKeyTableIDLen)
	require.Equal(t, encodedKeyTxnStartTsOffset, encodedKeyTxnCommitTsOffset+encodedKeyTxnCommitTsLen)
	require.Equal(t, encodedKeyDMLOrderOffset, encodedKeyTxnStartTsOffset+encodedKeyTxnStartTsLen)
	require.Equal(t, encodedKeyCompressionOffset, encodedKeyDMLOrderOffset+encodedKeyDMLOrderLen)
	require.Equal(t, encodedKeyMaskOffset, encodedKeyCompressionOffset+encodedKeyCompressionLen)
	require.Equal(t, encodedKeyAttributesEnd, encodedKeyMaskOffset+encodedKeyMaskLen)

	require.Equal(t, expectedKey[:encodedKeyTxnCommitTsOffset+encodedKeyTxnCommitTsLen],
		encodeTxnCommitTsBoundaryKey(uniqueID, tableID, txnCommitTs))
	require.Equal(t, expectedKey[:encodedKeyTxnStartTsOffset+encodedKeyTxnStartTsLen],
		encodeScanLowerBound(uniqueID, tableID, txnCommitTs, txnStartTs))

	decodedTxnCommitTs, ok := decodeTxnCommitTsFromEncodedKey(key)
	require.True(t, ok)
	require.Equal(t, txnCommitTs, decodedTxnCommitTs)

	dmlOrder, compressionType := DecodeKeyAttributes(key)
	require.Equal(t, DMLOrderInsert, dmlOrder)
	require.Equal(t, CompressionZSTD, compressionType)
	require.False(t, KeyUsesEncryptionLayer(key))

	keyWithEncryptionLayer := encodeKeyToWithEncryptionLayer(make([]byte, 0, encodedKeyLen(event)), uniqueID, tableID, event, CompressionZSTD)
	expectedKeyWithEncryptionLayer := mustDecodeHex(t, "0102030405060708111213141516171821222324252627283132333435363738030100000000000000014142")
	require.Equal(t, expectedKeyWithEncryptionLayer, keyWithEncryptionLayer)
	require.True(t, KeyUsesEncryptionLayer(keyWithEncryptionLayer))
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
