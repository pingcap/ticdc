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
	expectedKey := mustDecodeHex(t, "010203040506070811121314151617182122232425262728313233343536373803014142")
	require.Equal(t, expectedKey, key)
	require.Equal(t, len(expectedKey), encodedKeyLen(event))

	require.Equal(t, 16, encodedKeyTxnCommitTsStart)
	require.Equal(t, 24, encodedKeyTxnCommitTsEnd)
	require.Equal(t, 32, encodedKeyAttributesOffset)
	require.Equal(t, 34, encodedKeyAttributesEnd)

	require.Equal(t, expectedKey[:encodedKeyTxnCommitTsEnd],
		encodeTxnCommitTsBoundaryKey(uniqueID, tableID, txnCommitTs))
	require.Equal(t, expectedKey[:encodedKeyAttributesOffset],
		encodeScanLowerBound(uniqueID, tableID, txnCommitTs, txnStartTs))

	decodedTxnCommitTs, ok := decodeTxnCommitTsFromEncodedKey(key)
	require.True(t, ok)
	require.Equal(t, txnCommitTs, decodedTxnCommitTs)

	dmlOrder, compressionType := DecodeKeyAttributes(key)
	require.Equal(t, DMLOrderInsert, dmlOrder)
	require.Equal(t, CompressionZSTD, compressionType)
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}
