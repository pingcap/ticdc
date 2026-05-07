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

package eventstore

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

type DMLOrder uint8

const (
	// DML type order, used for sorting.
	DMLOrderDelete DMLOrder = iota + 1
	DMLOrderUpdate
	DMLOrderInsert
)

type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionZSTD
)

const (
	// Encoded event-store key layout:
	//
	//   byte offset
	//   0                   8                  16                 24                 32 33 34                 42
	//   |-------------------|------------------|------------------|------------------|--|--|------------------|
	//   | uniqueID (8B)     | tableID (8B)     | txnCommitTs (8B) | txnStartTs (8B)  |DO|CT| mask (8B)       | key...
	//   |-------------------|------------------|------------------|------------------|--|--|------------------|
	//
	//   DO: DMLOrder (1 byte)
	//   CT: CompressionType (1 byte)
	//
	// Mask bits:
	//   bit 0: value passed through the encryption layer
	//   bit 1+: reserved
	encodedKeyUniqueIDOffset    = 0
	encodedKeyTableIDOffset     = encodedKeyUniqueIDOffset + 8
	encodedKeyTxnCommitTsOffset = encodedKeyTableIDOffset + 8
	encodedKeyTxnStartTsOffset  = encodedKeyTxnCommitTsOffset + 8
	encodedKeyDMLOrderOffset    = encodedKeyTxnStartTsOffset + 8
	encodedKeyCompressionOffset = encodedKeyDMLOrderOffset + 1
	encodedKeyMaskOffset        = encodedKeyCompressionOffset + 1

	encodedKeyAttributesOffset = encodedKeyDMLOrderOffset
	encodedKeyAttributesEnd    = encodedKeyMaskOffset + 8
)

const (
	// encodedKeyMaskEncryption indicates the value passed through the encryption layer.
	encodedKeyMaskEncryption uint64 = 1 << iota
)

// encodeTxnCommitTsBoundaryKey encodes the event-store key boundary up to txnCommitTs.
func encodeTxnCommitTsBoundaryKey(uniqueID uint64, tableID int64, txnCommitTs uint64) []byte {
	buf := make([]byte, encodedKeyTxnStartTsOffset)
	encodeTxnCommitTsBoundaryKeyTo(buf, uniqueID, tableID, txnCommitTs)
	return buf
}

func encodeScanLowerBound(uniqueID uint64, tableID int64, txnCommitTs uint64, txnStartTs uint64) []byte {
	buf := make([]byte, encodedKeyAttributesOffset)
	encodeTxnCommitTsBoundaryKeyTo(buf, uniqueID, tableID, txnCommitTs)
	binary.BigEndian.PutUint64(buf[encodedKeyTxnStartTsOffset:encodedKeyDMLOrderOffset], txnStartTs)
	return buf
}

func encodeTxnCommitTsBoundaryKeyTo(buf []byte, uniqueID uint64, tableID int64, txnCommitTs uint64) {
	binary.BigEndian.PutUint64(buf[encodedKeyUniqueIDOffset:encodedKeyTableIDOffset], uniqueID)
	binary.BigEndian.PutUint64(buf[encodedKeyTableIDOffset:encodedKeyTxnCommitTsOffset], uint64(tableID))
	binary.BigEndian.PutUint64(buf[encodedKeyTxnCommitTsOffset:encodedKeyTxnStartTsOffset], txnCommitTs)
}

func encodedKeyLen(event *common.RawKVEntry) int {
	// uniqueID, tableID, txnCommitTs, txnStartTs, DMLOrder, CompressionType, Mask, Key
	return encodedKeyAttributesEnd + len(event.Key)
}

func encodeKeyTo(
	buf []byte,
	uniqueID uint64,
	tableID int64,
	event *common.RawKVEntry,
	compressionType CompressionType,
	usesEncryptionLayer bool,
) []byte {
	if event == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// unique ID
	buf = binary.BigEndian.AppendUint64(buf, uniqueID)
	// table ID
	buf = binary.BigEndian.AppendUint64(buf, uint64(tableID))
	// txn commit ts
	buf = binary.BigEndian.AppendUint64(buf, event.CRTs)
	// txn start ts
	buf = binary.BigEndian.AppendUint64(buf, event.StartTs)
	// Let Delete < Update < Insert
	dmlOrder := getDMLOrder(event)
	buf = append(buf, byte(dmlOrder))
	buf = append(buf, byte(compressionType))
	mask := uint64(0)
	if usesEncryptionLayer {
		mask |= encodedKeyMaskEncryption
	}
	buf = binary.BigEndian.AppendUint64(buf, mask)
	// key
	return append(buf, event.Key...)
}

// EncodeKeyTo appends an encoded event-store key to buf.
//
//	| uniqueID | tableID | txnCommitTs | txnStartTs | dmlOrder | compressionType | mask | key |
//	|   8B     |   8B    |     8B      |     8B     |    1B    |       1B        |  8B  | ... |
func EncodeKeyTo(
	buf []byte,
	uniqueID uint64,
	tableID int64,
	event *common.RawKVEntry,
	compressionType CompressionType,
) []byte {
	return encodeKeyTo(buf, uniqueID, tableID, event, compressionType, false)
}

func encodeKeyToWithEncryptionLayer(
	buf []byte,
	uniqueID uint64,
	tableID int64,
	event *common.RawKVEntry,
	compressionType CompressionType,
) []byte {
	return encodeKeyTo(buf, uniqueID, tableID, event, compressionType, true)
}

// EncodeKey encodes a key according to event.
func EncodeKey(uniqueID uint64, tableID int64, event *common.RawKVEntry, compressionType CompressionType) []byte {
	return EncodeKeyTo(make([]byte, 0, encodedKeyLen(event)), uniqueID, tableID, event, compressionType)
}

// DecodeKeyAttributes decodes compression type and dml order from the key.
func DecodeKeyAttributes(key []byte) (DMLOrder, CompressionType) {
	return DMLOrder(key[encodedKeyDMLOrderOffset]), CompressionType(key[encodedKeyCompressionOffset])
}

func KeyUsesEncryptionLayer(key []byte) bool {
	mask := binary.BigEndian.Uint64(key[encodedKeyMaskOffset:encodedKeyAttributesEnd])
	return mask&encodedKeyMaskEncryption != 0
}

// decodeTxnCommitTsFromEncodedKey decodes txnCommitTs from an event-store key boundary.
// It works for both full event keys and DeleteRange boundary keys because both
// contain uniqueID, tableID, and txnCommitTs as the first three fields.
func decodeTxnCommitTsFromEncodedKey(key []byte) (uint64, bool) {
	if len(key) < encodedKeyTxnStartTsOffset {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[encodedKeyTxnCommitTsOffset:encodedKeyTxnStartTsOffset]), true
}

// getDMLOrder returns the order of the dml types: delete<update<insert
func getDMLOrder(rowKV *common.RawKVEntry) DMLOrder {
	if rowKV.OpType == common.OpTypeDelete {
		return DMLOrderDelete
	} else if rowKV.OldValue != nil {
		return DMLOrderUpdate
	}
	return DMLOrderInsert
}

func deleteDataRange(
	db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
) error {
	start := encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs)
	end := encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs)

	return db.DeleteRange(start, end, pebble.NoSync)
}

func compactDataRange(
	db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
) error {
	start := encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs)
	end := encodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs)

	return db.Compact(start, end, false)
}
