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

type DMLOrder uint16

const (
	// DML type order, used for sorting.
	DMLOrderDelete DMLOrder = iota + 1
	DMLOrderUpdate
	DMLOrderInsert
)

type CompressionType uint16

const (
	CompressionNone CompressionType = iota
	CompressionZSTD
)

const (
	encodedKeyUint64Len        = 8
	encodedKeyTxnCommitTsStart = 2 * encodedKeyUint64Len
	encodedKeyTxnCommitTsEnd   = encodedKeyTxnCommitTsStart + encodedKeyUint64Len
	encodedKeyAttributesOffset = 4 * encodedKeyUint64Len
	encodedKeyAttributesEnd    = encodedKeyAttributesOffset + 2
)

const (
	// Bitmask for DML order and compression type.
	dmlOrderMask    = 0xFF00 // DML order is stored in the high 8 bits for sorting.
	compressionMask = 0x00FF // Compression type is stored in the low 8 bits.
	dmlOrderShift   = 8
)

// EncodeTxnCommitTsBoundaryKey encodes the event-store key boundary up to txnCommitTs.
func EncodeTxnCommitTsBoundaryKey(uniqueID uint64, tableID int64, txnCommitTs uint64) []byte {
	buf := make([]byte, encodedKeyTxnCommitTsEnd)
	encodeTxnCommitTsBoundaryKeyTo(buf, uniqueID, tableID, txnCommitTs)
	return buf
}

func encodeScanLowerBound(uniqueID uint64, tableID int64, txnCommitTs uint64, txnStartTs uint64) []byte {
	buf := make([]byte, encodedKeyAttributesOffset)
	encodeTxnCommitTsBoundaryKeyTo(buf, uniqueID, tableID, txnCommitTs)
	binary.BigEndian.PutUint64(buf[encodedKeyTxnCommitTsEnd:encodedKeyAttributesOffset], txnStartTs)
	return buf
}

func encodeTxnCommitTsBoundaryKeyTo(buf []byte, uniqueID uint64, tableID int64, txnCommitTs uint64) {
	binary.BigEndian.PutUint64(buf[:encodedKeyUint64Len], uniqueID)
	binary.BigEndian.PutUint64(buf[encodedKeyUint64Len:encodedKeyTxnCommitTsStart], uint64(tableID))
	binary.BigEndian.PutUint64(buf[encodedKeyTxnCommitTsStart:encodedKeyTxnCommitTsEnd], txnCommitTs)
}

func encodedKeyLen(event *common.RawKVEntry) int {
	// uniqueID, tableID, txnCommitTs, txnStartTs, Put/Delete, CompressionType, Key
	return encodedKeyAttributesEnd + len(event.Key)
}

// EncodeKeyTo appends an encoded event-store key to buf.
// Format: uniqueID, tableID, txnCommitTs, txnStartTs, delete/update/insert, Key.
func EncodeKeyTo(
	buf []byte,
	uniqueID uint64,
	tableID int64,
	event *common.RawKVEntry,
	compressionType CompressionType,
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
	combinedOrder := uint16(compressionType) | (uint16(dmlOrder) << dmlOrderShift)
	buf = binary.BigEndian.AppendUint16(buf, combinedOrder)
	// key
	return append(buf, event.Key...)
}

// EncodeKey encodes a key according to event.
func EncodeKey(uniqueID uint64, tableID int64, event *common.RawKVEntry, compressionType CompressionType) []byte {
	return EncodeKeyTo(make([]byte, 0, encodedKeyLen(event)), uniqueID, tableID, event, compressionType)
}

// DecodeKeyAttributes decodes compression type and dml order from the key.
func DecodeKeyAttributes(key []byte) (DMLOrder, CompressionType) {
	combinedOrder := binary.BigEndian.Uint16(key[encodedKeyAttributesOffset:encodedKeyAttributesEnd])
	return DMLOrder((combinedOrder & dmlOrderMask) >> dmlOrderShift), CompressionType(combinedOrder & compressionMask)
}

// decodeTxnCommitTsFromEncodedKey decodes txnCommitTs from an event-store key boundary.
// It works for both full event keys and DeleteRange boundary keys because both
// contain uniqueID, tableID, and txnCommitTs as the first three fields.
func decodeTxnCommitTsFromEncodedKey(key []byte) (uint64, bool) {
	if len(key) < encodedKeyTxnCommitTsEnd {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[encodedKeyTxnCommitTsStart:encodedKeyTxnCommitTsEnd]), true
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
	start := EncodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs)
	end := EncodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs)

	return db.DeleteRange(start, end, pebble.NoSync)
}

func compactDataRange(
	db *pebble.DB, uniqueKeyID uint64, tableID int64, startTxnCommitTs uint64, endTxnCommitTs uint64,
) error {
	start := EncodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, startTxnCommitTs)
	end := EncodeTxnCommitTsBoundaryKey(uniqueKeyID, tableID, endTxnCommitTs)

	return db.Compact(start, end, false)
}
