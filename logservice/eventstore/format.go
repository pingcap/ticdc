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
	encodedKeyUint64Len = 8
	encodedKeyOrderLen  = 2

	encodedKeyUniqueIDOffset = 0
	encodedKeyTableIDOffset  = encodedKeyUniqueIDOffset + encodedKeyUint64Len
	encodedKeyCRTsOffset     = encodedKeyTableIDOffset + encodedKeyUint64Len
	encodedKeyCRTsEnd        = encodedKeyCRTsOffset + encodedKeyUint64Len
	encodedKeyStartTsOffset  = encodedKeyCRTsEnd
	encodedKeyStartTsEnd     = encodedKeyStartTsOffset + encodedKeyUint64Len
	encodedKeyMetasOffset    = encodedKeyStartTsEnd
	encodedKeyMetasEnd       = encodedKeyMetasOffset + encodedKeyOrderLen
)

const (
	// Bitmask for DML order and compression type.
	dmlOrderMask    = 0xFF00 // DML order is stored in the high 8 bits for sorting.
	compressionMask = 0x00FF // Compression type is stored in the low 8 bits.
	dmlOrderShift   = 8
)

// EncodeKeyPrefix encodes uniqueID, tableID, CRTs and StartTs.
// StartTs is optional.
// The result should be a prefix of normal key. (TODO: add a unit test)
func EncodeKeyPrefix(uniqueID uint64, tableID int64, CRTs uint64, startTs ...uint64) []byte {
	if len(startTs) > 1 {
		log.Panic("startTs should be at most one")
	}
	// uniqueID, tableID, CRTs.
	keySize := encodedKeyCRTsEnd
	if len(startTs) > 0 {
		keySize = encodedKeyStartTsEnd
	}
	buf := make([]byte, 0, keySize)
	uint64Buf := [8]byte{}
	// uniqueID
	binary.BigEndian.PutUint64(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:]...)
	// tableID
	binary.BigEndian.PutUint64(uint64Buf[:], uint64(tableID))
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], CRTs)
	buf = append(buf, uint64Buf[:]...)
	if len(startTs) > 0 {
		// startTs
		binary.BigEndian.PutUint64(uint64Buf[:], startTs[0])
		buf = append(buf, uint64Buf[:]...)
	}
	return buf
}

func encodedKeyLen(event *common.RawKVEntry) int {
	// uniqueID, tableID, CRTs, startTs, Put/Delete, CompressionType, Key
	return encodedKeyMetasEnd + len(event.Key)
}

// EncodeKeyTo appends an encoded event-store key to buf.
// Format: uniqueID, tableID, CRTs, startTs, delete/update/insert, Key.
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
	// CRTs
	buf = binary.BigEndian.AppendUint64(buf, event.CRTs)
	// startTs
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

// DecodeKeyMetas decodes compression type and dml order from the key.
func DecodeKeyMetas(key []byte) (DMLOrder, CompressionType) {
	combinedOrder := binary.BigEndian.Uint16(key[encodedKeyMetasOffset:encodedKeyMetasEnd])
	return DMLOrder((combinedOrder & dmlOrderMask) >> dmlOrderShift), CompressionType(combinedOrder & compressionMask)
}

// decodeCRTsFromEncodedKey decodes CRTs from an event-store key prefix.
// It works for both full event keys and DeleteRange boundary keys because both
// contain uniqueID, tableID, and CRTs as the first three fields.
func decodeCRTsFromEncodedKey(key []byte) (uint64, bool) {
	if len(key) < encodedKeyCRTsEnd {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[encodedKeyCRTsOffset:encodedKeyCRTsEnd]), true
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

func deleteDataRange(db *pebble.DB, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error {
	start := EncodeKeyPrefix(uniqueKeyID, tableID, startTs)
	end := EncodeKeyPrefix(uniqueKeyID, tableID, endTs)

	return db.DeleteRange(start, end, pebble.NoSync)
}

func compactDataRange(db *pebble.DB, uniqueKeyID uint64, tableID int64, startTs uint64, endTs uint64) error {
	start := EncodeKeyPrefix(uniqueKeyID, tableID, startTs)
	end := EncodeKeyPrefix(uniqueKeyID, tableID, endTs)

	return db.Compact(start, end, false)
}
