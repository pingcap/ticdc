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
	keySize := 8 + 8 + 8
	if len(startTs) > 0 {
		keySize += 8
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

// EncodeKey encodes a key according to event.
// Format: uniqueID, tableID, CRTs, startTs, delete/update/insert, Key.
func EncodeKey(uniqueID uint64, tableID int64, event *common.RawKVEntry, compressionType CompressionType) []byte {
	if event == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// uniqueID, tableID, CRTs, startTs, Put/Delete, CompressionType, Key
	length := 8 + 8 + 8 + 8 + 1 + 1 + len(event.Key)
	buf := make([]byte, 0, length)
	uint64Buf := [8]byte{}
	// unique ID
	binary.BigEndian.PutUint64(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:]...)
	// table ID
	binary.BigEndian.PutUint64(uint64Buf[:], uint64(tableID))
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.CRTs)
	buf = append(buf, uint64Buf[:]...)
	// startTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.StartTs)
	buf = append(buf, uint64Buf[:]...)
	// Let Delete < Update < Insert
	dmlOrder := getDMLOrder(event)
	combinedOrder := uint16(compressionType) | (uint16(dmlOrder) << dmlOrderShift)
	binary.BigEndian.PutUint16(uint64Buf[:], combinedOrder)
	buf = append(buf, uint64Buf[:2]...)
	// key
	return append(buf, event.Key...)
}

// DecodeKeyMetas decodes compression type and dml order from the key.
func DecodeKeyMetas(key []byte) (DMLOrder, CompressionType) {
	combinedOrder := binary.BigEndian.Uint16(key[32:34]) // The combined order is at offset 32 for 2 bytes.
	return DMLOrder((combinedOrder & dmlOrderMask) >> dmlOrderShift), CompressionType(combinedOrder & compressionMask)
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
