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

package event

import (
	"encoding/binary"
	"fmt"
)

const (
	// Magic bytes for event format validation
	// All event types share the same magic number to identify them as valid events
	eventMagicHi = 0xDA
	eventMagicLo = 0x7A

	// Header layout: [MAGIC(2B)][EVENT_TYPE(1B)][VERSION(1B)][PAYLOAD_LENGTH(4B)]
	// Total header size: 8 bytes
	eventHeaderSize = 8
)

// MarshalEventWithHeader wraps a payload with a standard event header.
// The header contains:
// - Magic bytes (2B): for format validation
// - Event type (1B): identifies the specific event type (DDL, DML, etc.)
// - Version (1B): version of the event payload format
// - Payload length (4B): length of the payload in bytes
//
// This function does not enforce a maximum payload size, as the upper layer
// (protobuf) will handle size constraints.
func MarshalEventWithHeader(eventType int, version byte, payload []byte) ([]byte, error) {
	header := make([]byte, eventHeaderSize)
	header[0] = eventMagicHi
	header[1] = eventMagicLo
	header[2] = byte(eventType)
	header[3] = version
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))

	result := make([]byte, 0, eventHeaderSize+len(payload))
	result = append(result, header...)
	result = append(result, payload...)

	return result, nil
}

// UnmarshalEventHeader parses the event header from the given data.
// It returns:
// - eventType: the type of event (TypeDDLEvent, TypeDMLEvent, etc.)
// - version: the version of the payload format
// - payloadLen: the length of the payload in bytes
// - err: any error encountered during parsing
//
// This function validates:
// - Minimum data length (at least header size)
// - Magic bytes correctness
func UnmarshalEventHeader(data []byte) (eventType int, version byte, payloadLen int, err error) {
	// 1. Validate minimum header size
	if len(data) < eventHeaderSize {
		return 0, 0, 0, fmt.Errorf("data too short: need at least %d bytes for header, got %d",
			eventHeaderSize, len(data))
	}

	// 2. Validate magic bytes
	if data[0] != eventMagicHi || data[1] != eventMagicLo {
		return 0, 0, 0, fmt.Errorf("invalid magic bytes: expected [0x%02X, 0x%02X], got [0x%02X, 0x%02X]",
			eventMagicHi, eventMagicLo, data[0], data[1])
	}

	// 3. Extract header fields
	eventType = int(data[2])
	version = data[3]
	payloadLen = int(binary.BigEndian.Uint32(data[4:8]))

	// 4. Validate payload length is non-negative
	if payloadLen < 0 {
		return 0, 0, 0, fmt.Errorf("invalid payload length: %d", payloadLen)
	}

	return eventType, version, payloadLen, nil
}

// GetEventHeaderSize returns the size of the event header in bytes.
func GetEventHeaderSize() int {
	return eventHeaderSize
}
