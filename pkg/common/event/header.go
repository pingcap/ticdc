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
	eventMagic = 0xDA7A6A6A
	// Header layout: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	// Total header size: 8 bytes
	eventHeaderSize = 16
)

// MarshalEventWithHeader wraps a payload with a standard event header.
// The header contains:
// - Magic bytes (4B): for format validation
// - Event type (2B): identifies the specific event type (DDL, DML, etc.)
// - Version (2B): version of the event payload format
// - Payload length (8B): length of the payload in bytes
//
// This function does not enforce a maximum payload size, as the upper layer
// (protobuf) will handle size constraints.
func MarshalEventWithHeader(eventType int, version int, payload []byte) ([]byte, error) {
	header := make([]byte, eventHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], eventMagic)
	binary.BigEndian.PutUint16(header[4:6], uint16(eventType))
	binary.BigEndian.PutUint16(header[6:8], uint16(version))
	binary.BigEndian.PutUint64(header[8:16], uint64(len(payload)))

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
func UnmarshalEventHeader(data []byte) (eventType int, version int, payloadLen uint64, err error) {
	// 1. Validate minimum header size
	if len(data) < eventHeaderSize {
		return 0, 0, 0, fmt.Errorf("data too short: need at least %d bytes for header, got %d",
			eventHeaderSize, len(data))
	}

	// 2. Validate magic bytes
	if binary.BigEndian.Uint32(data[0:4]) != eventMagic {
		return 0, 0, 0, fmt.Errorf("invalid magic bytes: expected [0x%08X], got [0x%08X]",
			eventMagic, binary.BigEndian.Uint32(data[0:4]))
	}

	// 3. Extract header fields
	eventType = int(binary.BigEndian.Uint16(data[4:6]))
	version = int(binary.BigEndian.Uint16(data[6:8]))
	payloadLen = binary.BigEndian.Uint64(data[8:16])

	return eventType, version, payloadLen, nil
}

// GetEventHeaderSize returns the size of the event header in bytes.
func GetEventHeaderSize() int {
	return eventHeaderSize
}

// ValidateAndExtractPayload validates the event header and extracts the payload.
// It performs the following steps:
// 1. Parse the unified header
// 2. Validate the event type matches the expected type
// 3. Validate the total data length
// 4. Extract and return the payload
//
// Returns:
// - payload: the extracted payload bytes
// - version: the version of the event
// - err: any error encountered during validation
func ValidateAndExtractPayload(data []byte, expectedType int) (payload []byte, version int, err error) {
	// 1. Parse unified header
	eventType, ver, payloadLen, err := UnmarshalEventHeader(data)
	if err != nil {
		return nil, 0, err
	}

	// 2. Validate event type
	if eventType != expectedType {
		return nil, 0, fmt.Errorf("expected %s (type %d), got type %d (%s)",
			TypeToString(expectedType), expectedType, eventType, TypeToString(eventType))
	}

	// 3. Validate total data length
	headerSize := GetEventHeaderSize()
	expectedLen := uint64(headerSize) + payloadLen
	if uint64(len(data)) < expectedLen {
		return nil, 0, fmt.Errorf("incomplete data: expected %d bytes (header %d + payload %d), got %d",
			expectedLen, headerSize, payloadLen, len(data))
	}

	// 4. Extract payload
	payload = data[headerSize:expectedLen]

	return payload, ver, nil
}
