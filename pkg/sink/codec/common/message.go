// Copyright 2024 PingCAP, Inc.
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

package common

import (
	"encoding/binary"
	"encoding/json"
)

// MaxRecordOverhead is used to calculate message size.
// For TiCDC, minimum supported kafka version is `0.11.0.2`,
// which will be treated as `version = 2` by producer.
const MaxRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1

// MessageType is the type of message, which is used by MqSink and RedoLog.
type MessageType int

const (
	// MessageTypeUnknown is unknown type of message key
	MessageTypeUnknown MessageType = iota
	// MessageTypeRow is row type of message key
	MessageTypeRow
	// MessageTypeDDL is ddl type of message key
	MessageTypeDDL
	// MessageTypeResolved is resolved type of message key
	MessageTypeResolved
)

// Message represents an message to the sink
type Message struct {
	Key       []byte
	Value     []byte
	rowsCount int    // rows in one Message
	Callback  func() // Callback function will be called when the message is sent to the sink.
}

// Length returns the expected size of the Kafka message
// We didn't append any `Headers` when send the message, so ignore the calculations related to it.
// If `ProducerMessage` Headers fields used, this method should also adjust.
func (m *Message) Length() int {
	return len(m.Key) + len(m.Value) + MaxRecordOverhead
}

// GetRowsCount returns the number of rows batched in one Message
func (m *Message) GetRowsCount() int {
	return m.rowsCount
}

// SetRowsCount set the number of rows
func (m *Message) SetRowsCount(cnt int) {
	m.rowsCount = cnt
}

// IncRowsCount increase the number of rows
func (m *Message) IncRowsCount() {
	m.rowsCount++
}

// NewMsg should be used when creating a Message struct.
// It copies the input byte slices to avoid any surprises in asynchronous MQ writes.
func NewMsg(
	key []byte,
	value []byte,
) *Message {
	ret := &Message{
		Key:       nil,
		Value:     nil,
		rowsCount: 0,
	}

	if key != nil {
		ret.Key = make([]byte, len(key))
		copy(ret.Key, key)
	}

	if value != nil {
		ret.Value = make([]byte, len(value))
		copy(ret.Value, value)
	}

	return ret
}

// ClaimCheckMessage is the message sent to the claim-check external storage.
type ClaimCheckMessage struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// UnmarshalClaimCheckMessage unmarshal bytes to ClaimCheckMessage.
func UnmarshalClaimCheckMessage(data []byte) (*ClaimCheckMessage, error) {
	var m ClaimCheckMessage
	err := json.Unmarshal(data, &m)
	return &m, err
}
