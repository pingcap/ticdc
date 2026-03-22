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

package event

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/ticdc/pkg/common"
)

const DispatcherControlEventVersion1 = 1

type DispatcherControlAction byte

const (
	DispatcherControlActionRegister DispatcherControlAction = 1
	DispatcherControlActionReset    DispatcherControlAction = 2
	DispatcherControlActionRemove   DispatcherControlAction = 3
)

type DispatcherControlStatus byte

const (
	DispatcherControlStatusAccepted DispatcherControlStatus = 1
	DispatcherControlStatusStale    DispatcherControlStatus = 2
	DispatcherControlStatusNotFound DispatcherControlStatus = 3
	DispatcherControlStatusRejected DispatcherControlStatus = 4
)

const (
	DispatcherControlReasonNone               uint64 = 0
	DispatcherControlReasonNotReusable        uint64 = 1
	DispatcherControlReasonSchemaStoreFailure uint64 = 2
	DispatcherControlReasonTableInfoFailure   uint64 = 3
)

var _ Event = &DispatcherControlEvent{}

type DispatcherControlEvent struct {
	Version      int
	DispatcherID common.DispatcherID
	Epoch        uint64
	Incarnation  uint64
	Action       DispatcherControlAction
	Status       DispatcherControlStatus
	ReasonCode   uint64
}

func NewDispatcherControlEvent(
	dispatcherID common.DispatcherID,
	epoch uint64,
	incarnation uint64,
	action DispatcherControlAction,
	status DispatcherControlStatus,
	reasonCode uint64,
) DispatcherControlEvent {
	return DispatcherControlEvent{
		Version:      DispatcherControlEventVersion1,
		DispatcherID: dispatcherID,
		Epoch:        epoch,
		Incarnation:  incarnation,
		Action:       action,
		Status:       status,
		ReasonCode:   reasonCode,
	}
}

func (e *DispatcherControlEvent) String() string {
	return fmt.Sprintf("DispatcherControlEvent{Version:%d DispatcherID:%s Epoch:%d Incarnation:%d Action:%d Status:%d Reason:%d}",
		e.Version, e.DispatcherID, e.Epoch, e.Incarnation, e.Action, e.Status, e.ReasonCode)
}

func (e *DispatcherControlEvent) GetType() int {
	return TypeDispatcherControlEvent
}

func (e *DispatcherControlEvent) GetSeq() uint64 {
	return 0
}

func (e *DispatcherControlEvent) GetEpoch() uint64 {
	return e.Epoch
}

func (e *DispatcherControlEvent) GetDispatcherID() common.DispatcherID {
	return e.DispatcherID
}

func (e *DispatcherControlEvent) GetCommitTs() common.Ts {
	return 0
}

func (e *DispatcherControlEvent) GetStartTs() common.Ts {
	return 0
}

func (e *DispatcherControlEvent) GetSize() int64 {
	return int64(e.DispatcherID.GetSize() + 8 + 8 + 1 + 1 + 8)
}

func (e *DispatcherControlEvent) IsPaused() bool {
	return false
}

func (e *DispatcherControlEvent) Len() int32 {
	return 0
}

func (e DispatcherControlEvent) Marshal() ([]byte, error) {
	payload, err := e.encodeV1()
	if err != nil {
		return nil, err
	}
	return MarshalEventWithHeader(TypeDispatcherControlEvent, e.Version, payload)
}

func (e *DispatcherControlEvent) Unmarshal(data []byte) error {
	payload, version, err := ValidateAndExtractPayload(data, TypeDispatcherControlEvent)
	if err != nil {
		return err
	}
	e.Version = version
	switch version {
	case DispatcherControlEventVersion1:
		return e.decodeV1(payload)
	default:
		return fmt.Errorf("unsupported DispatcherControlEvent version: %d", version)
	}
}

func (e DispatcherControlEvent) encodeV1() ([]byte, error) {
	payloadSize := e.DispatcherID.GetSize() + 8 + 8 + 1 + 1 + 8
	data := make([]byte, payloadSize)
	offset := 0
	copy(data[offset:], e.DispatcherID.Marshal())
	offset += e.DispatcherID.GetSize()
	binary.BigEndian.PutUint64(data[offset:], e.Epoch)
	offset += 8
	binary.BigEndian.PutUint64(data[offset:], e.Incarnation)
	offset += 8
	data[offset] = byte(e.Action)
	offset++
	data[offset] = byte(e.Status)
	offset++
	binary.BigEndian.PutUint64(data[offset:], e.ReasonCode)
	return data, nil
}

func (e *DispatcherControlEvent) decodeV1(data []byte) error {
	offset := 0
	if err := e.DispatcherID.Unmarshal(data[offset : offset+e.DispatcherID.GetSize()]); err != nil {
		return err
	}
	offset += e.DispatcherID.GetSize()
	e.Epoch = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	e.Incarnation = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	e.Action = DispatcherControlAction(data[offset])
	offset++
	e.Status = DispatcherControlStatus(data[offset])
	offset++
	e.ReasonCode = binary.BigEndian.Uint64(data[offset:])
	return nil
}
