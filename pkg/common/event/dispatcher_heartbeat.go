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
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/pingcap/ticdc/pkg/common"
)

const (
	DispatcherHeartbeatVersion = 0
)

// DispatcherProgress is used to report the progress of a dispatcher to the EventService
type DispatcherProgress struct {
	Version      byte // 1 byte
	DispatcherID common.DispatcherID
	CheckpointTs uint64 // 8 bytes
}

func (dp DispatcherProgress) GetSize() int {
	return dp.DispatcherID.GetSize() + 8 + 1 // version
}

func (dp DispatcherProgress) Marshal() ([]byte, error) {
	return dp.encodeV0()
}

func (dp *DispatcherProgress) Unmarshal(data []byte) error {
	return dp.decodeV0(data)
}

func (dp DispatcherProgress) encodeV0() ([]byte, error) {
	if dp.Version != 0 {
		return nil, errors.New("invalid version")
	}
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(dp.Version)
	buf.Write(dp.DispatcherID.Marshal())
	binary.Write(buf, binary.BigEndian, dp.CheckpointTs)
	return buf.Bytes(), nil
}

func (dp *DispatcherProgress) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	dp.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	dp.DispatcherID.Unmarshal(buf.Next(dp.DispatcherID.GetSize()))
	dp.CheckpointTs = binary.BigEndian.Uint64(buf.Next(8))
	return nil
}

// DispatcherHeartbeat is used to report the progress of a dispatcher to the EventService
type DispatcherHeartbeat struct {
	Version              byte
	DispatcherCount      uint32
	DispatcherProgresses []DispatcherProgress
}

func NewDispatcherHeartbeat(dispatcherCount int) *DispatcherHeartbeat {
	return &DispatcherHeartbeat{
		Version:              DispatcherHeartbeatVersion,
		DispatcherProgresses: make([]DispatcherProgress, 0, dispatcherCount),
	}
}

func (d *DispatcherHeartbeat) Append(dp DispatcherProgress) {
	d.DispatcherProgresses = append(d.DispatcherProgresses, dp)
}

func (d *DispatcherHeartbeat) GetSize() int {
	size := 1 // version
	size += 4 // dispatcher count
	for _, dp := range d.DispatcherProgresses {
		size += dp.GetSize()
	}
	return size
}

func (d *DispatcherHeartbeat) Marshal() ([]byte, error) {
	return d.encodeV0()
}

func (d *DispatcherHeartbeat) Unmarshal(data []byte) error {
	return d.decodeV0(data)
}

func (d *DispatcherHeartbeat) encodeV0() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.WriteByte(d.Version)
	binary.Write(buf, binary.BigEndian, d.DispatcherCount)
	for _, dp := range d.DispatcherProgresses {
		dpData, err := dp.Marshal()
		if err != nil {
			return nil, err
		}
		buf.Write(dpData)
	}
	return buf.Bytes(), nil
}

func (d *DispatcherHeartbeat) decodeV0(data []byte) error {
	buf := bytes.NewBuffer(data)
	var err error
	d.Version, err = buf.ReadByte()
	if err != nil {
		return err
	}
	d.DispatcherCount = binary.BigEndian.Uint32(buf.Next(4))
	d.DispatcherProgresses = make([]DispatcherProgress, 0, d.DispatcherCount)
	for range d.DispatcherCount {
		var dp DispatcherProgress
		dpData := buf.Next(dp.GetSize())
		if err := dp.Unmarshal(dpData); err != nil {
			return err
		}
		d.DispatcherProgresses = append(d.DispatcherProgresses, dp)
	}
	return nil
}
