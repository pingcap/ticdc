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

package set_checksum

import (
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
)

// Checksum is an order-independent, incrementally updatable checksum for a dispatcher set.
//
// It uses (count, xor, sum) of DispatcherID's 128-bit components. This is not a cryptographic
// hash; collisions are possible but expected to be extremely rare.
type Checksum struct {
	Count   uint64
	XorHigh uint64
	XorLow  uint64
	SumHigh uint64
	SumLow  uint64
}

// Add updates the checksum for inserting id.
//
// The checksum is order-independent and supports O(1) incremental updates.
func (c *Checksum) Add(id common.DispatcherID) {
	c.Count++
	c.XorHigh ^= id.High
	c.XorLow ^= id.Low
	c.SumHigh += id.High
	c.SumLow += id.Low
}

// Remove updates the checksum for removing id.
//
// Callers are expected to ensure id is present in the set before calling Remove.
func (c *Checksum) Remove(id common.DispatcherID) {
	c.Count--
	c.XorHigh ^= id.High
	c.XorLow ^= id.Low
	c.SumHigh -= id.High
	c.SumLow -= id.Low
}

// Equal reports whether two checksums are equal.
func (c Checksum) Equal(other Checksum) bool {
	return c.Count == other.Count &&
		c.XorHigh == other.XorHigh &&
		c.XorLow == other.XorLow &&
		c.SumHigh == other.SumHigh &&
		c.SumLow == other.SumLow
}

// FromPB converts a protobuf checksum message into a Checksum.
func FromPB(pb *heartbeatpb.DispatcherSetChecksum) Checksum {
	if pb == nil {
		return Checksum{}
	}
	return Checksum{
		Count:   pb.Count,
		XorHigh: pb.XorHigh,
		XorLow:  pb.XorLow,
		SumHigh: pb.SumHigh,
		SumLow:  pb.SumLow,
	}
}

// ToPB converts a Checksum into its protobuf message.
func (c Checksum) ToPB() *heartbeatpb.DispatcherSetChecksum {
	return &heartbeatpb.DispatcherSetChecksum{
		Count:   c.Count,
		XorHigh: c.XorHigh,
		XorLow:  c.XorLow,
		SumHigh: c.SumHigh,
		SumLow:  c.SumLow,
	}
}
