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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package resource defines process-wide scheduling resource measurements.
package resource

import (
	"math"
	"slices"

	"github.com/pingcap/ticdc/heartbeatpb"
)

type Type = heartbeatpb.ResourceType

const (
	Sink            = heartbeatpb.ResourceType_RESOURCE_SINK
	EventStoreWrite = heartbeatpb.ResourceType_RESOURCE_EVENT_STORE_WRITE
	EventStoreRead  = heartbeatpb.ResourceType_RESOURCE_EVENT_STORE_READ
)

// Load keeps resource rates separate: byte counts from different stages are
// not interchangeable and must never be added into one raw traffic score.
type Load map[Type]float64

// Counters accepts legacy sink-only reports. A missing resource is unsupported,
// not an observed zero. Invalid vectors cannot be used for scheduling.
func Counters(load *heartbeatpb.NodeLoad) (map[Type]uint64, bool) {
	if load == nil {
		return nil, false
	}
	if len(load.Resources) == 0 {
		return map[Type]uint64{Sink: load.FlushedEventBytes}, true
	}
	values := make(map[Type]uint64, len(load.Resources))
	for _, usage := range load.Resources {
		if usage == nil || usage.Resource <= heartbeatpb.ResourceType_RESOURCE_UNKNOWN {
			return nil, false
		}
		if _, exists := values[usage.Resource]; exists {
			return nil, false
		}
		values[usage.Resource] = usage.TotalBytes
	}
	_, hasSink := values[Sink]
	return values, hasSink
}

func Rates(load *heartbeatpb.NodeLoad) (Load, bool) {
	if load == nil {
		return nil, false
	}
	if len(load.Resources) == 0 {
		if !validRate(load.BytesPerSecond) {
			return nil, false
		}
		return Load{Sink: load.BytesPerSecond}, true
	}
	rates := make(Load, len(load.Resources))
	for _, usage := range load.Resources {
		if usage == nil || usage.Resource <= heartbeatpb.ResourceType_RESOURCE_UNKNOWN || !validRate(usage.BytesPerSecond) {
			return nil, false
		}
		if _, exists := rates[usage.Resource]; exists {
			return nil, false
		}
		rates[usage.Resource] = usage.BytesPerSecond
	}
	_, hasSink := rates[Sink]
	return rates, hasSink
}

func validRate(rate float64) bool {
	return rate >= 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0)
}

func (l Load) ToNodeLoad(id string) *heartbeatpb.NodeLoad {
	kinds := make([]Type, 0, len(l))
	for kind := range l {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	load := &heartbeatpb.NodeLoad{NodeId: id, BytesPerSecond: l[Sink]}
	for _, kind := range kinds {
		load.Resources = append(load.Resources, &heartbeatpb.ResourceUsage{Resource: kind, BytesPerSecond: l[kind]})
	}
	return load
}
