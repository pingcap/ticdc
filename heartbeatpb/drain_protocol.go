// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package heartbeatpb

const (
	// LegacyDrainProtocolVersion means the node only supports legacy hard-restart drain.
	LegacyDrainProtocolVersion uint32 = 0
	// DrainProtocolVersion1 supports coordinator-driven drain but does not report
	// the log service dispatcher count in node heartbeats.
	DrainProtocolVersion1 uint32 = 1
	// CurrentDrainProtocolVersion is the coordinator-driven drain protocol version.
	CurrentDrainProtocolVersion uint32 = 2
)

// SupportsCoordinatorDrivenDrain returns whether the node can run coordinator-driven drain.
func SupportsCoordinatorDrivenDrain(version uint32) bool {
	return version != LegacyDrainProtocolVersion
}

// SupportsLogServiceDispatcherCount reports whether STOPPING heartbeats include
// the log service dispatcher count used as a drain-completion gate.
func SupportsLogServiceDispatcherCount(version uint32) bool {
	return version >= CurrentDrainProtocolVersion
}
