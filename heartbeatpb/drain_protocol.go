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
	// CurrentDrainProtocolVersion adds broker drain reports through LogCoordinator.
	// Version 1 supports drain control messages but cannot report broker counts.
	CurrentDrainProtocolVersion uint32 = 2
)

// SupportsCoordinatorDrivenDrain checks support for the original drain control
// messages. Version 1 nodes must still participate in stale drain-target cleanup.
func SupportsCoordinatorDrivenDrain(version uint32) bool {
	return version != LegacyDrainProtocolVersion
}

// SupportsEventBrokerDrain checks support for the broker reports required before
// starting the current drain workflow. Older nodes use the legacy restart path.
func SupportsEventBrokerDrain(version uint32) bool {
	return version >= CurrentDrainProtocolVersion
}
