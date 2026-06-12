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

package common

// MaintainerEpochMatches keeps rolling-upgrade compatibility while enforcing
// exact owner epochs after upgraded maintainers report them. Epoch 0 means
// either side predates the maintainer epoch field, so it stays accepted during
// mixed-version rollout. This compatibility gate is not intended to fence every
// mixed-version race, only stale non-zero epochs after rollout completes.
func MaintainerEpochMatches(reportedEpoch, currentEpoch uint64) bool {
	return reportedEpoch == 0 || currentEpoch == 0 || reportedEpoch == currentEpoch
}
