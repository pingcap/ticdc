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

package syncpoint

import (
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

// SyncPointConfig not nil only when enable sync point
type SyncPointConfig struct {
	SyncPointInterval  time.Duration
	SyncPointRetention time.Duration
}

func CalculateStartSyncPointTs(startTs uint64, syncPointInterval time.Duration) uint64 {
	if syncPointInterval == time.Duration(0) {
		return 0
	}
	k := oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0)) / syncPointInterval
	if oracle.GetTimeFromTS(startTs).Sub(time.Unix(0, 0))%syncPointInterval != 0 || oracle.ExtractLogical(startTs) != 0 {
		k += 1
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * syncPointInterval))
}
