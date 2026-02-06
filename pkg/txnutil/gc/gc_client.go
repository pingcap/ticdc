import (
	"context"
	"time"
)

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

import (
	pd "github.com/tikv/pd/client"
)

type gcClient interface {
	UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pd.GCBarrierInfo, error)
	DeleteGCBarrier(ctx context.Context, barrierID string) (*pd.GCBarrierInfo, error)
	GetGCState(ctx context.Context) (pd.GCState, error)
}