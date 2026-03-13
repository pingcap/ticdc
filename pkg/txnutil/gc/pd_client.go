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

package gc

import (
	"context"
	"time"

	pdgc "github.com/tikv/pd/client/clients/gc"
)

// GCServiceClient is the subset of PD GC APIs TiCDC needs to maintain
// service safepoints / keyspace barriers.
type GCServiceClient interface {
	UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error)
	GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient
}

// GCStatesClient is the subset of keyspace GC barrier APIs TiCDC uses.
type GCStatesClient interface {
	SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error)
	DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error)
	GetGCState(ctx context.Context) (pdgc.GCState, error)
}
