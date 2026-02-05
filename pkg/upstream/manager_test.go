// Copyright 2022 PingCAP, Inc.
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

package upstream

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	uatomic "go.uber.org/atomic"
)

func TestManagerAddUpstreamSameIDResetsIdleTime(t *testing.T) {
	t.Parallel()

	m := NewManager(context.Background(), NodeTopologyCfg{GCServiceID: "id"})
	m.initUpstreamFunc = func(context.Context, *Upstream, *NodeTopologyCfg) error {
		return nil
	}

	up := m.AddUpstream(&UpstreamInfo{ID: 3})
	require.NotNil(t, up)

	up.mu.Lock()
	up.idleTime = time.Now()
	up.mu.Unlock()
	require.False(t, up.idleTime.IsZero())

	up2 := m.AddUpstream(&UpstreamInfo{ID: 3})
	require.Same(t, up, up2)
	require.True(t, up2.idleTime.IsZero())
}

func TestManagerAddDefaultUpstream(t *testing.T) {
	t.Parallel()

	m := NewManager(context.Background(), NodeTopologyCfg{GCServiceID: "id"})

	m.initUpstreamFunc = func(context.Context, *Upstream, *NodeTopologyCfg) error {
		return errors.New("test")
	}
	_, err := m.AddDefaultUpstream([]string{}, &security.Credential{}, nil, nil)
	require.Error(t, err)
	_, err = m.GetDefaultUpstream()
	require.Error(t, err)

	m.initUpstreamFunc = func(_ context.Context, up *Upstream, _ *NodeTopologyCfg) error {
		up.ID = uint64(2)
		up.cancel = func() {}
		atomic.StoreInt32(&up.status, normal)
		return nil
	}

	_, err = m.AddDefaultUpstream([]string{}, &security.Credential{}, nil, nil)
	require.NoError(t, err)

	up, err := m.GetDefaultUpstream()
	require.NoError(t, err)
	require.NotNil(t, up)

	up2, ok := m.Get(uint64(2))
	require.True(t, ok)
	require.Same(t, up, up2)
}

func TestManagerCloseRemovesUpstreams(t *testing.T) {
	t.Parallel()

	m := NewManager(context.Background(), NodeTopologyCfg{GCServiceID: "id"})

	canceled := uatomic.NewBool(false)
	up := &Upstream{
		cancel: func() { canceled.Store(true) },
		wg:     new(sync.WaitGroup),
	}
	atomic.StoreInt32(&up.status, normal)
	m.ups.Store(uint64(1), up)

	m.Close()
	require.True(t, canceled.Load())
	_, ok := m.ups.Load(uint64(1))
	require.False(t, ok)
}

func TestManagerVisit(t *testing.T) {
	t.Parallel()

	m := NewManager(context.Background(), NodeTopologyCfg{GCServiceID: "id"})

	up1 := &Upstream{cancel: func() {}, wg: new(sync.WaitGroup)}
	up2 := &Upstream{cancel: func() {}, wg: new(sync.WaitGroup)}
	m.ups.Store(uint64(1), up1)
	m.ups.Store(uint64(2), up2)

	visited := uatomic.NewInt64(0)
	require.NoError(t, m.Visit(func(up *Upstream) error {
		require.NotNil(t, up)
		visited.Inc()
		return nil
	}))
	require.Equal(t, int64(2), visited.Load())
}
