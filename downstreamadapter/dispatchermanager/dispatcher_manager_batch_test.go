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

package dispatchermanager

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDispatcherManager_GetEventCollectorBatchCountAndBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := newMockKafkaSink(ctx, cancel)

	cases := []struct {
		name      string
		cfg       *config.ChangefeedConfig
		wantCount int
		wantBytes int
	}{
		{
			name:      "defaults-from-manager-and-sink",
			cfg:       &config.ChangefeedConfig{},
			wantCount: defaultEventCollectorBatchCount,
			wantBytes: 0,
		},
		{
			name: "override-count-only",
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchCount: 123,
			},
			wantCount: 123,
			wantBytes: 0,
		},
		{
			name: "override-bytes-only",
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchBytes: 456,
			},
			wantCount: defaultEventCollectorBatchCount,
			wantBytes: 456,
		},
		{
			name: "override-both",
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchCount: 123,
				EventCollectorBatchBytes: 456,
			},
			wantCount: 123,
			wantBytes: 456,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &DispatcherManager{
				sink:   s,
				config: tc.cfg,
			}
			gotCount, gotBytes := m.getEventCollectorBatchCountAndBytes()
			require.Equal(t, tc.wantCount, gotCount)
			require.Equal(t, tc.wantBytes, gotBytes)
		})
	}
}
