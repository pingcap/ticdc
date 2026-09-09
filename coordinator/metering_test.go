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

package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	meteringconfig "github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

func TestMeteringInitializationFailure(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	base := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(base, []byte("existing file"), 0o600))
	for _, tc := range []struct {
		name string
		cfg  *meteringconfig.MeteringConfig
		code string
	}{
		{"invalid config", &meteringconfig.MeteringConfig{Type: "s3"}, string(errors.ErrInvalidServerOption.RFCCode())},
		{"provider initialization", meteringconfig.NewMeteringConfig().WithLocalFS(filepath.Join(base, "child")), string(errors.ErrExternalStorageAPI.RFCCode())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := original.Clone()
			cfg.Metering = tc.cfg
			config.StoreGlobalServerConfig(cfg)
			// Initialization must fail before starting any coordinator loops.
			c := &coordinator{}
			err := c.Run(context.Background())
			require.ErrorContains(t, err, tc.code)
			require.Nil(t, c.trafficReporter)
		})
	}
}

func TestMeteringCoordinatorTerm(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	dir := filepath.Join(t.TempDir(), "metering")
	for _, tc := range []struct {
		name string
		cfg  *meteringconfig.MeteringConfig
	}{
		{"absent", nil},
		{"empty type", &meteringconfig.MeteringConfig{Bucket: "unused"}},
		{"enabled", meteringconfig.NewMeteringConfig().WithLocalFS(dir)},
		{"next term", meteringconfig.NewMeteringConfig().WithLocalFS(dir)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := original.Clone()
			cfg.Metering = tc.cfg
			config.StoreGlobalServerConfig(cfg)
			c := &coordinator{
				gcTickInterval: time.Hour,
				controller:     &Controller{},
				gcCleaner:      gccleaner.New(nil, "test"),
				eventCh:        chann.NewAutoDrainChann[*Event](),
			}
			defer c.eventCh.CloseAndDrain()
			// A configured follower has not initialized metering. Entering Run
			// creates the writer for this term, even when cancellation is pending.
			require.Nil(t, c.trafficReporter)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, c.Run(ctx), context.Canceled)
			if tc.cfg == nil || tc.cfg.Type == "" {
				require.Nil(t, c.trafficReporter)
			} else {
				require.NotNil(t, c.trafficReporter)
				_, err := os.Stat(dir)
				require.NoError(t, err)
				require.ErrorContains(t, c.trafficReporter.Report(context.Background(), time.Now(), []metering.TrafficRecord{{ChangefeedGID: "gid1", TrafficBytes: 1}}), "closed")
			}
		})
	}
}
