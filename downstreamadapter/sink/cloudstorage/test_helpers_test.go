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

package cloudstorage

import (
	"context"
	"os"
	"testing"
	"time"

	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

const (
	testEventuallyTimeout = 5 * time.Second
	testEventuallyTick    = 10 * time.Millisecond
)

func TestMain(m *testing.M) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	os.Exit(m.Run())
}

func setPDClockForTest(t *testing.T, clock pdutil.Clock) func(pdutil.Clock) {
	t.Helper()

	previous, hasPrevious := appcontext.TryGetService[pdutil.Clock](appcontext.DefaultPDClock)

	setClock := func(clock pdutil.Clock) {
		appcontext.SetService(appcontext.DefaultPDClock, clock)
	}
	setClock(clock)

	t.Cleanup(func() {
		if hasPrevious {
			appcontext.SetService(appcontext.DefaultPDClock, previous)
		}
	})
	return setClock
}

func runSinkInBackground(t *testing.T, ctx context.Context, s *sink) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- s.Run(ctx)
	}()
	return done
}

func cancelAndWaitSink(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testEventuallyTimeout):
		t.Fatal("sink.Run did not exit after context cancel")
	}
}

func readFileEventually(t *testing.T, filePath string) []byte {
	t.Helper()

	var content []byte
	require.Eventually(t, func() bool {
		var err error
		content, err = os.ReadFile(filePath)
		return err == nil
	}, testEventuallyTimeout, testEventuallyTick)
	return content
}
