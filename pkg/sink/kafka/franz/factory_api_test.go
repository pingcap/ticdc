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

package franz

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildFranzCompressionOptionHasNoErrorReturn(t *testing.T) {
	t.Parallel()

	o := &Options{}
	_ = newCompressionOption(o)
}

func TestBuildFranzCompressionOptionNilOption(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		_ = newCompressionOption(nil)
	})
}

func TestBuildFranzProducerOptionsHasNoErrorReturn(t *testing.T) {
	t.Parallel()

	o := &Options{}
	_ = newProducerOptions(o)
}

func TestBuildFranzProducerOptionsNilOption(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		opts := newProducerOptions(nil)
		require.NotEmpty(t, opts)
	})
}

func TestBuildFranzClientOptionsNilOption(t *testing.T) {
	t.Parallel()

	require.NotPanics(t, func() {
		opts, err := newOptions(context.Background(), nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, opts)
	})
}

func TestNewSyncProducerNilOptionsDoesNotPanic(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz-sync-nil-options")
	require.NotPanics(t, func() {
		producer, err := NewSyncProducer(context.Background(), changefeedID, nil, nil)
		if producer != nil {
			producer.Close()
		}
		require.Error(t, err)
	})
}

func TestBuildFranzRequiredAcks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		requiredAcks int16
		expected     kgo.Acks
	}{
		{name: "all", requiredAcks: -1, expected: kgo.AllISRAcks()},
		{name: "leader", requiredAcks: 1, expected: kgo.LeaderAck()},
		{name: "none", requiredAcks: 0, expected: kgo.NoAck()},
		{name: "invalid fallback all", requiredAcks: 2, expected: kgo.AllISRAcks()},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, newRequiredAcks(&Options{RequiredAcks: tc.requiredAcks}))
		})
	}

	require.Equal(t, kgo.AllISRAcks(), newRequiredAcks(nil))
}
