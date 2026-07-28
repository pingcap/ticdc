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

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestOptionsDerivesRequestTimeout(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                   string
		readTimeout            time.Duration
		writeTimeout           time.Duration
		expectedRequestTimeout time.Duration
	}{
		{
			name:                   "request timeout uses larger write timeout",
			readTimeout:            time.Second,
			writeTimeout:           2 * time.Minute,
			expectedRequestTimeout: 2 * time.Minute,
		},
		{
			name:                   "request timeout uses larger read timeout",
			readTimeout:            5 * time.Second,
			writeTimeout:           time.Second,
			expectedRequestTimeout: 5 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			o := NewOptions()
			o.ReadTimeout = tc.readTimeout
			o.WriteTimeout = tc.writeTimeout

			require.Equal(t, tc.expectedRequestTimeout, o.requestTimeout())
		})
	}
}

func TestNewFactoryAdminCreationReturnsKafkaSinkError(t *testing.T) {
	t.Parallel()

	options := NewOptions()
	options.Version = "invalid"
	options.IsAssignedVersion = true

	factory, err := NewFactory(
		context.Background(),
		options,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
	)
	require.Nil(t, factory)
	requireNewKafkaSinkError(t, err)
}

func TestFactoryComponentCreationReturnsKafkaSinkError(t *testing.T) {
	t.Parallel()

	factory := &factory{
		changefeedID: common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
		options: options{
			Version:           "invalid",
			IsAssignedVersion: true,
			sasl:              &saslConfig{},
		},
	}

	_, err := factory.Admin(context.Background())
	requireNewKafkaSinkError(t, err)
	_, err = factory.SyncProducer(context.Background())
	requireNewKafkaSinkError(t, err)
	_, err = factory.AsyncProducer(context.Background())
	requireNewKafkaSinkError(t, err)
}

func requireNewKafkaSinkError(t *testing.T, err error) {
	t.Helper()

	errCode, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrNewKafkaSink.RFCCode(), errCode)
}
