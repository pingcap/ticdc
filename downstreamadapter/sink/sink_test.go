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

package sink

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestUnknownSchemeMasksSensitiveSinkURI(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID(common.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		SinkURI: "unknown://127.0.0.1:9092/topic?sasl-password=verysecure&access-key=rawkey",
	}

	_, err := New(context.Background(), cfg, changefeedID)
	require.Error(t, err)
	requireMaskedSinkURIError(t, err)

	err = Verify(context.Background(), cfg, changefeedID)
	require.Error(t, err)
	requireMaskedSinkURIError(t, err)
}

func TestParseErrorMasksSensitiveSinkURI(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID(common.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		SinkURI: "mysql://root:verysecure@127.0.0.1/%zz",
	}

	_, err := New(context.Background(), cfg, changefeedID)
	require.Error(t, err)
	requireInvalidSinkURIError(t, err)

	err = Verify(context.Background(), cfg, changefeedID)
	require.Error(t, err)
	requireInvalidSinkURIError(t, err)
}

func requireMaskedSinkURIError(t *testing.T, err error) {
	t.Helper()

	errMsg := err.Error()
	require.NotContains(t, errMsg, "verysecure")
	require.NotContains(t, errMsg, "rawkey")
	require.Contains(t, errMsg, "sasl-password=xxxxx")
	require.Contains(t, errMsg, "access-key=xxxxx")
}

func requireInvalidSinkURIError(t *testing.T, err error) {
	t.Helper()

	errMsg := err.Error()
	require.NotContains(t, errMsg, "verysecure")
	require.Contains(t, errMsg, "<invalid uri>")
	require.Contains(t, errMsg, `parse "<invalid uri>"`)
	require.Contains(t, errMsg, "invalid URL escape")
}
