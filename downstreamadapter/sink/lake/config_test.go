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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestLakeOptions(t *testing.T) {
	uri, err := url.Parse("lake://user:pass@warehouse/lake?exclusive=true&workers=2&batch-bytes=4096&spool-bytes=16384")
	require.NoError(t, err)
	o, err := parseOptions(uri, nil)
	require.NoError(t, err)
	require.Equal(t, "https://warehouse", o.endpoint)
	require.Equal(t, "lake", o.database)
	require.Equal(t, 2, o.workers)
	require.EqualValues(t, 16384, o.spoolBytes)
	lakeURI, err := url.Parse("lake://user:pass@warehouse:443/default?exclusive=true&warehouse=default")
	require.NoError(t, err)
	lake, err := parseOptions(lakeURI, nil)
	require.NoError(t, err)
	require.Equal(t, "https://warehouse:443", lake.endpoint)
	for _, raw := range []string{
		"lake://host", "lake://host?exclusive=false", "lake://host?exclusive=true&workers=0", "lake://host?exclusive=true&memory-ratio=NaN", "lake://host?exclusive=true&batch-bytes=4096&spool-bytes=4096", "lake://host?exclusive=true&raw-change=true", "lake://host?exclusive=true&workers=1&workers=2", "lake://host?exclusive=true&query-timeout=0s",
	} {
		u, err := url.Parse(raw)
		require.NoError(t, err)
		_, err = parseOptions(u, nil)
		require.Error(t, err, raw)
	}
	_, err = parseOptions(uri, &config.ChangefeedConfig{EnableTableAcrossNodes: true})
	require.Error(t, err)
	_, err = parseOptions(uri, &config.ChangefeedConfig{EnableSyncPoint: true})
	require.Error(t, err)
}

func TestConflictKeyOption(t *testing.T) {
	for _, key := range []string{"id", "BusinessID", "a\"b"} {
		u, err := url.Parse("lake://host/default?exclusive=true&conflict-key=" + url.QueryEscape(key))
		require.NoError(t, err)
		o, err := parseOptions(u, &config.ChangefeedConfig{ForceReplicate: true})
		require.NoError(t, err)
		require.Equal(t, key, o.conflictKey)
	}
	for _, raw := range []string{"", " ", "bad\x00name"} {
		u, err := url.Parse("lake://host/default?exclusive=true&conflict-key=" + url.QueryEscape(raw))
		require.NoError(t, err)
		_, err = parseOptions(u, nil)
		require.Error(t, err)
	}
}

func TestLakePlaintextRequiresLoopback(t *testing.T) {
	for _, host := range []string{"localhost", "LOCALHOST:8000", "127.0.0.1:8000", "127.1.2.3", "[::1]:8000", "[::ffff:127.0.0.1]:8000"} {
		u, err := url.Parse("lake+http://user:pass@" + host + "/default?exclusive=true")
		require.NoError(t, err)
		o, err := parseOptions(u, nil)
		require.NoError(t, err, host)
		require.Equal(t, "http://"+host, o.endpoint)
	}
	for _, host := range []string{"warehouse.example:8000", "localhost.example", "127.0.0.1.example", "10.0.0.1", "0.0.0.0", "[::]", "[2001:db8::1]"} {
		u, err := url.Parse("lake+http://user:pass@" + host + "?exclusive=true")
		require.NoError(t, err)
		_, err = parseOptions(u, nil)
		require.Error(t, err, host)
	}
}
