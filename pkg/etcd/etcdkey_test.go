// Copyright 2021 PingCAP, Inc.
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

package etcd

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestEtcdKey(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		key      string
		expected *CDCKey
	}{{
		key: fmt.Sprintf("%s/owner/223176cb44d20a13", DefaultClusterAndMetaPrefix),
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "223176cb44d20a13",
			ClusterID:    DefaultCDCClusterID,
		},
	}, {
		key: fmt.Sprintf("%s/owner", DefaultClusterAndMetaPrefix),
		expected: &CDCKey{
			Tp:           CDCKeyTypeOwner,
			OwnerLeaseID: "",
			ClusterID:    DefaultCDCClusterID,
		},
	}, {
		key: fmt.Sprintf("%s/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
			DefaultClusterAndMetaPrefix),
		expected: &CDCKey{
			Tp:        CDCKeyTypeCapture,
			CaptureID: "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID: DefaultCDCClusterID,
		},
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/changefeed/info/test-_@#$%changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: common.ChangeFeedID{DisplayName: common.NewChangeFeedDisplayName("test-_@#$%changefeed", "default")},
			ClusterID:    DefaultCDCClusterID,
			Keyspace:     "default",
		},
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/changefeed/info/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangefeedInfo,
			ChangefeedID: common.ChangeFeedID{DisplayName: common.NewChangeFeedDisplayName("test/changefeed", "default")},
			ClusterID:    DefaultCDCClusterID,
			Keyspace:     "default",
		},
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/changefeed/status/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeChangeFeedStatus,
			ChangefeedID: common.ChangeFeedID{DisplayName: common.NewChangeFeedDisplayName("test-changefeed", "default")},
			ClusterID:    DefaultCDCClusterID,
			Keyspace:     "default",
		},
	}, {
		key: "/tidb/cdc/default/name/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test-changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskPosition,
			ChangefeedID: common.ChangeFeedID{DisplayName: common.NewChangeFeedDisplayName("test-changefeed", "name")},
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID:    DefaultCDCClusterID,
			Keyspace:     "name",
		},
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		expected: &CDCKey{
			Tp:           CDCKeyTypeTaskPosition,
			ChangefeedID: common.ChangeFeedID{DisplayName: common.NewChangeFeedDisplayName("test/changefeed", "default")},
			CaptureID:    "6bbc01c8-0605-4f86-a0f9-b3119109b225",
			ClusterID:    DefaultCDCClusterID,
			Keyspace:     "default",
		},
	}, {
		key: DefaultClusterAndKeyspacePrefix + "/upstream/12345",
		expected: &CDCKey{
			Tp:         CDCKeyTypeUpStream,
			ClusterID:  DefaultCDCClusterID,
			Keyspace:   "default",
			UpstreamID: 12345,
		},
	}, {
		key: fmt.Sprintf("%s%s", DefaultClusterAndMetaPrefix, metaVersionKey),
		expected: &CDCKey{
			Tp:        CDCKeyTypeMetaVersion,
			ClusterID: DefaultCDCClusterID,
		},
	}}
	for _, tc := range testcases {
		k := new(CDCKey)
		err := k.Parse(DefaultCDCClusterID, tc.key)
		require.NoError(t, err)
		require.Equal(t, k, tc.expected)
		require.Equal(t, k.String(), tc.key)
	}
}

func TestEtcdKeyParseError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		key   string
		error bool
	}{{
		key: DefaultClusterAndKeyspacePrefix +
			"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/test/changefeed",
		error: false,
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225/",
		error: false,
	}, {
		key: DefaultClusterAndKeyspacePrefix +
			"/task/position/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		error: true,
	}, {
		key:   "/tidb/cd",
		error: true,
	}, {
		key:   "/tidb/cdc/",
		error: true,
	}, {
		key:   "/tidb/cdc/default/__meta_data__/abcd",
		error: true,
	}, {
		key:   "/tidb/cdc/default/default/abcd",
		error: true,
	}}
	for _, tc := range testCases {
		k := new(CDCKey)
		err := k.Parse(DefaultCDCClusterID, tc.key)
		if tc.error {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
		}
	}
	k := new(CDCKey)
	k.Tp = CDCKeyTypeUpStream + 1
	require.Panics(t, func() {
		_ = k.String()
	})
}
