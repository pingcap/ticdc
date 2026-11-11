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

package traffic

import (
	"encoding/json/v2"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceMeteringJSON(t *testing.T) {
	first := common.ChangeFeedID{
		Id:          common.NewGIDWithValue(1, 23),
		DisplayName: common.NewChangeFeedDisplayName("payments", "tenant-a"),
	}
	recreated := first
	recreated.Id = common.NewGIDWithValue(12, 3)
	data := KeyspaceMetering{
		KeyspaceID:   12345,
		KeyspaceName: "tenant-a",
		Changefeeds: []ChangefeedMetering{
			{KeyspaceID: 12345, ChangefeedID: first, EffectiveBytes: 1024},
			{KeyspaceID: 12345, ChangefeedID: recreated, EffectiveBytes: 0},
		},
	}
	encoded, err := json.Marshal(&data)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"keyspace_name": "tenant-a",
		"changefeeds": [
			{
				"changefeed_id": {
					"id": {"low": 1, "high": 23},
					"name": "payments"
				},
				"effective_bytes": 1024
			},
			{
				"changefeed_id": {
					"id": {"low": 12, "high": 3},
					"name": "payments"
				},
				"effective_bytes": 0
			}
		]
	}`, string(encoded))
}
