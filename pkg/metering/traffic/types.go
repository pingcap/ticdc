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

	"github.com/pingcap/ticdc/pkg/common"
)

type ChangefeedMetering struct {
	KeyspaceID     uint32              `json:"-"`
	ChangefeedID   common.ChangeFeedID `json:"changefeed_id"`
	EffectiveBytes uint64              `json:"effective_bytes"`
}

func (m ChangefeedMetering) MarshalJSON() ([]byte, error) {
	var data struct {
		ChangefeedID struct {
			ID   common.GID `json:"id"`
			Name string     `json:"name"`
		} `json:"changefeed_id"`
		EffectiveBytes uint64 `json:"effective_bytes"`
	}
	data.ChangefeedID.ID = m.ChangefeedID.ID()
	data.ChangefeedID.Name = m.ChangefeedID.Name()
	data.EffectiveBytes = m.EffectiveBytes
	return json.Marshal(&data)
}

type KeyspaceMetering struct {
	KeyspaceID   uint32               `json:"-"`
	KeyspaceName string               `json:"keyspace_name"`
	Changefeeds  []ChangefeedMetering `json:"changefeeds"`
}
