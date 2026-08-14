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

package franz

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

func buildEventLogContext(keyspace, changefeed string, info *common.MessageLogInfo) string {
	var text strings.Builder
	text.WriteString("keyspace=" + keyspace + ", changefeed=" + changefeed + ", eventType=" + eventType(info))
	if info == nil {
		return text.String()
	}
	if rows, err := json.Marshal(info.Rows); len(info.Rows) > 0 && err == nil {
		text.WriteString(", dmlInfo=" + string(rows))
	}
	if info.DDL != nil {
		if info.DDL.Query != "" {
			text.WriteString(", ddlQuery=" + strconv.Quote(info.DDL.Query))
		}
		if info.DDL.StartTs != 0 {
			text.WriteString(", ddlStartTs=" + strconv.FormatUint(info.DDL.StartTs, 10))
		}
		if info.DDL.CommitTs != 0 {
			text.WriteString(", ddlCommitTs=" + strconv.FormatUint(info.DDL.CommitTs, 10))
		}
	}
	if info.Checkpoint != nil && info.Checkpoint.CommitTs != 0 {
		text.WriteString(", checkpointTs=" + strconv.FormatUint(info.Checkpoint.CommitTs, 10))
	}
	return text.String()
}

func eventType(info *common.MessageLogInfo) string {
	if info == nil {
		return "unknown"
	}
	if info.DDL != nil {
		return "ddl"
	}
	if info.Checkpoint != nil {
		return "checkpoint"
	}
	if len(info.Rows) > 0 {
		return "dml"
	}
	return "unknown"
}
