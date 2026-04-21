// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka/internal/logutil"
)

// DetermineEventType infers the event type based on MessageLogInfo content.
func DetermineEventType(info *common.MessageLogInfo) string {
	return logutil.DetermineEventType(info)
}

// BuildEventLogContext builds a textual representation of event info.
func BuildEventLogContext(keyspace, changefeed string, info *common.MessageLogInfo) string {
	return logutil.BuildEventLogContext(keyspace, changefeed, info)
}

// AnnotateEventError logs the event context and annotates the error with that context.
func AnnotateEventError(
	keyspace, changefeed string,
	info *common.MessageLogInfo,
	err error,
) error {
	return logutil.AnnotateEventError(keyspace, changefeed, info, err)
}
