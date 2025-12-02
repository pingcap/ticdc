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

package common

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"go.uber.org/zap"
)

func FormatBlockStatusRequest(r *heartbeatpb.BlockStatusRequest) string {
	if r == nil {
		return ""
	}
	if r.ChangefeedID == nil {
		log.Warn("changefeedID is nil, it should not happen", zap.String("request", r.String()))
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("changefeed: %s, changefeedID: %s",
		r.ChangefeedID.GetName(),
		NewChangefeedGIDFromPB(r.ChangefeedID).String()))
	for _, status := range r.BlockStatuses {
		sb.WriteString(FormatTableSpanBlockStatus(status))
	}
	sb.WriteString("\n")
	return sb.String()
}

func FormatTableSpanBlockStatus(s *heartbeatpb.TableSpanBlockStatus) string {
	if s == nil {
		return ""
	}
	if s.ID == nil {
		log.Warn("dispatcherID is nil, it should not happen", zap.String("status", s.String()))
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("[ dispatcherID: %s, state: %s ]",
		NewDispatcherIDFromPB(s.ID).String(),
		s.State.String()))
	sb.WriteString("\n")
	return sb.String()
}

func FormatDispatcherStatus(d *heartbeatpb.DispatcherStatus) string {
	if d == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("action: %s, ack: %s, influencedDispatchers: %s",
		d.Action.String(),
		d.Ack.String(),
		FormatInfluencedDispatchers(d.InfluencedDispatchers)))
	sb.WriteString("\n")
	return sb.String()
}

func FormatInfluencedDispatchers(d *heartbeatpb.InfluencedDispatchers) string {
	if d == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("schemaID: %d, influenceType: %s",
		d.SchemaID,
		d.InfluenceType.String()))
	sb.WriteString("dispatcherIDs: [")
	for _, dispatcherID := range d.DispatcherIDs {
		sb.WriteString(fmt.Sprintf("%s, ",
			NewDispatcherIDFromPB(dispatcherID).String()))
	}
	sb.WriteString("]")
	if d.ExcludeDispatcherId != nil {
		sb.WriteString(fmt.Sprintf(", excludeDispatcherID: %s",
			NewDispatcherIDFromPB(d.ExcludeDispatcherId).String()))
	}
	sb.WriteString("\n")
	return sb.String()
}

func FormatTableSpan(s *heartbeatpb.TableSpan) string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("tableID: %d, startKey: %s, endKey: %s, keyspaceID: %d",
		s.TableID,
		hex.EncodeToString(s.StartKey),
		hex.EncodeToString(s.EndKey),
		s.KeyspaceID))
	return sb.String()
}

func FormatMaintainerStatus(s *heartbeatpb.MaintainerStatus) string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(
		"changefeed: %s, feedState: %s, state: %s, checkpointTs: %d, bootstrapDone: %t, errs: [",
		s.ChangefeedID.GetName(),
		s.FeedState,
		s.State.String(),
		s.CheckpointTs,
		s.BootstrapDone,
	))
	for _, err := range s.Err {
		sb.WriteString(err.String())
	}
	sb.WriteString("]")
	sb.WriteString("\n")
	return sb.String()
}
