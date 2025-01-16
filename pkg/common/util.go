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
	"fmt"
	"strings"

	"github.com/pingcap/ticdc/heartbeatpb"
)

func FormatBlockStatusRequest(r *heartbeatpb.BlockStatusRequest) string {
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
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("[ dispatcherID: %s, state: %s ]",
		NewDispatcherIDFromPB(s.ID).String(),
		s.State.String()))
	sb.WriteString("\n")
	return sb.String()
}

func FormatDispatcherStatus(d *heartbeatpb.DispatcherStatus) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("action: %s, ack: %s, influencedDispatchers: %s",
		d.Action.String(),
		d.Ack.String(),
		FormatInfluencedDispatchers(d.InfluencedDispatchers)))
	sb.WriteString("\n")
	return sb.String()
}

func FormatInfluencedDispatchers(d *heartbeatpb.InfluencedDispatchers) string {
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
