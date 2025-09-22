// Copyright 2024 PingCAP, Inc.
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
	"bytes"
	"fmt"

	"github.com/pingcap/ticdc/heartbeatpb"
)

type DataRange struct {
	ClusterID     uint64
	Span          *heartbeatpb.TableSpan
	CommitTsStart uint64
	CommitTsEnd   uint64

	// LastScannedTxnStartTs is the start-ts of the last scanned DML event.
	// it should less than the CommitTsStart
	LastScannedTxnStartTs uint64
}

func NewDataRange(clusterID uint64, span *heartbeatpb.TableSpan, startTs, endTs uint64) *DataRange {
	return &DataRange{
		ClusterID:     clusterID,
		Span:          span,
		CommitTsStart: startTs,
		CommitTsEnd:   endTs,
	}
}

func (d *DataRange) GetStartTs() uint64 {
	return d.CommitTsStart
}

func (d *DataRange) GetEndTs() uint64 {
	return d.CommitTsEnd
}

func (d *DataRange) String() string {
	return fmt.Sprintf("span: %s, startTs: %d, endTs: %d", d.Span.String(), d.CommitTsStart, d.CommitTsEnd)
}

func (d *DataRange) Equal(other *DataRange) bool {
	return d.Span.Equal(other.Span) && d.CommitTsStart == other.CommitTsStart && d.CommitTsEnd == other.CommitTsEnd
}

// Merge merges two DataRange, if the two DataRange have different Span, return nil.
// Otherwise, return the merged DataRange.
// The merged DataRange has the same Span, and the startTs is the minimum of the two DataRange,
// and the endTs is the maximum of the two DataRange.
func (d *DataRange) Merge(other *DataRange) *DataRange {
	if other == nil {
		return d
	}
	if !d.Span.Equal(other.Span) {
		return nil
	}
	if d.CommitTsStart > other.CommitTsStart {
		d.CommitTsStart = other.CommitTsStart
	}
	if d.CommitTsEnd < other.CommitTsEnd {
		d.CommitTsEnd = other.CommitTsEnd
	}
	return d
}

// IsTableSpanConsecutive checks if the two table spans are consecutive.
// The two table spans are consecutive if they are for the same table and the end key of the previous span is the same as the start key of the current span.
func IsTableSpanConsecutive(prev *heartbeatpb.TableSpan, current *heartbeatpb.TableSpan) bool {
	if prev.TableID != current.TableID {
		return false
	}
	if bytes.Equal(prev.EndKey, current.StartKey) {
		return true
	}
	return false
}

const (
	// DDLSpanSchemaID is the special schema id for DDL
	DDLSpanSchemaID int64 = 0

	// DDLSpanTableID is the special table id for DDL
	DDLSpanTableID int64 = 0
)

// KeyspaceDDLSpan is the a special keyspace span for Table Trigger Event Dispatcher
func KeyspaceDDLSpan(keyspaceID uint32) *heartbeatpb.TableSpan {
	span := TableIDToComparableSpan(keyspaceID, DDLSpanTableID)
	return &span
}

func LessTableSpan(t1, t2 *heartbeatpb.TableSpan) bool {
	return t1.Less(t2)
}
