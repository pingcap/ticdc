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

package utils

type PkType string

type ColumnValue struct {
	ColumnID int64
	Value    any
}

type CdcVersion struct {
	CommitTs uint64
	OriginTs uint64
}

func (v *CdcVersion) GetCompareTs() uint64 {
	if v.OriginTs > 0 {
		return v.OriginTs
	}
	return v.CommitTs
}

type Record struct {
	CdcVersion
	Pk           PkType
	ColumnValues []ColumnValue
}

func (r *Record) EqualDownstreamRecord(downstreamRecord *Record) bool {
	if downstreamRecord == nil {
		return false
	}
	if r.CommitTs != downstreamRecord.OriginTs {
		return false
	}
	if r.Pk != downstreamRecord.Pk {
		return false
	}
	if len(r.ColumnValues) != len(downstreamRecord.ColumnValues) {
		return false
	}
	for i, columnValue := range r.ColumnValues {
		if columnValue.ColumnID != downstreamRecord.ColumnValues[i].ColumnID {
			return false
		}
		if columnValue.Value != downstreamRecord.ColumnValues[i].Value {
			return false
		}
	}
	return true
}
