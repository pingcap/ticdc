// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package config

import (
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

type CaptureID = string

// TableOperation records the current information of a table migration
type TableOperation struct {
	Delete bool   `json:"delete"`
	Flag   uint64 `json:"flag,omitempty"`
	// if the operation is a delete operation, BoundaryTs is checkpoint ts
	// if the operation is an add operation, BoundaryTs is start ts
	BoundaryTs uint64 `json:"boundary_ts"`
	Status     uint64 `json:"status,omitempty"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// Table list, containing tables that processor should process
	Tables    []int64                            `json:"table_ids,omitempty"`
	Operation map[common.TableID]*TableOperation `json:"table_operations,omitempty"`
}

// CaptureInfo store in etcd.
type CaptureInfo struct {
	ID            CaptureID `json:"id"`
	AdvertiseAddr string    `json:"address"`

	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`
	IsNewArch      bool   `json:"is-new-arch"`
}

// Marshal using json.Marshal.
func (c *CaptureInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *CaptureInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

// TaskPosition records the process information of a capture
type TaskPosition struct {
	// The maximum event CommitTs that has been synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	CheckPointTs uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	ResolvedTs uint64 `json:"resolved-ts"`
	// The count of events were synchronized. This is updated by corresponding processor.
	//
	// Deprecated: only used in API. TODO: remove API usage.
	Count uint64 `json:"count"`

	// Error when changefeed error happens
	Error *RunningError `json:"error"`
	// Warning when module error happens
	Warning *RunningError `json:"warning"`
}

// Marshal returns the json marshal format of a TaskStatus
func (tp *TaskPosition) Marshal() (string, error) {
	data, err := json.Marshal(tp)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *TaskStatus from json marshal byte slice
func (tp *TaskPosition) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, tp)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// String implements fmt.Stringer interface.
func (tp *TaskPosition) String() string {
	data, _ := tp.Marshal()
	return data
}

// Clone returns a deep clone of TaskPosition
func (tp *TaskPosition) Clone() *TaskPosition {
	ret := &TaskPosition{
		CheckPointTs: tp.CheckPointTs,
		ResolvedTs:   tp.ResolvedTs,
		Count:        tp.Count,
	}
	if tp.Error != nil {
		ret.Error = &RunningError{
			Time:    tp.Error.Time,
			Addr:    tp.Error.Addr,
			Code:    tp.Error.Code,
			Message: tp.Error.Message,
		}
	}
	if tp.Warning != nil {
		ret.Warning = &RunningError{
			Time:    tp.Warning.Time,
			Addr:    tp.Warning.Addr,
			Code:    tp.Warning.Code,
			Message: tp.Warning.Message,
		}
	}
	return ret
}
