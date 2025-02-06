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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestDDLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	ddlEvent := &DDLEvent{
		Version:      DDLEventVersion,
		DispatcherID: common.NewDispatcherID(),
		Type:         byte(ddlJob.Type),
		SchemaID:     ddlJob.SchemaID,
		TableID:      ddlJob.TableID,
		SchemaName:   ddlJob.SchemaName,
		TableName:    ddlJob.TableName,
		Query:        ddlJob.Query,
		TableInfo:    common.WrapTableInfo(ddlJob.SchemaID, ddlJob.SchemaName, ddlJob.BinlogInfo.TableInfo),
		FinishedTs:   ddlJob.BinlogInfo.FinishedTS,
		Err:          apperror.ErrDDLEventError.GenWithStackByArgs("test"),
	}

	data, err := ddlEvent.Marshal()
	require.Nil(t, err)

	reverseEvent := &DDLEvent{}
	err = reverseEvent.Unmarshal(data)
	reverseEvent.eventSize = 0
	require.Nil(t, err)

	// Compare individual fields instead of using DeepEqual
	require.Equal(t, ddlEvent.Version, reverseEvent.Version)
	require.Equal(t, ddlEvent.DispatcherID, reverseEvent.DispatcherID)
	require.Equal(t, ddlEvent.Type, reverseEvent.Type)
	require.Equal(t, ddlEvent.SchemaID, reverseEvent.SchemaID)
	require.Equal(t, ddlEvent.TableID, reverseEvent.TableID)
	require.Equal(t, ddlEvent.SchemaName, reverseEvent.SchemaName)
	require.Equal(t, ddlEvent.TableName, reverseEvent.TableName)
	require.Equal(t, ddlEvent.Query, reverseEvent.Query)
	require.Equal(t, ddlEvent.TableInfo, reverseEvent.TableInfo)
	require.Equal(t, ddlEvent.FinishedTs, reverseEvent.FinishedTs)
	require.Equal(t, ddlEvent.Err.Error(), reverseEvent.Err.Error())
}
