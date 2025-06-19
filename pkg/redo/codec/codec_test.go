// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestDDLRedoConvert(t *testing.T) {
	t.Parallel()

	ddl := &pevent.DDLEvent{
		FinishedTs: 1030,
		Type:       byte(timodel.ActionAddColumn),
		Query:      "ALTER TABLE test.t1 ADD COLUMN a int",
		SchemaName: "Hello",
		TableName:  "World",
		TableID:    1,
		TableInfo:  &common.TableInfo{},
	}

	redoLog := ddl.ToRedoLog()
	data, err := MarshalRedoLog(redoLog, nil)
	require.Nil(t, err)

	data1 := data
	redoLog2, data, err := UnmarshalRedoLog(data)
	require.Nil(t, err)
	require.Zero(t, len(data))

	data2, err := MarshalRedoLog(redoLog2, nil)
	require.Equal(t, data1, data2)
}
