// Copyright 2022 PingCAP, Inc.
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
package csv

import (
	"context"
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchDecoder(t *testing.T) {
	csvData := `"I","employee","hr",433305438660591626,101,"Smith","Bob","2014-06-04","New York"
"U","employee","hr",433305438660591627,101,"Smith","Bob","2015-10-08","Los Angeles"
"D","employee","hr",433305438660591629,101,"Smith","Bob","2017-03-13","Dallas"
"I","employee","hr",433305438660591630,102,"Alex","Alice","2017-03-14","Shanghai"
"U","employee","hr",433305438660591630,102,"Alex","Alice","2018-06-15","Beijing"
`
	ctx := context.Background()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Job("create database hr")
	createTableDDL := helper.DDL2Event("create table hr.employee(Id int, LastName varchar(255), FirstName varchar(255), HireDate date, OfficeLocation varchar(255))")

	codecConfig := &common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	}
	decoder, err := NewDecoder(ctx, codecConfig, createTableDDL.TableInfo, []byte(csvData))
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		tp, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, tp)
		event := decoder.NextDMLEvent()
		require.NotNil(t, event)
	}

	_, hasNext := decoder.HasNext()
	require.False(t, hasNext)
}
