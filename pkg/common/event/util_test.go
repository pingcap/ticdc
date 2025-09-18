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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestIsSplitable(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	testCases := []struct {
		name        string
		createSQL   string
		expected    bool
		description string
	}{
		{
			name:        "table_with_pk_no_uk",
			createSQL:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(32));",
			expected:    true,
			description: "Table with primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_pk_and_uk",
			createSQL:   "CREATE TABLE t2 (student_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, email VARCHAR(100) UNIQUE);",
			expected:    false,
			description: "Table with primary key and unique key should not be splitable",
		},
		{
			name:        "table_with_no_pk",
			createSQL:   "CREATE TABLE t3 (id INT, name VARCHAR(32));",
			expected:    false,
			description: "Table with no primary key should not be splitable",
		},
		{
			name:        "table_with_varchar_pk",
			createSQL:   "CREATE TABLE t4 (id VARCHAR(32) PRIMARY KEY, name VARCHAR(32));",
			expected:    true,
			description: "Table with varchar primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_nonclustered_pk",
			createSQL:   "CREATE TABLE t5 (a VARCHAR(200), b INT, PRIMARY KEY(a) NONCLUSTERED);",
			expected:    true,
			description: "Table with nonclustered primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_composite_pk",
			createSQL:   "CREATE TABLE t6 (a INT, b INT, PRIMARY KEY(a, b));",
			expected:    true,
			description: "Table with composite primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_uk_no_pk",
			createSQL:   "CREATE TABLE t7 (a INT, b INT, UNIQUE KEY(a, b));",
			expected:    false,
			description: "Table with unique key but no primary key should not be splitable",
		},
		{
			name:        "table_with_multiple_uk",
			createSQL:   "CREATE TABLE t8 (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE, phone VARCHAR(20) UNIQUE);",
			expected:    false,
			description: "Table with primary key and multiple unique keys should not be splitable",
		},
		{
			name:        "table_with_pk_and_index",
			createSQL:   "CREATE TABLE t9 (id INT PRIMARY KEY, name VARCHAR(32), INDEX idx_name(name));",
			expected:    true,
			description: "Table with primary key and non-unique index should be splitable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create table using DDL
			job := helper.DDL2Job(tc.createSQL)
			modelTableInfo := helper.GetModelTableInfo(job)

			// Convert model.TableInfo to common.TableInfo
			commonTableInfo := common.WrapTableInfo("test", modelTableInfo)

			commonResult := IsSplitable(commonTableInfo)

			// Verify the result matches expected value
			require.Equal(t, tc.expected, commonResult,
				"Test case: %s - %s", tc.name, tc.description)
		})
	}
}
