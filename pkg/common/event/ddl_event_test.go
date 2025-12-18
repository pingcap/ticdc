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
	"encoding/binary"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	ddlEvent := &DDLEvent{
		Version:      DDLEventVersion1,
		DispatcherID: common.NewDispatcherID(),
		Type:         byte(ddlJob.Type),
		SchemaID:     ddlJob.SchemaID,
		SchemaName:   ddlJob.SchemaName,
		TableName:    ddlJob.TableName,
		Query:        ddlJob.Query,
		TableInfo:    common.WrapTableInfo(ddlJob.SchemaName, ddlJob.BinlogInfo.TableInfo),
		FinishedTs:   ddlJob.BinlogInfo.FinishedTS,
		BlockedTableNames: []SchemaTableName{
			{SchemaName: ddlJob.SchemaName, TableName: ddlJob.TableName},
		},
		Err: errors.ErrDDLEventError.GenWithStackByArgs("test").Error(),
	}
	ddlEvent.TableInfo.InitPrivateFields()

	// Test normal marshal/unmarshal
	data, err := ddlEvent.Marshal()
	require.Nil(t, err)
	require.Greater(t, len(data), 16, "data should include header")

	// Verify header format: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	require.Equal(t, uint32(0xDA7A6A6A), binary.BigEndian.Uint32(data[0:4]), "magic bytes")
	require.Equal(t, uint16(TypeDDLEvent), binary.BigEndian.Uint16(data[4:6]), "event type")
	require.Equal(t, uint16(DDLEventVersion1), binary.BigEndian.Uint16(data[6:8]), "version")

	data2 := make([]byte, len(data))
	copy(data2, data)

	reverseEvent := &DDLEvent{}
	err = reverseEvent.Unmarshal(data)
	reverseEvent.eventSize = 0
	require.Nil(t, err)

	// Compare individual fields instead of using DeepEqual
	require.Equal(t, ddlEvent.Version, reverseEvent.Version)
	require.Equal(t, ddlEvent.DispatcherID, reverseEvent.DispatcherID)
	require.Equal(t, ddlEvent.Type, reverseEvent.Type)
	require.Equal(t, ddlEvent.SchemaID, reverseEvent.SchemaID)
	require.Equal(t, ddlEvent.SchemaName, reverseEvent.SchemaName)
	require.Equal(t, ddlEvent.TableName, reverseEvent.TableName)
	require.Equal(t, ddlEvent.Query, reverseEvent.Query)
	require.Equal(t, ddlEvent.TableInfo, reverseEvent.TableInfo)
	require.Equal(t, ddlEvent.FinishedTs, reverseEvent.FinishedTs)
	require.Equal(t, ddlEvent.Err, reverseEvent.Err)
	require.Equal(t, ddlEvent.BlockedTableNames, reverseEvent.BlockedTableNames)

	// Test unsupported version in Marshal
	mockDDLVersion1 := 99
	ddlEvent.Version = mockDDLVersion1
	_, err = ddlEvent.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported DDLEvent version")

	// Test unsupported version in Unmarshal
	binary.BigEndian.PutUint16(data2[6:8], uint16(mockDDLVersion1)) // version is at bytes 6-7 in new format
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported DDLEvent version")

	// Test invalid magic bytes
	data2[0] = 0xFF
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Test data too short (less than header size)
	shortData := []byte{0xDA, 0x7A, 0x6A}
	err = reverseEvent.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test incomplete payload
	incompleteData := make([]byte, 16)
	// Set magic bytes
	binary.BigEndian.PutUint32(incompleteData[0:4], 0xDA7A6A6A)
	// Set event type
	binary.BigEndian.PutUint16(incompleteData[4:6], uint16(TypeDDLEvent))
	// Set version
	binary.BigEndian.PutUint16(incompleteData[6:8], uint16(DDLEventVersion1))
	// Set payload length to 100 but don't provide that much data
	binary.BigEndian.PutUint64(incompleteData[8:16], 100)
	err = reverseEvent.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete data")
}

// TestSplitQueries tests the SplitQueries function
func TestSplitQueries(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expected      []string
		expectedError bool
	}{
		{
			name:          "Empty input",
			input:         "",
			expected:      []string(nil),
			expectedError: false,
		},
		{
			name:          "Single query without trailing semicolon",
			input:         "CREATE TABLE test (id INT)",
			expected:      []string{"CREATE TABLE `test` (`id` INT);"},
			expectedError: false,
		},
		{
			name:          "Single query with trailing semicolon",
			input:         "CREATE TABLE test (id INT);",
			expected:      []string{"CREATE TABLE `test` (`id` INT);"},
			expectedError: false,
		},
		{
			name: "Multiple queries with trailing semicolons",
			input: `
CREATE TABLE test1 (id INT);
CREATE TABLE test2 (name VARCHAR(20));
INSERT INTO test1 VALUES (1);
`,
			expected: []string{
				"CREATE TABLE `test1` (`id` INT);",
				"CREATE TABLE `test2` (`name` VARCHAR(20));",
				"INSERT INTO `test1` VALUES (1);",
			},
			expectedError: false,
		},
		{
			name: "Query with semicolons inside column values",
			input: `
CREATE TABLE test (name VARCHAR(50));
INSERT INTO test VALUES ('This; is; a test');
`,
			expected: []string{
				"CREATE TABLE `test` (`name` VARCHAR(50));",
				"INSERT INTO `test` VALUES (_UTF8MB4'This; is; a test');",
			},
			expectedError: false,
		},
		{
			name: "Query with escaped quotes inside strings",
			input: `
CREATE TABLE test (name VARCHAR(50));
INSERT INTO test VALUES ('This ''is'' a test');
`,
			expected: []string{
				"CREATE TABLE `test` (`name` VARCHAR(50));",
				"INSERT INTO `test` VALUES (_UTF8MB4'This ''is'' a test');",
			},
			expectedError: false,
		},
		{
			name: "Nested queries or functions with semicolons",
			input: `
CREATE TABLE test (id INT, name VARCHAR(50));
INSERT INTO test VALUES (1, CONCAT('Name;', 'Test'));
`,
			expected: []string{
				"CREATE TABLE `test` (`id` INT,`name` VARCHAR(50));",
				"INSERT INTO `test` VALUES (1,CONCAT(_UTF8MB4'Name;', _UTF8MB4'Test'));",
			},
			expectedError: false,
		},
		{
			name:          "Malformed SQL query",
			input:         "CREATE TABLE test (id INT;",
			expected:      nil,
			expectedError: true,
		},
		{
			name: "SQL injection edge case",
			input: `
CREATE TABLE users (id INT, name VARCHAR(50));
INSERT INTO users VALUES (1, 'test; DROP TABLE users; --');
`,
			expected: []string{
				"CREATE TABLE `users` (`id` INT,`name` VARCHAR(50));",
				"INSERT INTO `users` VALUES (1,_UTF8MB4'test; DROP TABLE users; --');",
			},
			expectedError: false,
		},
		{
			name: "Complex queries with comments",
			input: `
-- This is a comment
CREATE TABLE test (id INT); -- Inline comment
/* Multi-line
comment */
INSERT INTO test VALUES (1);
`,
			expected: []string{
				"CREATE TABLE `test` (`id` INT);",
				"INSERT INTO `test` VALUES (1);",
			},
			expectedError: false,
		},
		{
			name: "Queries with whitespace and newlines",
			input: `
    
    CREATE TABLE test (id INT);
    
    INSERT INTO test VALUES (1);
    
`,
			expected: []string{
				"CREATE TABLE `test` (`id` INT);",
				"INSERT INTO `test` VALUES (1);",
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SplitQueries(tt.input)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestDDLEventCloneForRouting tests the CloneForRouting method to ensure it properly
// clones DDL events to avoid race conditions between multiple dispatchers
func TestDDLEventCloneForRouting(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	// Create original DDL event with all fields populated
	originalTableInfo := common.WrapTableInfo(ddlJob.SchemaName, ddlJob.BinlogInfo.TableInfo)
	originalTableInfo.InitPrivateFields()

	multipleTableInfo1 := common.WrapTableInfo("schema1", ddlJob.BinlogInfo.TableInfo)
	multipleTableInfo1.InitPrivateFields()
	multipleTableInfo2 := common.WrapTableInfo("schema2", ddlJob.BinlogInfo.TableInfo)
	multipleTableInfo2.InitPrivateFields()

	postFlushFunc1 := func() {}
	postFlushFunc2 := func() {}

	original := &DDLEvent{
		Version:            DDLEventVersion1,
		DispatcherID:       common.NewDispatcherID(),
		Type:               byte(ddlJob.Type),
		SchemaID:           ddlJob.SchemaID,
		SchemaName:         ddlJob.SchemaName,
		TableName:          ddlJob.TableName,
		TargetSchemaName:   "",
		Query:              ddlJob.Query,
		TableInfo:          originalTableInfo,
		FinishedTs:         ddlJob.BinlogInfo.FinishedTS,
		Seq:                1,
		Epoch:              2,
		MultipleTableInfos: []*common.TableInfo{multipleTableInfo1, multipleTableInfo2},
		PostTxnFlushed:     []func(){postFlushFunc1, postFlushFunc2},
		TiDBOnly:           true,
		BDRMode:            "test-mode",
	}

	// Clone the event
	cloned := original.CloneForRouting()
	require.NotNil(t, cloned)

	// Verify that cloned is a separate object
	require.False(t, original == cloned, "cloned event should be a different object")

	// Verify that immutable fields are shared (shallow copy)
	require.Equal(t, original.Version, cloned.Version)
	require.Equal(t, original.DispatcherID, cloned.DispatcherID)
	require.Equal(t, original.Type, cloned.Type)
	require.Equal(t, original.SchemaID, cloned.SchemaID)
	require.Equal(t, original.SchemaName, cloned.SchemaName)
	require.Equal(t, original.TableName, cloned.TableName)
	require.Equal(t, original.Query, cloned.Query)
	require.Equal(t, original.FinishedTs, cloned.FinishedTs)
	require.Equal(t, original.Seq, cloned.Seq)
	require.Equal(t, original.Epoch, cloned.Epoch)
	require.Equal(t, original.TiDBOnly, cloned.TiDBOnly)
	require.Equal(t, original.BDRMode, cloned.BDRMode)

	// Verify that TableInfo pointer is shared initially
	require.True(t, original.TableInfo == cloned.TableInfo, "TableInfo should be shared initially")

	// Verify that MultipleTableInfos is a new slice (but points to same TableInfo objects initially)
	require.False(t, &original.MultipleTableInfos[0] == &cloned.MultipleTableInfos[0], "MultipleTableInfos should be a new slice")
	require.True(t, original.MultipleTableInfos[0] == cloned.MultipleTableInfos[0], "MultipleTableInfos elements should be shared initially")
	require.True(t, original.MultipleTableInfos[1] == cloned.MultipleTableInfos[1], "MultipleTableInfos elements should be shared initially")

	// Verify that PostTxnFlushed is shared (same slice header)
	require.NotNil(t, cloned.PostTxnFlushed)
	require.Equal(t, 2, len(cloned.PostTxnFlushed), "PostTxnFlushed should be shared with original")
	require.Equal(t, 2, len(original.PostTxnFlushed), "Original PostTxnFlushed should remain unchanged")
	// Both should point to the same underlying array since it's a shallow copy
	require.Equal(t, &original.PostTxnFlushed[0], &cloned.PostTxnFlushed[0], "PostTxnFlushed should share the same underlying array")

	// Now simulate what happens during routing: mutate the cloned event
	cloned.TargetSchemaName = "routed_schema"
	cloned.Query = "CREATE TABLE routed_schema.test ..."
	newRoutedTableInfo := originalTableInfo.CloneWithRouting("routed_schema", "test")
	cloned.TableInfo = newRoutedTableInfo
	cloned.MultipleTableInfos[0] = multipleTableInfo1.CloneWithRouting("routed_schema1", "table1")
	cloned.MultipleTableInfos[1] = multipleTableInfo2.CloneWithRouting("routed_schema2", "table2")

	// Verify that mutations to cloned event don't affect the original
	require.Equal(t, "", original.TargetSchemaName, "Original TargetSchemaName should be unchanged")
	require.Equal(t, ddlJob.Query, original.Query, "Original Query should be unchanged")
	require.True(t, original.TableInfo == originalTableInfo, "Original TableInfo should be unchanged")
	require.True(t, original.MultipleTableInfos[0] == multipleTableInfo1, "Original MultipleTableInfos[0] should be unchanged")
	require.True(t, original.MultipleTableInfos[1] == multipleTableInfo2, "Original MultipleTableInfos[1] should be unchanged")

	// Verify that cloned event has the mutations
	require.Equal(t, "routed_schema", cloned.TargetSchemaName)
	require.Equal(t, "CREATE TABLE routed_schema.test ...", cloned.Query)
	require.True(t, cloned.TableInfo == newRoutedTableInfo)
	require.Equal(t, "routed_schema", cloned.TableInfo.TableName.TargetSchema)

	// Test cloning nil event
	var nilEvent *DDLEvent
	clonedNil := nilEvent.CloneForRouting()
	require.Nil(t, clonedNil)
}
