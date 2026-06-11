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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type legacyDDLEventJSON struct {
	Version     int    `json:"version"`
	Type        byte   `json:"type"`
	SchemaID    int64  `json:"schema_id"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	Query       string `json:"query"`
	StartTs     uint64 `json:"start_ts"`
	FinishedTs  uint64 `json:"finished_ts"`
	Seq         uint64 `json:"seq"`
	Epoch       uint64 `json:"epoch"`
	TiDBOnly    bool   `json:"tidb_only"`
	BDRMode     string `json:"bdr_mode"`
	Err         string `json:"err"`
	NotSync     bool   `msg:"not_sync"`
	IsBootstrap bool   `json:"-"`
}

func buildDDLEventV1Payload(
	t *testing.T,
	restData []byte,
	dispatcherID common.DispatcherID,
) []byte {
	t.Helper()

	dispatcherIDData := dispatcherID.Marshal()
	dispatcherIDDataSize := make([]byte, 8)
	binary.BigEndian.PutUint64(dispatcherIDDataSize, uint64(len(dispatcherIDData)))

	tableInfoDataSize := make([]byte, 8)
	binary.BigEndian.PutUint64(tableInfoDataSize, 0)

	multipleTableInfosDataSize := make([]byte, 8)
	binary.BigEndian.PutUint64(multipleTableInfosDataSize, 0)

	payload := append([]byte{}, restData...)
	payload = append(payload, dispatcherIDData...)
	payload = append(payload, dispatcherIDDataSize...)
	payload = append(payload, tableInfoDataSize...)
	payload = append(payload, multipleTableInfosDataSize...)
	return payload
}

func extractDDLEventV1RestData(t *testing.T, payload []byte) []byte {
	t.Helper()

	end := len(payload)
	require.GreaterOrEqual(t, end, 24, "payload should contain footer fields")

	multipleTableInfoCount := binary.BigEndian.Uint64(payload[end-8 : end])
	require.Equal(t, uint64(0), multipleTableInfoCount)
	end -= 8

	tableInfoDataSize := binary.BigEndian.Uint64(payload[end-8 : end])
	require.Equal(t, uint64(0), tableInfoDataSize)
	end -= 8

	dispatcherIDDataSize := binary.BigEndian.Uint64(payload[end-8 : end])
	require.Greater(t, dispatcherIDDataSize, uint64(0))
	require.LessOrEqual(t, dispatcherIDDataSize, uint64(end-8))

	restDataEnd := end - 8 - int(dispatcherIDDataSize)
	require.GreaterOrEqual(t, restDataEnd, 0)
	return payload[:restDataEnd]
}

func TestDDLEventNotSyncJSONCompatibility(t *testing.T) {
	ddlEvent := DDLEvent{
		Version:  DDLEventVersion1,
		NotSync:  true,
		BDRMode:  "bdr",
		Err:      "err",
		SchemaID: 100,
	}

	// Marshal should emit both new and legacy keys for mixed-version compatibility.
	data, err := json.Marshal(ddlEvent)
	require.NoError(t, err)

	payload := make(map[string]any)
	require.NoError(t, json.Unmarshal(data, &payload))
	require.Equal(t, true, payload["not_sync"])
	require.Equal(t, true, payload["NotSync"])

	// Unmarshal should accept the new key.
	var fromNew DDLEvent
	require.NoError(t, json.Unmarshal([]byte(`{"not_sync":true}`), &fromNew))
	require.True(t, fromNew.NotSync)

	// Unmarshal should also accept the legacy key.
	var fromLegacy DDLEvent
	require.NoError(t, json.Unmarshal([]byte(`{"NotSync":true}`), &fromLegacy))
	require.True(t, fromLegacy.NotSync)

	// If both keys are present, the new key takes precedence.
	var withBoth DDLEvent
	require.NoError(t, json.Unmarshal([]byte(`{"NotSync":false,"not_sync":true}`), &withBoth))
	require.True(t, withBoth.NotSync)
}

func TestDDLEventRollingUpgradeLegacyMarshalNewUnmarshal(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	legacy := legacyDDLEventJSON{
		Version:    DDLEventVersion1,
		Type:       1,
		SchemaID:   101,
		SchemaName: "test",
		TableName:  "t1",
		Query:      "create table test.t1(id int)",
		StartTs:    1000,
		FinishedTs: 2000,
		Seq:        10,
		Epoch:      20,
		TiDBOnly:   true,
		BDRMode:    "sync",
		Err:        "legacy",
		NotSync:    true,
	}

	restData, err := json.Marshal(legacy)
	require.NoError(t, err)

	// Legacy marshal uses NotSync as JSON key and keeps the same V1 binary layout.
	payload := buildDDLEventV1Payload(t, restData, dispatcherID)
	data, err := MarshalEventWithHeader(TypeDDLEvent, DDLEventVersion1, payload)
	require.NoError(t, err)

	var newEvent DDLEvent
	require.NoError(t, newEvent.Unmarshal(data))
	require.Equal(t, legacy.Version, newEvent.Version)
	require.Equal(t, legacy.Type, newEvent.Type)
	require.Equal(t, legacy.SchemaID, newEvent.SchemaID)
	require.Equal(t, legacy.SchemaName, newEvent.SchemaName)
	require.Equal(t, legacy.TableName, newEvent.TableName)
	require.Equal(t, legacy.Query, newEvent.Query)
	require.Equal(t, legacy.StartTs, newEvent.StartTs)
	require.Equal(t, legacy.FinishedTs, newEvent.FinishedTs)
	require.Equal(t, legacy.Seq, newEvent.Seq)
	require.Equal(t, legacy.Epoch, newEvent.Epoch)
	require.Equal(t, legacy.TiDBOnly, newEvent.TiDBOnly)
	require.Equal(t, legacy.BDRMode, newEvent.BDRMode)
	require.Equal(t, legacy.Err, newEvent.Err)
	require.True(t, newEvent.NotSync)
}

func TestDDLEventRollingUpgradeNewMarshalLegacyUnmarshal(t *testing.T) {
	newEvent := DDLEvent{
		Version:      DDLEventVersion1,
		DispatcherID: common.NewDispatcherID(),
		Type:         2,
		SchemaID:     102,
		SchemaName:   "test",
		TableName:    "t2",
		Query:        "alter table test.t2 add column c int",
		StartTs:      3000,
		FinishedTs:   4000,
		Seq:          30,
		Epoch:        40,
		TiDBOnly:     false,
		BDRMode:      "async",
		Err:          "new",
		NotSync:      true,
	}

	data, err := newEvent.Marshal()
	require.NoError(t, err)

	payload, version, err := ValidateAndExtractPayload(data, TypeDDLEvent)
	require.NoError(t, err)
	require.Equal(t, DDLEventVersion1, version)

	restData := extractDDLEventV1RestData(t, payload)
	var legacy legacyDDLEventJSON
	require.NoError(t, json.Unmarshal(restData, &legacy))

	// Legacy receiver reads NotSync from legacy key and ignores not_sync.
	require.True(t, legacy.NotSync)
	require.Equal(t, newEvent.SchemaID, legacy.SchemaID)
	require.Equal(t, newEvent.SchemaName, legacy.SchemaName)
	require.Equal(t, newEvent.TableName, legacy.TableName)

	rawMap := make(map[string]any)
	require.NoError(t, json.Unmarshal(restData, &rawMap))
	_, hasLegacyKey := rawMap["NotSync"]
	_, hasNewKey := rawMap["not_sync"]
	require.True(t, hasLegacyKey, fmt.Sprintf("legacy key should exist in payload: %s", string(restData)))
	require.True(t, hasNewKey, fmt.Sprintf("new key should exist in payload: %s", string(restData)))
}

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
		// NotSync must survive Marshal/Unmarshal because it controls whether dispatchers
		// should forward this DDL to downstream sinks.
		NotSync: true,
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
	require.Equal(t, ddlEvent.NotSync, reverseEvent.NotSync)

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

func TestDDLEventDecodeV1WithMultipleTableInfos(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob1 := helper.DDL2Job("create table t1 (id int primary key)")
	ddlJob2 := helper.DDL2Job("create table t2 (id int primary key)")
	require.NotNil(t, ddlJob1)
	require.NotNil(t, ddlJob2)

	tableInfo1 := helper.GetTableInfo(ddlJob1)
	tableInfo2 := helper.GetTableInfo(ddlJob2)
	require.NotNil(t, tableInfo1)
	require.NotNil(t, tableInfo2)

	ddlEvent := &DDLEvent{
		Version:            DDLEventVersion1,
		DispatcherID:       common.NewDispatcherID(),
		Type:               byte(ddlJob2.Type),
		SchemaID:           ddlJob2.SchemaID,
		SchemaName:         ddlJob2.SchemaName,
		TableName:          ddlJob2.TableName,
		Query:              ddlJob2.Query,
		TableInfo:          tableInfo2,
		FinishedTs:         ddlJob2.BinlogInfo.FinishedTS,
		MultipleTableInfos: []*common.TableInfo{tableInfo1, tableInfo2},
	}

	data, err := ddlEvent.Marshal()
	require.NoError(t, err)

	reverseEvent := &DDLEvent{}
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)
	reverseEvent.eventSize = 0

	require.Equal(t, ddlEvent.DispatcherID, reverseEvent.DispatcherID)
	require.NotNil(t, reverseEvent.TableInfo)
	require.Equal(t, ddlEvent.TableInfo.TableName.TableID, reverseEvent.TableInfo.TableName.TableID)
	require.Len(t, reverseEvent.MultipleTableInfos, 2)
	require.Equal(t, ddlEvent.MultipleTableInfos[0].TableName.TableID, reverseEvent.MultipleTableInfos[0].TableName.TableID)
	require.Equal(t, ddlEvent.MultipleTableInfos[1].TableName.TableID, reverseEvent.MultipleTableInfos[1].TableName.TableID)

	// Reuse the same object to ensure decode does not keep stale entries.
	err = reverseEvent.Unmarshal(data)
	require.NoError(t, err)
	require.Len(t, reverseEvent.MultipleTableInfos, 2)
	require.Equal(t, ddlEvent.MultipleTableInfos[0].TableName.TableID, reverseEvent.MultipleTableInfos[0].TableName.TableID)
	require.Equal(t, ddlEvent.MultipleTableInfos[1].TableName.TableID, reverseEvent.MultipleTableInfos[1].TableName.TableID)
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
