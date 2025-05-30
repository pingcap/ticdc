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

package util

import (
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// TableSchemaStore is store some schema info for dispatchers.
// It is responsible for
// 1. [By TableNameStore]provide all the table name of the specified ts(only support incremental ts), mainly for generate topic for kafka sink when send watermark.
// 2. [By TableIDStore]provide the tableids based on schema-id or all tableids when send ddl ts in mysql sink.
//
// TableSchemaStore only exists in the table trigger event dispatcher, and the same instance's sink of this changefeed,
// which means each changefeed only has one TableSchemaStore.
// for mysql sink, tableSchemaStore only need the id-class infos; otherwise, it only need name-class infos.
type TableSchemaStore struct {
	sinkType       commonType.SinkType
	tableNameStore *TableNameStore
	tableIDStore   *TableIDStore
}

func NewTableSchemaStore(schemaInfo []*heartbeatpb.SchemaInfo, sinkType commonType.SinkType) *TableSchemaStore {
	tableSchemaStore := &TableSchemaStore{
		sinkType: sinkType,
		tableIDStore: &TableIDStore{
			schemaIDToTableIDs: make(map[int64]map[int64]interface{}),
			tableIDToSchemaID:  make(map[int64]int64),
		},
	}
	switch sinkType {
	case commonType.MysqlSinkType:
		for _, schema := range schemaInfo {
			schemaID := schema.SchemaID
			for _, table := range schema.Tables {
				tableID := table.TableID
				tableSchemaStore.tableIDStore.Add(schemaID, tableID)
			}
		}
	default:
		tableSchemaStore.tableNameStore = &TableNameStore{
			existingTables:         make(map[string]map[string]*commonEvent.SchemaTableName),
			latestTableNameChanges: &LatestTableNameChanges{m: make(map[uint64]*commonEvent.TableNameChange)},
		}
		for _, schema := range schemaInfo {
			schemaName := schema.SchemaName
			schemaID := schema.SchemaID
			for _, table := range schema.Tables {
				tableName := table.TableName
				tableSchemaStore.tableNameStore.Add(schemaName, tableName)
				tableID := table.TableID
				tableSchemaStore.tableIDStore.Add(schemaID, tableID)
			}
		}
	}
	return tableSchemaStore
}

func (s *TableSchemaStore) Clear() {
	s = nil
}

func (s *TableSchemaStore) AddEvent(event *commonEvent.DDLEvent) {
	if s.sinkType == commonType.MysqlSinkType {
		s.tableIDStore.AddEvent(event)
	} else {
		s.tableNameStore.AddEvent(event)
	}
}

func (s *TableSchemaStore) initialized() bool {
	if s == nil || (s.tableIDStore == nil && s.tableNameStore == nil) {
		log.Panic("TableSchemaStore is not initialized", zap.Any("tableSchemaStore", s))
		return false
	}
	return true
}

func (s *TableSchemaStore) GetTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() {
		return nil
	}
	return s.tableIDStore.GetTableIdsByDB(schemaID)
}

// GetNormalTableIdsByDB will not return table id = 0 , this is the only different between GetTableIdsByDB and GetNormalTableIdsByDB
func (s *TableSchemaStore) GetNormalTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() {
		return nil
	}
	return s.tableIDStore.GetNormalTableIdsByDB(schemaID)
}

func (s *TableSchemaStore) GetAllTableIds() []int64 {
	if !s.initialized() {
		return nil
	}
	return s.tableIDStore.GetAllTableIds()
}

// GetAllNormalTableIds will not return table id = 0 , this is the only different between GetAllNormalTableIds and GetAllTableIds
func (s *TableSchemaStore) GetAllNormalTableIds() []int64 {
	if !s.initialized() {
		return nil
	}
	return s.tableIDStore.GetAllNormalTableIds()
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableSchemaStore) GetAllTableNames(ts uint64) []*commonEvent.SchemaTableName {
	if !s.initialized() {
		return nil
	}
	return s.tableNameStore.GetAllTableNames(ts)
}

type LatestTableNameChanges struct {
	mutex sync.Mutex
	m     map[uint64]*commonEvent.TableNameChange
}

func (l *LatestTableNameChanges) Add(ddlEvent *commonEvent.DDLEvent) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.m[ddlEvent.GetCommitTs()] = ddlEvent.TableNameChange
}

type TableNameStore struct {
	// store all the existing table which existed at the latest query ts
	existingTables map[string]map[string]*commonEvent.SchemaTableName // databaseName -> {tableName -> SchemaTableName}
	// store the change of table name from the latest query ts to now(latest event)
	latestTableNameChanges *LatestTableNameChanges
}

func (s *TableNameStore) Add(databaseName string, tableName string) {
	if s.existingTables[databaseName] == nil {
		s.existingTables[databaseName] = make(map[string]*commonEvent.SchemaTableName, 0)
	}
	s.existingTables[databaseName][tableName] = &commonEvent.SchemaTableName{
		SchemaName: databaseName,
		TableName:  tableName,
	}
}

func (s *TableNameStore) AddEvent(event *commonEvent.DDLEvent) {
	if event.TableNameChange != nil {
		s.latestTableNameChanges.Add(event)
	}
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableNameStore) GetAllTableNames(ts uint64) []*commonEvent.SchemaTableName {
	// we have to send checkpointTs to the drop schema/tables so that consumer can know the schema/table is dropped.
	tableNames := make([]*commonEvent.SchemaTableName, 0)
	s.latestTableNameChanges.mutex.Lock()
	if len(s.latestTableNameChanges.m) > 0 {
		// update the existingTables with the latest table changes <= ts
		for commitTs, tableNameChange := range s.latestTableNameChanges.m {
			if commitTs <= ts {
				if tableNameChange.DropDatabaseName != "" {
					tableNames = append(tableNames, &commonEvent.SchemaTableName{
						SchemaName: tableNameChange.DropDatabaseName,
					})
					delete(s.existingTables, tableNameChange.DropDatabaseName)
				} else {
					for _, addName := range tableNameChange.AddName {
						if s.existingTables[addName.SchemaName] == nil {
							s.existingTables[addName.SchemaName] = make(map[string]*commonEvent.SchemaTableName, 0)
						}
						s.existingTables[addName.SchemaName][addName.TableName] = &addName
					}
					for _, dropName := range tableNameChange.DropName {
						tableNames = append(tableNames, &dropName)
						delete(s.existingTables[dropName.SchemaName], dropName.TableName)
						if len(s.existingTables[dropName.SchemaName]) == 0 {
							delete(s.existingTables, dropName.SchemaName)
						}
					}
				}
				delete(s.latestTableNameChanges.m, commitTs)
			}
		}
	}
	s.latestTableNameChanges.mutex.Unlock()

	for _, tables := range s.existingTables {
		for _, tableName := range tables {
			tableNames = append(tableNames, tableName)
		}
	}
	return tableNames
}

type TableIDStore struct {
	mutex              sync.Mutex
	schemaIDToTableIDs map[int64]map[int64]interface{} // schemaID -> tableIDs
	tableIDToSchemaID  map[int64]int64                 // tableID -> schemaID
}

func (s *TableIDStore) Add(schemaID int64, tableID int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.schemaIDToTableIDs[schemaID] == nil {
		s.schemaIDToTableIDs[schemaID] = make(map[int64]interface{})
	}
	s.schemaIDToTableIDs[schemaID][tableID] = nil
	s.tableIDToSchemaID[tableID] = schemaID
}

func (s *TableIDStore) AddEvent(event *commonEvent.DDLEvent) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(event.NeedAddedTables) != 0 {
		for _, table := range event.NeedAddedTables {
			if s.schemaIDToTableIDs[table.SchemaID] == nil {
				s.schemaIDToTableIDs[table.SchemaID] = make(map[int64]interface{})
			}
			s.schemaIDToTableIDs[table.SchemaID][table.TableID] = nil
			s.tableIDToSchemaID[table.TableID] = table.SchemaID
		}
	}

	if event.NeedDroppedTables != nil {
		switch event.NeedDroppedTables.InfluenceType {
		case commonEvent.InfluenceTypeNormal:
			for _, tableID := range event.NeedDroppedTables.TableIDs {
				schemaId := s.tableIDToSchemaID[tableID]
				delete(s.schemaIDToTableIDs[schemaId], tableID)
				if len(s.schemaIDToTableIDs[schemaId]) == 0 {
					delete(s.schemaIDToTableIDs, schemaId)
				}
				delete(s.tableIDToSchemaID, tableID)
			}
		case commonEvent.InfluenceTypeDB:
			tables := s.schemaIDToTableIDs[event.NeedDroppedTables.SchemaID]
			for tableID := range tables {
				delete(s.tableIDToSchemaID, tableID)
			}
			delete(s.schemaIDToTableIDs, event.NeedDroppedTables.SchemaID)
		case commonEvent.InfluenceTypeAll:
			log.Error("Should not reach here, InfluenceTypeAll is should not be used in NeedDroppedTables")
		default:
			log.Error("Unknown InfluenceType")
		}
	}

	if event.UpdatedSchemas != nil {
		for _, schemaIDChange := range event.UpdatedSchemas {
			delete(s.schemaIDToTableIDs[schemaIDChange.OldSchemaID], schemaIDChange.TableID)
			if len(s.schemaIDToTableIDs[schemaIDChange.OldSchemaID]) == 0 {
				delete(s.schemaIDToTableIDs, schemaIDChange.OldSchemaID)
			}

			if s.schemaIDToTableIDs[schemaIDChange.NewSchemaID] == nil {
				s.schemaIDToTableIDs[schemaIDChange.NewSchemaID] = make(map[int64]interface{})
			}
			s.schemaIDToTableIDs[schemaIDChange.NewSchemaID][schemaIDChange.TableID] = nil
			s.tableIDToSchemaID[schemaIDChange.TableID] = schemaIDChange.NewSchemaID
		}
	}
}

func (s *TableIDStore) GetNormalTableIdsByDB(schemaID int64) []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tables := s.schemaIDToTableIDs[schemaID]
	tableIds := make([]int64, 0, len(tables))
	for tableID := range tables {
		tableIds = append(tableIds, tableID)
	}
	return tableIds
}

func (s *TableIDStore) GetTableIdsByDB(schemaID int64) []int64 {
	tableIds := s.GetNormalTableIdsByDB(schemaID)
	// Add the table id of the span of table trigger event dispatcher
	// Each influence-DB ddl must have table trigger event dispatcher's participation
	tableIds = append(tableIds, commonType.DDLSpan.TableID)
	return tableIds
}

func (s *TableIDStore) GetAllNormalTableIds() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	tableIds := make([]int64, 0, len(s.tableIDToSchemaID))
	for tableID := range s.tableIDToSchemaID {
		tableIds = append(tableIds, tableID)
	}
	return tableIds
}

func (s *TableIDStore) GetAllTableIds() []int64 {
	tableIds := s.GetAllNormalTableIds()
	// Add the table id of the span of table trigger event dispatcher
	// Each influence-DB ddl must have table trigger event dispatcher's participation
	tableIds = append(tableIds, commonType.DDLSpan.TableID)
	return tableIds
}
