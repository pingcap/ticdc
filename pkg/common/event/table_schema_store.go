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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/zap"
)

//go:generate msgp
//msgp:maps autoshim

// TableSchemaStore is store some schema info for dispatchers.
// It is responsible for
// 1. [By TableNameStore]provide all the table name of the specified ts(only support incremental ts), mainly for generate topic for kafka sink when send watermark.
// 2. [By TableIDStore]provide the tableids based on schema-id or all tableids when send ddl ts in mysql sink.
//
// TableSchemaStore only exists in the table trigger event dispatcher, and the same instance's sink of this changefeed,
// which means each changefeed only has one TableSchemaStore.
// for mysql sink, tableSchemaStore only need the id-class infos; otherwise, it only need name-class infos.
type TableSchemaStore struct {
	sinkType       commonType.SinkType `msg:"-"`
	tableNameStore *TableNameStore     `msg:"-"`
	// TableIDStore will be used in redo ddl event to record all block tables id,
	// so it has to support Marshal/Unmarshal
	TableIDStore *TableIDStore `msg:"table_id_store"`
}

func NewTableSchemaStore(schemaInfo []*heartbeatpb.SchemaInfo, sinkType commonType.SinkType) *TableSchemaStore {
	tableSchemaStore := &TableSchemaStore{
		sinkType: sinkType,
		TableIDStore: &TableIDStore{
			SchemaIDToTableIDs: make(map[int64]map[int64]interface{}),
			TableIDToSchemaID:  make(map[int64]int64),
		},
	}
	switch sinkType {
	case commonType.MysqlSinkType, commonType.RedoSinkType:
		for _, schema := range schemaInfo {
			schemaID := schema.SchemaID
			for _, table := range schema.Tables {
				tableID := table.TableID
				tableSchemaStore.TableIDStore.Add(schemaID, tableID)
			}
		}
	default:
		tableSchemaStore.tableNameStore = &TableNameStore{
			existingTables:         make(map[string]map[string]*SchemaTableName),
			latestTableNameChanges: &LatestTableNameChanges{m: make(map[uint64]*TableNameChange)},
		}
		for _, schema := range schemaInfo {
			schemaName := schema.SchemaName
			schemaID := schema.SchemaID
			for _, table := range schema.Tables {
				tableName := table.TableName
				tableSchemaStore.tableNameStore.Add(schemaName, tableName)
				tableID := table.TableID
				tableSchemaStore.TableIDStore.Add(schemaID, tableID)
			}
		}
	}
	return tableSchemaStore
}

func (s *TableSchemaStore) Clear() {
	s = nil
}

func (s *TableSchemaStore) AddEvent(event *DDLEvent) {
	switch s.sinkType {
	case commonType.MysqlSinkType, commonType.RedoSinkType:
		s.TableIDStore.AddEvent(event)
	default:
		s.tableNameStore.AddEvent(event)
	}
}

func (s *TableSchemaStore) initialized() bool {
	if s == nil || (s.TableIDStore == nil && s.tableNameStore == nil) {
		log.Panic("TableSchemaStore is not initialized", zap.Any("tableSchemaStore", s))
		return false
	}
	return true
}

func (s *TableSchemaStore) GetTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() {
		return nil
	}
	return s.TableIDStore.GetTableIdsByDB(schemaID)
}

// GetNormalTableIdsByDB will not return table id = 0 , this is the only different between GetTableIdsByDB and GetNormalTableIdsByDB
func (s *TableSchemaStore) GetNormalTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() {
		return nil
	}
	return s.TableIDStore.GetNormalTableIdsByDB(schemaID)
}

func (s *TableSchemaStore) GetAllTableIds() []int64 {
	if !s.initialized() {
		return nil
	}
	return s.TableIDStore.GetAllTableIds()
}

// GetAllNormalTableIds will not return table id = 0 , this is the only different between GetAllNormalTableIds and GetAllTableIds
func (s *TableSchemaStore) GetAllNormalTableIds() []int64 {
	if !s.initialized() {
		return nil
	}
	return s.TableIDStore.GetAllNormalTableIds()
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableSchemaStore) GetAllTableNames(ts uint64) []*SchemaTableName {
	if !s.initialized() {
		return nil
	}
	return s.tableNameStore.GetAllTableNames(ts)
}

//msgp:ignore LatestTableNameChanges
type LatestTableNameChanges struct {
	mutex sync.Mutex
	m     map[uint64]*TableNameChange
}

func (l *LatestTableNameChanges) Add(ddlEvent *DDLEvent) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.m[ddlEvent.GetCommitTs()] = ddlEvent.TableNameChange
}

//msgp:ignore TableNameStore
type TableNameStore struct {
	// store all the existing table which existed at the latest query ts
	existingTables map[string]map[string]*SchemaTableName // databaseName -> {tableName -> SchemaTableName}
	// store the change of table name from the latest query ts to now(latest event)
	latestTableNameChanges *LatestTableNameChanges
}

func (s *TableNameStore) Add(databaseName string, tableName string) {
	if s.existingTables[databaseName] == nil {
		s.existingTables[databaseName] = make(map[string]*SchemaTableName, 0)
	}
	s.existingTables[databaseName][tableName] = &SchemaTableName{
		SchemaName: databaseName,
		TableName:  tableName,
	}
}

func (s *TableNameStore) AddEvent(event *DDLEvent) {
	if event.TableNameChange != nil {
		s.latestTableNameChanges.Add(event)
	}
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableNameStore) GetAllTableNames(ts uint64) []*SchemaTableName {
	// we have to send checkpointTs to the drop schema/tables so that consumer can know the schema/table is dropped.
	tableNames := make([]*SchemaTableName, 0)
	s.latestTableNameChanges.mutex.Lock()
	if len(s.latestTableNameChanges.m) > 0 {
		// update the existingTables with the latest table changes <= ts
		for commitTs, tableNameChange := range s.latestTableNameChanges.m {
			if commitTs <= ts {
				if tableNameChange.DropDatabaseName != "" {
					tableNames = append(tableNames, &SchemaTableName{
						SchemaName: tableNameChange.DropDatabaseName,
					})
					delete(s.existingTables, tableNameChange.DropDatabaseName)
				} else {
					for _, addName := range tableNameChange.AddName {
						if s.existingTables[addName.SchemaName] == nil {
							s.existingTables[addName.SchemaName] = make(map[string]*SchemaTableName, 0)
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
	SchemaIDToTableIDs map[int64]map[int64]interface{} `msg:"schema_to_tables"` // schemaID -> tableIDs
	TableIDToSchemaID  map[int64]int64                 `msg:"table_to_schema"`  // tableID -> schemaID
}

func (s *TableIDStore) Add(schemaID int64, tableID int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.SchemaIDToTableIDs[schemaID] == nil {
		s.SchemaIDToTableIDs[schemaID] = make(map[int64]interface{})
	}
	s.SchemaIDToTableIDs[schemaID][tableID] = nil
	s.TableIDToSchemaID[tableID] = schemaID
}

func (s *TableIDStore) AddEvent(event *DDLEvent) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(event.NeedAddedTables) != 0 {
		for _, table := range event.NeedAddedTables {
			if s.SchemaIDToTableIDs[table.SchemaID] == nil {
				s.SchemaIDToTableIDs[table.SchemaID] = make(map[int64]interface{})
			}
			s.SchemaIDToTableIDs[table.SchemaID][table.TableID] = nil
			s.TableIDToSchemaID[table.TableID] = table.SchemaID
		}
	}

	if event.NeedDroppedTables != nil {
		switch event.NeedDroppedTables.InfluenceType {
		case InfluenceTypeNormal:
			for _, tableID := range event.NeedDroppedTables.TableIDs {
				schemaId := s.TableIDToSchemaID[tableID]
				delete(s.SchemaIDToTableIDs[schemaId], tableID)
				if len(s.SchemaIDToTableIDs[schemaId]) == 0 {
					delete(s.SchemaIDToTableIDs, schemaId)
				}
				delete(s.TableIDToSchemaID, tableID)
			}
		case InfluenceTypeDB:
			tables := s.SchemaIDToTableIDs[event.NeedDroppedTables.SchemaID]
			for tableID := range tables {
				delete(s.TableIDToSchemaID, tableID)
			}
			delete(s.SchemaIDToTableIDs, event.NeedDroppedTables.SchemaID)
		case InfluenceTypeAll:
			log.Error("Should not reach here, InfluenceTypeAll is should not be used in NeedDroppedTables")
		default:
			log.Error("Unknown InfluenceType")
		}
	}

	if event.UpdatedSchemas != nil {
		for _, schemaIDChange := range event.UpdatedSchemas {
			delete(s.SchemaIDToTableIDs[schemaIDChange.OldSchemaID], schemaIDChange.TableID)
			if len(s.SchemaIDToTableIDs[schemaIDChange.OldSchemaID]) == 0 {
				delete(s.SchemaIDToTableIDs, schemaIDChange.OldSchemaID)
			}

			if s.SchemaIDToTableIDs[schemaIDChange.NewSchemaID] == nil {
				s.SchemaIDToTableIDs[schemaIDChange.NewSchemaID] = make(map[int64]interface{})
			}
			s.SchemaIDToTableIDs[schemaIDChange.NewSchemaID][schemaIDChange.TableID] = nil
			s.TableIDToSchemaID[schemaIDChange.TableID] = schemaIDChange.NewSchemaID
		}
	}
}

func (s *TableIDStore) GetNormalTableIdsByDB(schemaID int64) []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	tables := s.SchemaIDToTableIDs[schemaID]
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
	tableIds = append(tableIds, commonType.DDLSpanTableID)
	return tableIds
}

func (s *TableIDStore) GetAllNormalTableIds() []int64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	tableIds := make([]int64, 0, len(s.TableIDToSchemaID))
	for tableID := range s.TableIDToSchemaID {
		tableIds = append(tableIds, tableID)
	}
	return tableIds
}

func (s *TableIDStore) GetAllTableIds() []int64 {
	tableIds := s.GetAllNormalTableIds()
	// Add the table id of the span of table trigger event dispatcher
	// Each influence-DB ddl must have table trigger event dispatcher's participation
	tableIds = append(tableIds, commonType.DDLSpanTableID)
	return tableIds
}
