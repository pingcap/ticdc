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

// NeedTableNameStoreAndCheckpointTs returns true when downstream components must maintain table names info
// for checkpoint propagation.
// Non-MySQL sinks always require table names, while MySQL sinks
// only need them when active-active replication is enabled.
func NeedTableNameStoreAndCheckpointTs(isMysqlCompatibleBackend bool, enableActiveActive bool) bool {
	if !isMysqlCompatibleBackend {
		return true
	}
	return enableActiveActive
}

type tableSchemaStoreRequirements struct {
	needTableIDs   bool
	updateTableIDs bool
	needTableNames bool
}

// tableSchemaStoreRequirements documents which metadata classes TableSchemaStore must
// track for each sink type and mode.
//
// Table IDs:
//   - MySQL-class sink: used by the DDL-ts writer to expand InfluenceTypeDB/All into table IDs.
//     See pkg/sink/mysql/mysql_writer_for_ddl_ts.go.
//   - Redo pipeline: attached to redo DDL logs for InfluenceTypeDB/All expansion, and consumed
//     by redo applier to expand dropped tables for cleanup/skip decisions.
//     See pkg/common/event/redo.go and pkg/applier/redo.go.
//   - Kafka/Pulsar (simple protocol, send-all-bootstrap-at-start): used by the table trigger dispatcher
//     to list all current tables and emit bootstrap schema messages at changefeed start.
//     See downstreamadapter/dispatcher/event_dispatcher.go.
//
// Table names:
//   - Kafka/Pulsar: used to route checkpointTs to active topics (and include dropped names for tombstones).
//     See downstreamadapter/sink/kafka/sink.go, downstreamadapter/sink/pulsar/sink.go,
//     downstreamadapter/sink/eventrouter/event_router.go.
//   - MySQL-class sink with enable-active-active: used by ProgressTableWriter to build progress rows.
//     See pkg/sink/mysql/progress_table_writer.go.
//
// Note: Some sink types may not use TableSchemaStore metadata directly, but the store is still
// initialized on the table trigger dispatcher and receives DDL updates.
func newTableSchemaStoreRequirements(
	sinkType commonType.SinkType, enableActiveActive bool,
) tableSchemaStoreRequirements {
	switch sinkType {
	case commonType.MysqlSinkType:
		return tableSchemaStoreRequirements{
			needTableIDs:   true,
			updateTableIDs: true,
			needTableNames: enableActiveActive,
		}
	case commonType.RedoSinkType:
		// Redo log only needs table IDs for schema/table expansion in non-normal DDLs.
		// Table names are not used for redo.
		return tableSchemaStoreRequirements{
			needTableIDs:   true,
			updateTableIDs: true,
			needTableNames: false,
		}
	case commonType.KafkaSinkType, commonType.PulsarSinkType:
		// Kafka/Pulsar use table names for checkpoint routing, and may need table IDs for
		// bootstrap messages at changefeed start (simple protocol).
		// Table ID updates are not needed after initialization.
		return tableSchemaStoreRequirements{
			needTableIDs:   true,
			updateTableIDs: false,
			needTableNames: true,
		}
	case commonType.CloudStorageSinkType:
		// Cloud storage sink drains by dispatcher route in PassBlockEvent and does not
		// require table-id expansion from TableSchemaStore.
		return tableSchemaStoreRequirements{
			needTableIDs:   false,
			updateTableIDs: false,
			needTableNames: false,
		}
	case commonType.BlackHoleSinkType:
		return tableSchemaStoreRequirements{
			needTableIDs:   false,
			updateTableIDs: false,
			needTableNames: false,
		}
	default:
		log.Panic("Unknown sink type for TableSchemaStore requirements", zap.Any("sinkType", sinkType))
		return tableSchemaStoreRequirements{}
	}
}

// TableSchemaStore is store some schema info for dispatchers.
// It is responsible for
// 1. [By TableNameStore]provide all the table name of the specified ts(only support incremental ts), mainly for generate topic for kafka sink when send watermark.
// 2. [By TableIDStore]provide the tableids based on schema-id or all tableids when send ddl ts in mysql sink.
//
// TableSchemaStore only exists in the table trigger event dispatcher, and the same instance's sink of this changefeed,
// which means each changefeed only has one TableSchemaStore.
type TableSchemaStore struct {
	sinkType           commonType.SinkType          `msg:"-"`
	enableActiveActive bool                         `msg:"-"`
	req                tableSchemaStoreRequirements `msg:"-"`
	tableNameStore     *TableNameStore              `msg:"-"`
	// TableIDStore will be used in redo ddl event to record all block tables id,
	// so it has to support Marshal/Unmarshal
	TableIDStore *TableIDStore `msg:"table_id_store"`
}

func NewTableSchemaStore(schemaInfo []*heartbeatpb.SchemaInfo, sinkType commonType.SinkType, enableActiveActive bool) *TableSchemaStore {
	req := newTableSchemaStoreRequirements(sinkType, enableActiveActive)
	tableSchemaStore := &TableSchemaStore{
		sinkType:           sinkType,
		enableActiveActive: enableActiveActive,
		req:                req,
	}
	if req.needTableIDs {
		tableSchemaStore.TableIDStore = &TableIDStore{
			SchemaIDToTableIDs: make(map[int64]map[int64]interface{}),
			TableIDToSchemaID:  make(map[int64]int64),
		}
	}
	if req.needTableNames {
		tableSchemaStore.tableNameStore = &TableNameStore{
			existingTables:         make(map[string]map[string]*SchemaTableName),
			latestTableNameChanges: &LatestTableNameChanges{m: make(map[uint64]*TableNameChange)},
			sinkType:               sinkType,
		}
	}
	for _, schema := range schemaInfo {
		schemaID := schema.SchemaID
		for _, table := range schema.Tables {
			tableID := table.TableID
			if tableSchemaStore.TableIDStore != nil {
				tableSchemaStore.TableIDStore.Add(schemaID, tableID)
			}
			if tableSchemaStore.tableNameStore != nil {
				tableSchemaStore.tableNameStore.Add(schema.SchemaName, table.TableName)
			}
		}
	}
	return tableSchemaStore
}

func (s *TableSchemaStore) Clear() {
	s = nil
}

func (s *TableSchemaStore) AddEvent(event *DDLEvent) {
	if !s.initialized() {
		return
	}
	// Routing is decided at initialization time. If a store is nil, this instance
	// does not track that class of metadata.
	if s.TableIDStore != nil && s.req.updateTableIDs {
		s.TableIDStore.AddEvent(event)
	}
	if s.tableNameStore != nil {
		s.tableNameStore.AddEvent(event)
	}
}

func (s *TableSchemaStore) initialized() bool {
	if s == nil {
		log.Panic("TableSchemaStore is not initialized", zap.Any("tableSchemaStore", s))
		return false
	}
	if s.req.needTableIDs && s.TableIDStore == nil {
		log.Panic("TableSchemaStore is missing TableIDStore", zap.Any("tableSchemaStore", s))
		return false
	}
	if s.req.needTableNames && s.tableNameStore == nil {
		log.Panic("TableSchemaStore is missing TableNameStore", zap.Any("tableSchemaStore", s))
		return false
	}
	return true
}

func (s *TableSchemaStore) GetTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() || s.TableIDStore == nil {
		return nil
	}
	return s.TableIDStore.GetTableIdsByDB(schemaID)
}

// GetNormalTableIdsByDB will not return table id = 0 , this is the only different between GetTableIdsByDB and GetNormalTableIdsByDB
func (s *TableSchemaStore) GetNormalTableIdsByDB(schemaID int64) []int64 {
	if !s.initialized() || s.TableIDStore == nil {
		return nil
	}
	return s.TableIDStore.GetNormalTableIdsByDB(schemaID)
}

func (s *TableSchemaStore) GetAllTableIds() []int64 {
	if !s.initialized() || s.TableIDStore == nil {
		return nil
	}
	return s.TableIDStore.GetAllTableIds()
}

// GetAllNormalTableIds will not return table id = 0 , this is the only different between GetAllNormalTableIds and GetAllTableIds
func (s *TableSchemaStore) GetAllNormalTableIds() []int64 {
	if !s.initialized() || s.TableIDStore == nil {
		return nil
	}
	return s.TableIDStore.GetAllNormalTableIds()
}

// GetAllTableNames will be called in two cases:
// 1. for non-mysqlSink case, when maintainer send message to ask dispatcher to write checkpointTs to downstream. In this case, tableNames should also include the dropped tables, needDroppedTableName will be true.
// 2. for mysqlSink with enableActiveActive case, when maintainer send message to ask dispatcher to write checkpointTs to downstream, needDroppedTableName will be false.
// the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableSchemaStore) GetAllTableNames(ts uint64, needDroppedTableName bool) []*SchemaTableName {
	if !s.initialized() || s.tableNameStore == nil {
		return nil
	}
	return s.tableNameStore.GetAllTableNames(ts, needDroppedTableName)
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
	sinkType               commonType.SinkType
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

// GetAllTableNames will be called in two cases:
// 1. for non-mysqlSink case, when maintainer send message to ask dispatcher to write checkpointTs to downstream. In this case, tableNames should also include the dropped tables, needDroppedTableName will be true.
// 2. for mysqlSink with enableActiveActive case, when maintainer send message to ask dispatcher to write checkpointTs to downstream, needDroppedTableName will be false.
// the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableNameStore) GetAllTableNames(ts uint64, needDroppedTableName bool) []*SchemaTableName {
	// In non-mysqlSink cases, we have to send checkpointTs to the drop schema/tables so that consumer can know the schema/table is dropped.
	tableNames := make([]*SchemaTableName, 0)
	s.latestTableNameChanges.mutex.Lock()
	if len(s.latestTableNameChanges.m) > 0 {
		// update the existingTables with the latest table changes <= ts
		for commitTs, tableNameChange := range s.latestTableNameChanges.m {
			if commitTs <= ts {
				if tableNameChange.DropDatabaseName != "" {
					if needDroppedTableName {
						tableNames = append(tableNames, &SchemaTableName{
							SchemaName: tableNameChange.DropDatabaseName,
						})
					}
					delete(s.existingTables, tableNameChange.DropDatabaseName)
				} else {
					for _, addName := range tableNameChange.AddName {
						if s.existingTables[addName.SchemaName] == nil {
							s.existingTables[addName.SchemaName] = make(map[string]*SchemaTableName, 0)
						}
						s.existingTables[addName.SchemaName][addName.TableName] = &addName
					}
					for _, dropName := range tableNameChange.DropName {
						if needDroppedTableName {
							tableNames = append(tableNames, &dropName)
						}
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
