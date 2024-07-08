package common

import (
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

// TxnEvent represents all events in the current txn
// It could be a DDL event, or multiple DML events, but can't be both.
// TODO: field 改成小写？
type TxnEvent struct {
	DDLEvent *DDLEvent
	Rows     []*RowChangedEvent
	StartTs  uint64
	CommitTs uint64
}

func (e *TxnEvent) GetDDLQuery() string {
	if e.DDLEvent == nil {
		return "" // 要报错的
	}
	return e.DDLEvent.Job.Query
}

func (e *TxnEvent) GetDDLSchemaName() string {
	if e.DDLEvent == nil {
		return "" // 要报错的
	}
	return e.DDLEvent.Job.SchemaName
}

func (e *TxnEvent) GetDDLType() timodel.ActionType {
	return e.DDLEvent.Job.Type
}

func (e *TxnEvent) GetRows() []*RowChangedEvent {
	return e.Rows
}

// ColumnData represents a column value in row changed event
type ColumnData struct {
	// ColumnID may be just a mock id, because we don't store it in redo log.
	// So after restore from redo log, we need to give every a column a mock id.
	// The only guarantee is that the column id is unique in a RowChangedEvent
	ColumnID int64
	Value    interface{}

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int
}

type RowChangedEvent struct {
	PhysicalTableID int64

	// NOTICE: We probably store the logical ID inside TableInfo's TableName,
	// not the physical ID.
	// For normal table, there is only one ID, which is the physical ID.
	// AKA TIDB_TABLE_ID.
	// For partitioned table, there are two kinds of ID:
	// 1. TIDB_PARTITION_ID is the physical ID of the partition.
	// 2. TIDB_TABLE_ID is the logical ID of the table.
	// In general, we always use the physical ID to represent a table, but we
	// record the logical ID from the DDL event(job.BinlogInfo.TableInfo).
	// So be careful when using the TableInfo.
	TableInfo *TableInfo

	// Columns    []*ColumnData
	// PreColumns []*ColumnData

	Columns    []*Column
	PreColumns []*Column

	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs uint64
}

// Column represents a column value and its schema info
type Column struct {
	Name      string         `msg:"name"`
	Type      byte           `msg:"type"`
	Charset   string         `msg:"charset"`
	Collation string         `msg:"collation"`
	Flag      ColumnFlagType `msg:"-"`
	Value     interface{}    `msg:"-"`
	Default   interface{}    `msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `msg:"-"`
}

func columnData2Column(col *ColumnData, tableInfo *TableInfo) *Column {
	colID := col.ColumnID
	offset, ok := tableInfo.columnsOffset[colID]
	if !ok {
		log.Panic("invalid column id",
			zap.Int64("columnID", colID),
			zap.Any("tableInfo", tableInfo))
	}
	colInfo := tableInfo.Columns[offset]
	return &Column{
		Name:      colInfo.Name.O,
		Type:      colInfo.GetType(),
		Charset:   colInfo.GetCharset(),
		Collation: colInfo.GetCollate(),
		Flag:      *tableInfo.ColumnsFlag[colID],
		Value:     col.Value,
		Default:   GetColumnDefaultValue(colInfo),
	}
}

// func columnDatas2Columns(cols []*ColumnData, tableInfo *TableInfo) []*Column {
// 	if cols == nil {
// 		return nil
// 	}
// 	columns := make([]*Column, len(cols))
// 	for i, colData := range cols {
// 		if colData == nil {
// 			log.Warn("meet nil column data, should not happened in production env",
// 				zap.Any("cols", cols),
// 				zap.Any("tableInfo", tableInfo))
// 			continue
// 		}
// 		columns[i] = columnData2Column(colData, tableInfo)
// 	}
// 	return columns
// }

// GetColumns returns the columns of the event
func (r *RowChangedEvent) GetColumns() []*Column {
	// return columnDatas2Columns(r.Columns, r.TableInfo)
	return r.Columns
}

// GetPreColumns returns the pre columns of the event
func (r *RowChangedEvent) GetPreColumns() []*Column {
	return r.PreColumns
	//return columnDatas2Columns(r.PreColumns, r.TableInfo)
}

// // Columns2ColumnDatas convert `Column`s to `ColumnData`s
// func Columns2ColumnDatas(cols []*Column, tableInfo *TableInfo) []*ColumnData {
// 	if cols == nil {
// 		return nil
// 	}
// 	columns := make([]*ColumnData, len(cols))
// 	for i, col := range cols {
// 		if col == nil {
// 			continue
// 		}
// 		colID := tableInfo.ForceGetColumnIDByName(col.Name)
// 		columns[i] = &ColumnData{
// 			ColumnID: colID,
// 			Value:    col.Value,
// 		}
// 	}
// 	return columns
// }
