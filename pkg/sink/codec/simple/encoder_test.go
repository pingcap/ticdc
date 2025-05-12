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

package simple

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sort"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	mock_simple "github.com/pingcap/ticdc/pkg/sink/codec/simple/mock"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestEncodeCheckpoint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format

		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			checkpoint := 446266400629063682
			m, err := enc.EncodeCheckpointEvent(uint64(checkpoint))
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeResolved, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			ts := dec.NextResolvedEvent()
			require.Equal(t, uint64(checkpoint), ts)
		}
	}
}

func TestEncodeDMLEnableChecksum(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = config.CheckLevelCorrectness
	createTableDDL, _, updateEvent, _ := common.NewLargeEvent4Test(t)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			m, err := enc.EncodeDDLEvent(createTableDDL)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)

			decodedDDL := dec.NextDDLEvent()
			originCols := createTableDDL.TableInfo.GetColumns()

			for _, expected := range originCols {
				actualID := decodedDDL.TableInfo.ForceGetColumnIDByName(expected.Name.O)
				actual := decodedDDL.TableInfo.ForceGetColumnInfo(actualID)
				require.Equal(t, expected.GetFlag(), actual.GetFlag())
			}

			err = enc.AppendRowChangedEvent(ctx, "", updateEvent)
			require.NoError(t, err)

			messages := enc.Build()
			require.Len(t, messages, 1)

			dec.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)

			// decodedRow:= decoder.NextDMLEvent()
			// require.NoError(t, err)
			// require.Equal(t, updateEvent.Checksum.Current, decodedRow.Checksum.Current)
			// require.Equal(t, updateEvent.Checksum.Previous, decodedRow.Checksum.Previous)
			// require.False(t, decodedRow.Checksum.Corrupted)
		}
	}

	// tamper the checksum, to test error case
	// updateEvent.Checksum.Current = 1
	// updateEvent.Checksum.Previous = 2

	enc, err := NewEncoder(ctx, codecConfig)
	require.NoError(t, err)

	rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)
	dec, ok := rowEventDecoder.(*Decoder)
	require.True(t, ok)

	m, err := enc.EncodeDDLEvent(createTableDDL)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	_ = dec.NextDDLEvent()

	err = enc.AppendRowChangedEvent(ctx, "", updateEvent)
	require.NoError(t, err)

	messages := enc.Build()
	require.Len(t, messages, 1)

	dec.AddKeyValue(messages[0].Key, messages[0].Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	// decodedRow:= decoder.NextDMLEvent()
	// require.Error(t, err)
	// require.Nil(t, decodedRow)
}

// FIXME: support partition table
// func TestE2EPartitionTable(t *testing.T) {
// 	helper := commonEvent.NewEventTestHelper(t)
// 	defer helper.Close()

// 	helper.Tk().MustExec("use test")

// 	createPartitionTableJob := helper.DDL2Job(`create table test.t(a int primary key, b int) partition by range (a) (
// 		partition p0 values less than (10),
// 		partition p1 values less than (20),
// 		partition p2 values less than MAXVALUE)`)
// 	tableInfo := helper.GetTableInfo(createPartitionTableJob)
// 	createPartitionTableDDL := &commonEvent.DDLEvent{
// 		SchemaID:   createPartitionTableJob.SchemaID,
// 		TableID:    createPartitionTableJob.TableID,
// 		Query:      createPartitionTableJob.Query,
// 		TableInfo:  tableInfo,
// 		FinishedTs: createPartitionTableJob.BinlogInfo.FinishedTS,
// 	}

// 	insertEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 1)`)
// 	require.NotNil(t, insertEvent)

// 	insertEvent1 := helper.DML2Event("test", "t", `insert into test.t values (11, 11)`)
// 	require.NotNil(t, insertEvent1)

// 	insertEvent2 := helper.DML2Event("test", "t", `insert into test.t values (21, 21)`)
// 	require.NotNil(t, insertEvent2)

// 	events := []*commonEvent.DMLEvent{insertEvent, insertEvent1, insertEvent2}

// 	ctx := context.Background()
// 	codecConfig := common.NewConfig(config.ProtocolSimple)

// 	for _, format := range []common.EncodingFormatType{
// 		common.EncodingFormatAvro,
// 		common.EncodingFormatJSON,
// 	} {
// 		codecConfig.EncodingFormat = format
// 		enc, err := NewEncoder(ctx, codecConfig)
// 		require.NoError(t, err)
// 		Decoder, err := NewDecoder(ctx, codecConfig, nil)
// 		require.NoError(t, err)

// 		message, err := enc.EncodeDDLEvent(createPartitionTableDDL)
// 		require.NoError(t, err)

// 		err = Decoder.AddKeyValue(message.Key, message.Value)
// 		require.NoError(t, err)
// 		tp, hasNext, err := Decoder.HasNext()
// 		require.NoError(t, err)
// 		require.True(t, hasNext)
// 		require.Equal(t, common.MessageTypeDDL, tp)

// 		decodedDDL, err := Decoder.NextDDLEvent()
// 		require.NoError(t, err)
// 		require.NotNil(t, decodedDDL)

// 		for _, event := range events {
// 			row, ok := event.GetNextRow()
// 			require.True(t, ok)

// 			err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
// 				TableInfo:      tableInfo,
// 				Event:          row,
// 				CommitTs:       event.CommitTs,
// 				ColumnSelector: columnselector.NewDefaultColumnSelector(),
// 			})
// 			require.NoError(t, err)
// 			message := enc.Build()[0]

// 			err = Decoder.AddKeyValue(message.Key, message.Value)
// 			require.NoError(t, err)
// 			tp, hasNext, err := Decoder.HasNext()
// 			require.NoError(t, err)
// 			require.True(t, hasNext)
// 			require.Equal(t, common.MessageTypeRow, tp)

// 			decodedEvent, err := Decoder.NextDMLEvent()
// 			require.NoError(t, err)
// 			// table id should be set to the partition table id, the PhysicalTableID
// 			require.Equal(t, decodedEvent.GetTableID(), event.GetTableID())
// 		}
// 	}
// }

func TestEncodeDDLSequence(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	dropDBEvent := helper.DDL2Event(`DROP DATABASE IF EXISTS test`)
	createDBDDLEvent := helper.DDL2Event(`CREATE DATABASE IF NOT EXISTS test`)
	helper.Tk().MustExec("use test")

	createTableDDLEvent := helper.DDL2Event("CREATE TABLE `TBL1` (`id` INT PRIMARY KEY AUTO_INCREMENT,`value` VARCHAR(255),`payload` VARCHAR(2000),`a` INT)")

	addColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ADD COLUMN `nn` INT")

	dropColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` DROP COLUMN `nn`")

	changeColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` CHANGE COLUMN `value` `value2` VARCHAR(512)")

	modifyColumnDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `value2` VARCHAR(512) FIRST")

	setDefaultDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ALTER COLUMN `payload` SET DEFAULT _UTF8MB4'a'")

	dropDefaultDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` ALTER COLUMN `payload` DROP DEFAULT")

	autoIncrementDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` AUTO_INCREMENT = 5")

	modifyColumnNullDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `a` INT NULL")

	modifyColumnNotNullDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` MODIFY COLUMN `a` INT NOT NULL")

	addIndexDDLEvent := helper.DDL2Event("CREATE INDEX `idx_a` ON `TBL1` (`a`)")

	renameIndexDDLEvent := helper.DDL2Event("ALTER TABLE `TBL1` RENAME INDEX `idx_a` TO `new_idx_a`")

	indexVisibilityDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 ALTER INDEX `new_idx_a` INVISIBLE")

	dropIndexDDLEvent := helper.DDL2Event("DROP INDEX `new_idx_a` ON `TBL1`")

	truncateTableDDLEvent := helper.DDL2Event("TRUNCATE TABLE TBL1")

	multiSchemaChangeDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 ADD COLUMN `new_col` INT, ADD INDEX `idx_new_col` (`a`)")

	multiSchemaChangeDropDDLEvent := helper.DDL2Event("ALTER TABLE TBL1 DROP COLUMN `new_col`, DROP INDEX `idx_new_col`")

	renameTableDDLEvent := helper.DDL2Event("RENAME TABLE TBL1 TO TBL2")

	helper.Tk().MustExec("set @@tidb_allow_remove_auto_inc = 1")
	renameColumnDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 CHANGE COLUMN `id` `id2` INT")

	partitionTableDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 PARTITION BY RANGE (id2) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")

	addPartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")

	dropPartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 DROP PARTITION p2")

	truncatePartitionDDLevent := helper.DDL2Event("ALTER TABLE TBL2 TRUNCATE PARTITION p1")

	reorganizePartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 REORGANIZE PARTITION p1 INTO (PARTITION p3 VALUES LESS THAN (40))")

	removePartitionDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 REMOVE PARTITIONING")

	alterCharsetCollateDDLEvent := helper.DDL2Event("ALTER TABLE TBL2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin")

	dropTableDDLEvent := helper.DDL2Event("DROP TABLE TBL2")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			m, err := enc.EncodeDDLEvent(dropDBEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.Equal(t, DDLTypeQuery, dec.msg.Type)

			_ = dec.NextDDLEvent()

			m, err = enc.EncodeDDLEvent(createDBDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.Equal(t, DDLTypeQuery, dec.msg.Type)

			_ = dec.NextDDLEvent()

			m, err = enc.EncodeDDLEvent(createTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.Equal(t, DDLTypeCreate, dec.msg.Type)

			event := dec.NextDDLEvent()
			require.Len(t, event.TableInfo.GetIndices(), 1)
			require.Len(t, event.TableInfo.GetColumns(), 4)

			m, err = enc.EncodeDDLEvent(addColumnDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Len(t, event.TableInfo.GetIndices(), 1)
			require.Len(t, event.TableInfo.GetColumns(), 5)

			m, err = enc.EncodeDDLEvent(dropColumnDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Len(t, event.TableInfo.GetIndices(), 1)
			require.Len(t, event.TableInfo.GetColumns(), 4)

			m, err = enc.EncodeDDLEvent(changeColumnDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Len(t, event.TableInfo.GetIndices(), 1)
			require.Len(t, event.TableInfo.GetColumns(), 4)

			m, err = enc.EncodeDDLEvent(modifyColumnDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()), string(format), compressionType)
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(setDefaultDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
			for _, col := range event.TableInfo.GetColumns() {
				if col.Name.O == "payload" {
					require.Equal(t, "a", col.DefaultValue)
				}
			}

			m, err = enc.EncodeDDLEvent(dropDefaultDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
			for _, col := range event.TableInfo.GetColumns() {
				if col.Name.O == "payload" {
					require.Nil(t, col.DefaultValue)
				}
			}

			m, err = enc.EncodeDDLEvent(autoIncrementDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.NoError(t, err)
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(modifyColumnNullDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
			for _, col := range event.TableInfo.GetColumns() {
				if col.Name.O == "a" {
					require.True(t, !mysql.HasNotNullFlag(col.GetFlag()))
				}
			}

			m, err = enc.EncodeDDLEvent(modifyColumnNotNullDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
			for _, col := range event.TableInfo.GetColumns() {
				if col.Name.O == "a" {
					require.True(t, mysql.HasNotNullFlag(col.GetFlag()))
				}
			}

			m, err = enc.EncodeDDLEvent(addIndexDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeCIndex, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 2, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(renameIndexDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 2, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
			hasNewIndex := false
			noOldIndex := true
			for _, index := range event.TableInfo.GetIndices() {
				if index.Name.O == "new_idx_a" {
					hasNewIndex = true
				}
				if index.Name.O == "idx_a" {
					noOldIndex = false
				}
			}
			require.True(t, hasNewIndex)
			require.True(t, noOldIndex)

			m, err = enc.EncodeDDLEvent(indexVisibilityDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 2, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(dropIndexDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeDIndex, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(truncateTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeTruncate, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(multiSchemaChangeDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 2, len(event.TableInfo.GetIndices()))
			require.Equal(t, 5, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(multiSchemaChangeDropDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(renameTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeRename, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(renameColumnDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(partitionTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(addPartitionDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(dropPartitionDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(truncatePartitionDDLevent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(reorganizePartitionDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(removePartitionDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(alterCharsetCollateDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeAlter, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))

			m, err = enc.EncodeDDLEvent(dropTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			_, _ = dec.HasNext()
			require.Equal(t, DDLTypeErase, dec.msg.Type)

			event = dec.NextDDLEvent()
			require.Equal(t, 1, len(event.TableInfo.GetIndices()))
			require.Equal(t, 4, len(event.TableInfo.GetColumns()))
		}
	}
}

func TestEncodeDDLEvent(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = config.CheckLevelCorrectness
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	createTableSQL := `create table test.t(id int primary key, name varchar(255) not null, gender enum('male', 'female'), email varchar(255) null, key idx_name_email(name, email))`
	createTableDDLEvent := helper.DDL2Event(createTableSQL)

	insertEvent := helper.DML2Event("test", "t", `insert into test.t values (1, "jack", "male", "jack@abc.com")`)

	renameTableDDLEvent := helper.DDL2Event(`rename table test.t to test.abc`)

	insertEvent2 := helper.DML2Event("test", "abc", `insert into test.abc values (2, "anna", "female", "anna@abc.com")`)
	helper.Tk().MustExec("drop table test.abc")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			insertEvent.Rewind()
			insertEvent2.Rewind()
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			m, err := enc.EncodeDDLEvent(createTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)
			require.True(t, dec.msg.TableSchema.Indexes[0].Nullable)

			columnSchemas := dec.msg.TableSchema.Columns
			sortedColumns := make([]*timodel.ColumnInfo, len(createTableDDLEvent.TableInfo.GetColumns()))
			copy(sortedColumns, createTableDDLEvent.TableInfo.GetColumns())
			sort.Slice(sortedColumns, func(i, j int) bool {
				return sortedColumns[i].ID < sortedColumns[j].ID
			})

			for idx, column := range sortedColumns {
				require.Equal(t, column.Name.O, columnSchemas[idx].Name)
			}

			event := dec.NextDDLEvent()

			require.NoError(t, err)
			require.Equal(t, createTableDDLEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
			require.Equal(t, createTableDDLEvent.GetCommitTs(), event.GetCommitTs())
			require.Equal(t, createTableDDLEvent.Query, event.Query)
			require.Equal(t, len(createTableDDLEvent.TableInfo.GetColumns()), len(event.TableInfo.GetColumns()))
			require.Equal(t, 2, len(event.TableInfo.GetIndices()))
			// require.Nil(t, event.MultipleTableInfos[0])

			item := dec.memo.Read(createTableDDLEvent.TableInfo.TableName.Schema,
				createTableDDLEvent.TableInfo.TableName.Table, createTableDDLEvent.TableInfo.UpdateTS())
			require.NotNil(t, item)

			row, ok := insertEvent.GetNextRow()
			require.True(t, ok)

			err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
				TableInfo:      insertEvent.TableInfo,
				CommitTs:       insertEvent.CommitTs,
				Event:          row,
				ColumnSelector: columnselector.NewDefaultColumnSelector(),
			})
			require.NoError(t, err)

			messages := enc.Build()
			require.Len(t, messages, 1)

			dec.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			decodedRow := dec.NextDMLEvent()
			require.Equal(t, decodedRow.CommitTs, insertEvent.GetCommitTs())
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), insertEvent.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), insertEvent.TableInfo.GetTableName())

			m, err = enc.EncodeDDLEvent(renameTableDDLEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			event = dec.NextDDLEvent()
			require.Equal(t, renameTableDDLEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
			require.Equal(t, renameTableDDLEvent.GetCommitTs(), event.GetCommitTs())
			require.Equal(t, renameTableDDLEvent.Query, event.Query)
			require.Equal(t, len(renameTableDDLEvent.TableInfo.GetColumns()), len(event.TableInfo.GetColumns()))
			require.Equal(t, len(renameTableDDLEvent.TableInfo.GetIndices())+1, len(event.TableInfo.GetIndices()))
			// require.NotNil(t, event.PreTableInfo)
			require.NotNil(t, event.MultipleTableInfos)

			item = dec.memo.Read(renameTableDDLEvent.TableInfo.TableName.Schema,
				renameTableDDLEvent.TableInfo.TableName.Table, renameTableDDLEvent.TableInfo.UpdateTS())
			require.NotNil(t, item)

			row, ok = insertEvent2.GetNextRow()
			require.True(t, ok)

			err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
				TableInfo:      insertEvent2.TableInfo,
				CommitTs:       insertEvent2.GetCommitTs(),
				Event:          row,
				ColumnSelector: columnselector.NewDefaultColumnSelector(),
			})
			require.NoError(t, err)

			messages = enc.Build()
			require.Len(t, messages, 1)

			dec.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			decodedRow = dec.NextDMLEvent()
			require.Equal(t, insertEvent2.GetCommitTs(), decodedRow.GetCommitTs())
			require.Equal(t, insertEvent2.TableInfo.GetSchemaName(), decodedRow.TableInfo.GetSchemaName())
			require.Equal(t, insertEvent2.TableInfo.GetTableName(), decodedRow.TableInfo.GetTableName())
		}
	}
}

func TestColumnFlags(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	createTableDDL := `create table test.t(
		a bigint(20) unsigned not null,
		b bigint(20) default null,
		c varbinary(767) default null,
		d int(11) unsigned not null,
		e int(11) default null,
		primary key (a),
		key idx_c(c),
		key idx_b(b),
		unique key idx_de(d, e))`
	createTableDDLEvent := helper.DDL2Event(createTableDDL)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		enc, err := NewEncoder(ctx, codecConfig)
		require.NoError(t, err)

		m, err := enc.EncodeDDLEvent(createTableDDLEvent)
		require.NoError(t, err)

		rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)
		dec, ok := rowEventDecoder.(*Decoder)
		require.True(t, ok)

		dec.AddKeyValue(m.Key, m.Value)

		messageType, hasNext := dec.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeDDL, messageType)

		decodedDDLEvent := dec.NextDDLEvent()
		require.NoError(t, err)

		originCols := createTableDDLEvent.TableInfo.GetColumns()

		for _, expected := range originCols {
			actualID := decodedDDLEvent.TableInfo.ForceGetColumnIDByName(expected.Name.O)
			actual := decodedDDLEvent.TableInfo.ForceGetColumnInfo(actualID)
			// NoDefaultValueFlag is not set when decoding message
			expected.DelFlag(mysql.NoDefaultValueFlag)
			require.Equal(t, expected.GetFlag(), actual.GetFlag())
		}
	}
}

func TestEncodeIntegerTypes(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = config.CheckLevelCorrectness
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	createTableDDL := `create table test.t(
		id int primary key auto_increment,
 		a tinyint, b tinyint unsigned,
 		c smallint, d smallint unsigned,
 		e mediumint, f mediumint unsigned,
 		g int, h int unsigned,
 		i bigint, j bigint unsigned)`
	job := helper.DDL2Job(createTableDDL)
	ddlEvent := &commonEvent.DDLEvent{
		SchemaID:   job.SchemaID,
		TableID:    job.TableID,
		Query:      job.Query,
		TableInfo:  helper.GetTableInfo(job),
		FinishedTs: job.BinlogInfo.FinishedTS,
	}

	sql := `insert into test.t values(
		1,
		-128, 0,
		-32768, 0,
		-8388608, 0,
		-2147483648, 0,
		-9223372036854775808, 0)`
	minValues := helper.DML2Event("test", "t", sql)

	sql = `insert into test.t values (
		2,
 		127, 255,
 		32767, 65535,
 		8388607, 16777215,
 		2147483647, 4294967295,
 		9223372036854775807, 18446744073709551615)`
	maxValues := helper.DML2Event("test", "t", sql)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.EnableRowChecksum = true
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		minValues.Rewind()
		maxValues.Rewind()
		codecConfig.EncodingFormat = format
		enc, err := NewEncoder(ctx, codecConfig)
		require.NoError(t, err)

		m, err := enc.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)
		dec, ok := rowEventDecoder.(*Decoder)
		require.True(t, ok)

		dec.AddKeyValue(m.Key, m.Value)

		messageType, hasNext := dec.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeDDL, messageType)

		_ = dec.NextDDLEvent()

		for _, event := range []*commonEvent.DMLEvent{
			minValues,
			maxValues,
		} {
			rowChange, ok := event.GetNextRow()
			require.True(t, ok)
			e := &commonEvent.RowEvent{
				PhysicalTableID: event.PhysicalTableID,
				Event:           rowChange,
				CommitTs:        event.CommitTs,
				TableInfo:       event.TableInfo,
				ColumnSelector:  columnselector.NewDefaultColumnSelector(),
			}
			err = enc.AppendRowChangedEvent(ctx, "", e)
			require.NoError(t, err)

			messages := enc.Build()
			dec.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)

			decodedRow := dec.NextDMLEvent()
			require.Equal(t, decodedRow.CommitTs, event.GetCommitTs())

			decoded, ok := decodedRow.GetNextRow()
			require.True(t, ok)

			common.CompareRow(t, rowChange, event.TableInfo, decoded, decodedRow.TableInfo)
		}
	}
}

func TestEncoderOtherTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
			a int primary key auto_increment,
			b enum('a', 'b', 'c'),
			c set('a', 'b', 'c'),
			d bit(64),
			e json)`
	ddlEvent := helper.DDL2Event(sql)

	sql = `insert into test.t() values (1, 'a', 'a,b', b'1000001', '{
		  "key1": "value1",
		  "key2": "value2"
		}');`
	event := helper.DML2Event("test", "t", sql)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		event.Rewind()
		codecConfig.EncodingFormat = format
		enc, err := NewEncoder(ctx, codecConfig)
		require.NoError(t, err)

		m, err := enc.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)
		dec, ok := rowEventDecoder.(*Decoder)
		require.True(t, ok)

		dec.AddKeyValue(m.Key, m.Value)

		messageType, hasNext := dec.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeDDL, messageType)

		_ = dec.NextDDLEvent()

		row, ok := event.GetNextRow()
		require.True(t, ok)
		err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      event.TableInfo,
			Event:          row,
			CommitTs:       event.CommitTs,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
		require.NoError(t, err)

		messages := enc.Build()
		require.Len(t, messages, 1)

		dec.AddKeyValue(messages[0].Key, messages[0].Value)

		messageType, hasNext = dec.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decodedRow := dec.NextDMLEvent()
		decoded, ok := decodedRow.GetNextRow()
		require.True(t, ok)

		common.CompareRow(t, row, event.TableInfo, decoded, decodedRow.TableInfo)
	}
}

// FIXME: support partition table
// func TestE2EPartitionTableDMLBeforeDDL(t *testing.T) {
// 	helper := commonEvent.NewEventTestHelper(t)
// 	defer helper.Close()

// 	helper.Tk().MustExec("use test")

// 	createPartitionTableDDL := helper.DDL2Event(`create table test.t(a int primary key, b int) partition by range (a) (
// 		partition p0 values less than (10),
// 		partition p1 values less than (20),
// 		partition p2 values less than MAXVALUE)`)
// 	require.NotNil(t, createPartitionTableDDL)

// 	insertEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 1)`)
// 	require.NotNil(t, insertEvent)

// 	insertEvent1 := helper.DML2Event("test", "t", `insert into test.t values (11, 11)`)
// 	require.NotNil(t, insertEvent1)

// 	insertEvent2 := helper.DML2Event("test", "t", `insert into test.t values (21, 21)`)
// 	require.NotNil(t, insertEvent2)

// 	events := []*commonEvent.DMLEvent{insertEvent, insertEvent1, insertEvent2}

// 	ctx := context.Background()
// 	codecConfig := common.NewConfig(config.ProtocolSimple)

// 	for _, format := range []common.EncodingFormatType{
// 		common.EncodingFormatAvro,
// 		common.EncodingFormatJSON,
// 	} {
// 		codecConfig.EncodingFormat = format
// 		enc, err := NewEncoder(ctx, codecConfig)
// 		require.NoError(t, err)

// 		Decoder, err := NewDecoder(ctx, codecConfig, nil)
// 		require.NoError(t, err)

// 		codecConfig.EncodingFormat = format
// 		for _, event := range events {
// 			insertEvent.Rewind()
// 			insertEvent1.Rewind()
// 			insertEvent2.Rewind()
// 			row, ok := event.GetNextRow()
// 			require.True(t, ok)
// 			err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
// 				TableInfo:      event.TableInfo,
// 				Event:          row,
// 				CommitTs:       event.CommitTs,
// 				ColumnSelector: columnselector.NewDefaultColumnSelector(),
// 			})
// 			require.NoError(t, err)
// 			message := enc.Build()[0]

// 			err = Decoder.AddKeyValue(message.Key, message.Value)
// 			require.NoError(t, err)
// 			tp, hasNext, err := Decoder.HasNext()
// 			require.NoError(t, err)
// 			require.True(t, hasNext)
// 			require.Equal(t, common.MessageTypeRow, tp)

// 			decodedEvent, err := Decoder.NextDMLEvent()
// 			require.NoError(t, err)
// 			require.Nil(t, decodedEvent)
// 		}

// 		message, err := enc.EncodeDDLEvent(createPartitionTableDDL)
// 		require.NoError(t, err)

// 		err = Decoder.AddKeyValue(message.Key, message.Value)
// 		require.NoError(t, err)
// 		tp, hasNext, err := Decoder.HasNext()
// 		require.NoError(t, err)
// 		require.True(t, hasNext)
// 		require.Equal(t, common.MessageTypeDDL, tp)

// 		decodedDDL, err := Decoder.NextDDLEvent()
// 		require.NoError(t, err)
// 		require.NotNil(t, decodedDDL)

// 		cachedEvents := Decoder.GetCachedEvents()
// 		for idx, decodedRow := range cachedEvents {
// 			require.NotNil(t, decodedRow)
// 			require.NotNil(t, decodedRow.TableInfo)
// 			require.Equal(t, decodedRow.GetTableID(), events[idx].GetTableID())
// 		}
// 	}
// }

func TestEncodeDMLBeforeDDL(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b int)`
	ddlEvent := helper.DDL2Event(sql)

	sql = `insert into test.t values (1, 2)`
	event := helper.DML2Event("test", "t", sql)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)

	enc, err := NewEncoder(ctx, codecConfig)
	require.NoError(t, err)

	row, ok := event.GetNextRow()
	require.True(t, ok)
	err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
		TableInfo:      event.TableInfo,
		Event:          row,
		CommitTs:       event.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.NoError(t, err)

	messages := enc.Build()
	require.Len(t, messages, 1)

	rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)
	dec, ok := rowEventDecoder.(*Decoder)
	require.True(t, ok)

	dec.AddKeyValue(messages[0].Key, messages[0].Value)

	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decodedRow := dec.NextDMLEvent()
	require.Nil(t, decodedRow)

	m, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	ddlEvent = dec.NextDDLEvent()
	require.NotNil(t, ddlEvent)

	cachedEvents := dec.GetCachedEvents()
	for _, decodedRow = range cachedEvents {
		require.NotNil(t, decodedRow)
		require.NotNil(t, decodedRow.TableInfo)
		require.Equal(t, decodedRow.TableInfo.TableName.TableID, ddlEvent.TableInfo.TableName.TableID)
	}
}

func TestEncodeBootstrapEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
    	id int,
    	name varchar(255) not null,
    	age int,
    	email varchar(255) not null,
    	primary key(id, name),
    	key idx_name_email(name, email))`
	ddlEvent := helper.DDL2Event(sql)
	ddlEvent.IsBootstrap = true

	sql = `insert into test.t values (1, "jack", 23, "jack@abc.com")`
	dmlEvent := helper.DML2Event("test", "t", sql)

	helper.Tk().MustExec("drop table test.t")

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			dmlEvent.Rewind()
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType
			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			event := dec.NextDDLEvent()
			require.Equal(t, ddlEvent.TableInfo.TableName.TableID, event.TableInfo.TableName.TableID)
			// Bootstrap event doesn't have query
			require.Equal(t, "", event.Query)
			require.Equal(t, len(ddlEvent.TableInfo.GetColumns()), len(event.TableInfo.GetColumns()))
			require.Equal(t, len(ddlEvent.TableInfo.GetIndices()), len(event.TableInfo.GetIndices()))

			item := dec.memo.Read(ddlEvent.TableInfo.TableName.Schema,
				ddlEvent.TableInfo.TableName.Table, ddlEvent.TableInfo.UpdateTS())
			require.NotNil(t, item)

			row, ok := dmlEvent.GetNextRow()
			require.True(t, ok)
			err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
				TableInfo:      dmlEvent.TableInfo,
				Event:          row,
				CommitTs:       dmlEvent.CommitTs,
				ColumnSelector: columnselector.NewDefaultColumnSelector(),
			})
			require.NoError(t, err)

			messages := enc.Build()
			require.Len(t, messages, 1)

			dec.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext = dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)
			require.NotEqual(t, 0, dec.msg.BuildTs)

			decodedRow := dec.NextDMLEvent()
			decode, ok := decodedRow.GetNextRow()
			require.True(t, ok)
			require.Equal(t, decodedRow.CommitTs, dmlEvent.CommitTs)
			require.Equal(t, decodedRow.TableInfo.GetSchemaName(), dmlEvent.TableInfo.GetSchemaName())
			require.Equal(t, decodedRow.TableInfo.GetTableName(), dmlEvent.TableInfo.GetTableName())
			require.True(t, decode.PreRow.IsEmpty())
		}
	}
}

func TestEncodeLargeEventsNormal(t *testing.T) {
	ddlEvent, insertEvent, updateEvent, deleteEvent := common.NewLargeEvent4Test(t)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)

			obtainedDDL := dec.NextDDLEvent()
			require.NotNil(t, obtainedDDL)

			obtainedDefaultValues := make(map[string]interface{}, len(obtainedDDL.TableInfo.GetColumns()))
			for _, col := range obtainedDDL.TableInfo.GetColumns() {
				obtainedDefaultValues[col.Name.O] = col.GetDefaultValue()
				switch col.GetType() {
				case mysql.TypeFloat, mysql.TypeDouble:
					require.Equal(t, 0, col.GetDecimal())
				default:
				}
			}
			for _, col := range ddlEvent.TableInfo.GetColumns() {
				expected := col.GetDefaultValue()
				obtained := obtainedDefaultValues[col.Name.O]
				require.Equal(t, expected, obtained)
			}

			for _, event := range []*commonEvent.RowEvent{insertEvent, updateEvent, deleteEvent} {
				err = enc.AppendRowChangedEvent(ctx, "", event)
				require.NoError(t, err)

				messages := enc.Build()
				require.Len(t, messages, 1)

				dec.AddKeyValue(messages[0].Key, messages[0].Value)

				messageType, hasNext = dec.HasNext()
				require.True(t, hasNext)
				require.Equal(t, common.MessageTypeRow, messageType)

				if event.IsDelete() {
					require.Equal(t, dec.msg.Type, DMLTypeDelete)
				} else if event.IsUpdate() {
					require.Equal(t, dec.msg.Type, DMLTypeUpdate)
				} else {
					require.Equal(t, dec.msg.Type, DMLTypeInsert)
				}

				decodedRow := dec.NextDMLEvent()

				require.Equal(t, decodedRow.CommitTs, event.CommitTs)
				require.Equal(t, decodedRow.TableInfo.GetSchemaName(), event.TableInfo.GetSchemaName())
				require.Equal(t, decodedRow.TableInfo.GetTableName(), event.TableInfo.GetTableName())
				require.Equal(t, decodedRow.GetTableID(), event.GetTableID())

				decoded, ok := decodedRow.GetNextRow()
				require.True(t, ok)

				common.CompareRow(t, event.Event, event.TableInfo, decoded, decodedRow.TableInfo)
			}
		}
	}
}

func TestDDLMessageTooLarge(t *testing.T) {
	ddlEvent, _, _, _ := common.NewLargeEvent4Test(t)

	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.MaxMessageBytes = 100
	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format
		enc, err := NewEncoder(context.Background(), codecConfig)
		require.NoError(t, err)

		_, err = enc.EncodeDDLEvent(ddlEvent)
		require.ErrorIs(t, err, errors.ErrMessageTooLarge)
	}
}

func TestDMLMessageTooLarge(t *testing.T) {
	_, insertEvent, _, _ := common.NewLargeEvent4Test(t)

	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.MaxMessageBytes = 50

	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatAvro,
		common.EncodingFormatJSON,
	} {
		codecConfig.EncodingFormat = format

		for _, handle := range []string{
			config.LargeMessageHandleOptionNone,
			config.LargeMessageHandleOptionHandleKeyOnly,
			config.LargeMessageHandleOptionClaimCheck,
		} {
			codecConfig.LargeMessageHandle.LargeMessageHandleOption = handle
			if handle == config.LargeMessageHandleOptionClaimCheck {
				codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/simple-claim-check"
			}
			enc, err := NewEncoder(context.Background(), codecConfig)
			require.NoError(t, err)

			err = enc.AppendRowChangedEvent(context.Background(), "", insertEvent)
			require.ErrorIs(t, err, errors.ErrMessageTooLarge, string(format), handle)
		}
	}
}

func TestLargerMessageHandleClaimCheck(t *testing.T) {
	ddlEvent, _, updateEvent, _ := common.NewLargeEvent4Test(t)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck

	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "unsupported:///"

	badRowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
	require.Error(t, err)
	require.Nil(t, badRowEventDecoder)

	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/simple-claim-check"
	for _, rawValue := range []bool{false, true} {
		codecConfig.LargeMessageHandle.ClaimCheckRawValue = rawValue
		for _, format := range []common.EncodingFormatType{
			common.EncodingFormatAvro,
			common.EncodingFormatJSON,
		} {
			codecConfig.EncodingFormat = format
			for _, compressionType := range []string{
				compression.None,
				compression.Snappy,
				compression.LZ4,
			} {
				codecConfig.MaxMessageBytes = config.DefaultMaxMessageBytes
				codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

				enc, err := NewEncoder(ctx, codecConfig)
				require.NoError(t, err)

				m, err := enc.EncodeDDLEvent(ddlEvent)
				require.NoError(t, err)

				rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
				require.NoError(t, err)
				dec, ok := rowEventDecoder.(*Decoder)
				require.True(t, ok)

				dec.AddKeyValue(m.Key, m.Value)

				messageType, hasNext := dec.HasNext()
				require.True(t, hasNext)
				require.Equal(t, common.MessageTypeDDL, messageType)

				_ = dec.NextDDLEvent()
				enc.(*Encoder).config.MaxMessageBytes = 500
				err = enc.AppendRowChangedEvent(ctx, "", updateEvent)
				require.NoError(t, err)

				claimCheckLocationM := enc.Build()[0]

				dec.config.MaxMessageBytes = 500
				dec.AddKeyValue(claimCheckLocationM.Key, claimCheckLocationM.Value)

				messageType, hasNext = dec.HasNext()
				require.True(t, hasNext)
				require.Equal(t, common.MessageTypeRow, messageType)
				require.NotEqual(t, "", dec.msg.ClaimCheckLocation)

				decodedRow := dec.NextDMLEvent()

				require.Equal(t, decodedRow.CommitTs, updateEvent.CommitTs)
				require.Equal(t, decodedRow.TableInfo.GetSchemaName(), updateEvent.TableInfo.GetSchemaName())
				require.Equal(t, decodedRow.TableInfo.GetTableName(), updateEvent.TableInfo.GetTableName())

				decoded, ok := decodedRow.GetNextRow()
				require.True(t, ok)

				common.CompareRow(t, updateEvent.Event, updateEvent.TableInfo, decoded, decodedRow.TableInfo)
			}
		}
	}
}

func TestLargeMessageHandleKeyOnly(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	mock.MatchExpectationsInOrder(false)
	require.NoError(t, err)

	ddlEvent, insertEvent, updateEvent, deleteEvent := common.NewLargeEvent4Test(t)
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly

	badRowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
	require.Error(t, err)
	require.Nil(t, badRowEventDecoder)

	events := []*commonEvent.RowEvent{
		insertEvent,
		updateEvent,
		deleteEvent,
	}

	for _, format := range []common.EncodingFormatType{
		common.EncodingFormatJSON,
		common.EncodingFormatAvro,
	} {
		codecConfig.EncodingFormat = format
		for _, compressionType := range []string{
			compression.None,
			compression.Snappy,
			compression.LZ4,
		} {
			codecConfig.MaxMessageBytes = config.DefaultMaxMessageBytes
			codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compressionType

			enc, err := NewEncoder(ctx, codecConfig)
			require.NoError(t, err)

			rowEventDecoder, err := NewDecoder(ctx, codecConfig, db)
			require.NoError(t, err)
			dec, ok := rowEventDecoder.(*Decoder)
			require.True(t, ok)

			enc.(*Encoder).config.MaxMessageBytes = 500
			dec.config.MaxMessageBytes = 500
			for _, event = range events {
				err = enc.AppendRowChangedEvent(ctx, "", event)
				require.NoError(t, err)

				messages := enc.Build()
				require.Len(t, messages, 1)

				dec.AddKeyValue(messages[0].Key, messages[0].Value)

				messageType, hasNext := dec.HasNext()
				require.True(t, hasNext)
				require.Equal(t, common.MessageTypeRow, messageType)
				require.True(t, dec.msg.HandleKeyOnly)

				decodedRow := dec.NextDMLEvent()
				require.Nil(t, decodedRow)
			}

			enc.(*Encoder).config.MaxMessageBytes = config.DefaultMaxMessageBytes
			dec.config.MaxMessageBytes = config.DefaultMaxMessageBytes
			m, err := enc.EncodeDDLEvent(ddlEvent)
			require.NoError(t, err)

			dec.AddKeyValue(m.Key, m.Value)

			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeDDL, messageType)

			for _, event = range events {
				mock.ExpectQuery("SELECT @@global.time_zone").
					WillReturnRows(mock.NewRows([]string{""}).AddRow("SYSTEM"))

				query := fmt.Sprintf("set @@tidb_snapshot=%v", event.CommitTs)
				mock.ExpectExec(query).WillReturnResult(driver.ResultNoRows)

				query = fmt.Sprintf("set @@tidb_snapshot=%v", event.CommitTs-1)
				mock.ExpectExec(query).WillReturnResult(driver.ResultNoRows)

				names, values := common.LargeColumnKeyValues()
				mock.ExpectQuery("select * from test.t where tu1 = 127").
					WillReturnRows(mock.NewRows(names).AddRow(values...))

				mock.ExpectQuery("select * from test.t where tu1 = 127").
					WillReturnRows(mock.NewRows(names).AddRow(values...))

			}
			_ = dec.NextDDLEvent()

			decodedRows := dec.GetCachedEvents()
			for idx, decodedRow := range decodedRows {
				event := events[idx]

				require.Equal(t, decodedRow.CommitTs, event.CommitTs)
				require.Equal(t, decodedRow.TableInfo.GetSchemaName(), event.TableInfo.GetSchemaName())
				require.Equal(t, decodedRow.TableInfo.GetTableName(), event.TableInfo.GetTableName())

				decoded, ok := decodedRow.GetNextRow()
				require.True(t, ok)

				common.CompareRow(t, event.Event, event.TableInfo, decoded, decodedRow.TableInfo)
			}
		}
	}
}

func TestMarshallerError(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolSimple)

	enc, err := NewEncoder(ctx, codecConfig)
	require.NoError(t, err)

	mockMarshaller := mock_simple.NewMockmarshaller(gomock.NewController(t))
	enc.(*Encoder).marshaller = mockMarshaller

	mockMarshaller.EXPECT().MarshalCheckpoint(gomock.Any()).Return(nil, errors.ErrEncodeFailed)
	_, err = enc.EncodeCheckpointEvent(123)
	require.ErrorIs(t, err, errors.ErrEncodeFailed)

	mockMarshaller.EXPECT().MarshalDDLEvent(gomock.Any()).Return(nil, errors.ErrEncodeFailed)
	_, err = enc.EncodeDDLEvent(&commonEvent.DDLEvent{})
	require.ErrorIs(t, err, errors.ErrEncodeFailed)

	mockMarshaller.EXPECT().MarshalRowChangedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.ErrEncodeFailed)
	err = enc.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{})
	require.ErrorIs(t, err, errors.ErrEncodeFailed)

	rowEventDecoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)
	dec, ok := rowEventDecoder.(*Decoder)
	require.True(t, ok)
	dec.marshaller = mockMarshaller

	mockMarshaller.EXPECT().Unmarshal(gomock.Any(), gomock.Any()).Return(errors.ErrDecodeFailed)
	dec.AddKeyValue([]byte("key"), []byte("value"))

	require.Panics(t, func() {
		_, _ = dec.HasNext()
	})
}
