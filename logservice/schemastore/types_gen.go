package schemastore

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *PersistedDDLEvent) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.ID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "type":
			z.Type, err = dc.ReadByte()
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "table_name_in_ddl_job":
			z.TableNameInDDLJob, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "TableNameInDDLJob")
				return
			}
		case "db_name_in_ddl_job":
			z.DBNameInDDLJob, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "DBNameInDDLJob")
				return
			}
		case "schema_id":
			z.SchemaID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "SchemaID")
				return
			}
		case "table_id":
			z.TableID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "TableID")
				return
			}
		case "schema_name":
			z.SchemaName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "SchemaName")
				return
			}
		case "table_name":
			z.TableName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "TableName")
				return
			}
		case "extra_schema_id":
			z.ExtraSchemaID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaID")
				return
			}
		case "extra_table_id":
			z.ExtraTableID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableID")
				return
			}
		case "extra_schema_name":
			z.ExtraSchemaName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaName")
				return
			}
		case "extra_table_name":
			z.ExtraTableName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableName")
				return
			}
		case "schema_ids":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "SchemaIDs")
				return
			}
			if cap(z.SchemaIDs) >= int(zb0002) {
				z.SchemaIDs = (z.SchemaIDs)[:zb0002]
			} else {
				z.SchemaIDs = make([]int64, zb0002)
			}
			for za0001 := range z.SchemaIDs {
				z.SchemaIDs[za0001], err = dc.ReadInt64()
				if err != nil {
					err = msgp.WrapError(err, "SchemaIDs", za0001)
					return
				}
			}
		case "schema_names":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "SchemaNames")
				return
			}
			if cap(z.SchemaNames) >= int(zb0003) {
				z.SchemaNames = (z.SchemaNames)[:zb0003]
			} else {
				z.SchemaNames = make([]string, zb0003)
			}
			for za0002 := range z.SchemaNames {
				z.SchemaNames[za0002], err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "SchemaNames", za0002)
					return
				}
			}
		case "extra_schema_ids":
			var zb0004 uint32
			zb0004, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaIDs")
				return
			}
			if cap(z.ExtraSchemaIDs) >= int(zb0004) {
				z.ExtraSchemaIDs = (z.ExtraSchemaIDs)[:zb0004]
			} else {
				z.ExtraSchemaIDs = make([]int64, zb0004)
			}
			for za0003 := range z.ExtraSchemaIDs {
				z.ExtraSchemaIDs[za0003], err = dc.ReadInt64()
				if err != nil {
					err = msgp.WrapError(err, "ExtraSchemaIDs", za0003)
					return
				}
			}
		case "extra_schema_names":
			var zb0005 uint32
			zb0005, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaNames")
				return
			}
			if cap(z.ExtraSchemaNames) >= int(zb0005) {
				z.ExtraSchemaNames = (z.ExtraSchemaNames)[:zb0005]
			} else {
				z.ExtraSchemaNames = make([]string, zb0005)
			}
			for za0004 := range z.ExtraSchemaNames {
				z.ExtraSchemaNames[za0004], err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "ExtraSchemaNames", za0004)
					return
				}
			}
		case "extra_table_names":
			var zb0006 uint32
			zb0006, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableNames")
				return
			}
			if cap(z.ExtraTableNames) >= int(zb0006) {
				z.ExtraTableNames = (z.ExtraTableNames)[:zb0006]
			} else {
				z.ExtraTableNames = make([]string, zb0006)
			}
			for za0005 := range z.ExtraTableNames {
				z.ExtraTableNames[za0005], err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "ExtraTableNames", za0005)
					return
				}
			}
		case "prev_partitions":
			var zb0007 uint32
			zb0007, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "PrevPartitions")
				return
			}
			if cap(z.PrevPartitions) >= int(zb0007) {
				z.PrevPartitions = (z.PrevPartitions)[:zb0007]
			} else {
				z.PrevPartitions = make([]int64, zb0007)
			}
			for za0006 := range z.PrevPartitions {
				z.PrevPartitions[za0006], err = dc.ReadInt64()
				if err != nil {
					err = msgp.WrapError(err, "PrevPartitions", za0006)
					return
				}
			}
		case "query":
			z.Query, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Query")
				return
			}
		case "schema_version":
			z.SchemaVersion, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "SchemaVersion")
				return
			}
		case "finished_ts":
			z.FinishedTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "FinishedTs")
				return
			}
		case "table_info_value":
			z.TableInfoValue, err = dc.ReadBytes(z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		case "extra_table_info_value":
			z.ExtraTableInfoValue, err = dc.ReadBytes(z.ExtraTableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableInfoValue")
				return
			}
		case "multi_table_info_value":
			var zb0008 uint32
			zb0008, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "MultipleTableInfosValue")
				return
			}
			if cap(z.MultipleTableInfosValue) >= int(zb0008) {
				z.MultipleTableInfosValue = (z.MultipleTableInfosValue)[:zb0008]
			} else {
				z.MultipleTableInfosValue = make([][]byte, zb0008)
			}
			for za0007 := range z.MultipleTableInfosValue {
				z.MultipleTableInfosValue[za0007], err = dc.ReadBytes(z.MultipleTableInfosValue[za0007])
				if err != nil {
					err = msgp.WrapError(err, "MultipleTableInfosValue", za0007)
					return
				}
			}
		case "bdr_role":
			z.BDRRole, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "BDRRole")
				return
			}
		case "cdc_write_source":
			z.CDCWriteSource, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "CDCWriteSource")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *PersistedDDLEvent) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 26
	// write "id"
	err = en.Append(0xde, 0x0, 0x1a, 0xa2, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.ID)
	if err != nil {
		err = msgp.WrapError(err, "ID")
		return
	}
	// write "type"
	err = en.Append(0xa4, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return
	}
	err = en.WriteByte(z.Type)
	if err != nil {
		err = msgp.WrapError(err, "Type")
		return
	}
	// write "table_name_in_ddl_job"
	err = en.Append(0xb5, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x69, 0x6e, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x6a, 0x6f, 0x62)
	if err != nil {
		return
	}
	err = en.WriteString(z.TableNameInDDLJob)
	if err != nil {
		err = msgp.WrapError(err, "TableNameInDDLJob")
		return
	}
	// write "db_name_in_ddl_job"
	err = en.Append(0xb2, 0x64, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x69, 0x6e, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x6a, 0x6f, 0x62)
	if err != nil {
		return
	}
	err = en.WriteString(z.DBNameInDDLJob)
	if err != nil {
		err = msgp.WrapError(err, "DBNameInDDLJob")
		return
	}
	// write "schema_id"
	err = en.Append(0xa9, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.SchemaID)
	if err != nil {
		err = msgp.WrapError(err, "SchemaID")
		return
	}
	// write "table_id"
	err = en.Append(0xa8, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.TableID)
	if err != nil {
		err = msgp.WrapError(err, "TableID")
		return
	}
	// write "schema_name"
	err = en.Append(0xab, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.SchemaName)
	if err != nil {
		err = msgp.WrapError(err, "SchemaName")
		return
	}
	// write "table_name"
	err = en.Append(0xaa, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.TableName)
	if err != nil {
		err = msgp.WrapError(err, "TableName")
		return
	}
	// write "extra_schema_id"
	err = en.Append(0xaf, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.ExtraSchemaID)
	if err != nil {
		err = msgp.WrapError(err, "ExtraSchemaID")
		return
	}
	// write "extra_table_id"
	err = en.Append(0xae, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.ExtraTableID)
	if err != nil {
		err = msgp.WrapError(err, "ExtraTableID")
		return
	}
	// write "extra_schema_name"
	err = en.Append(0xb1, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.ExtraSchemaName)
	if err != nil {
		err = msgp.WrapError(err, "ExtraSchemaName")
		return
	}
	// write "extra_table_name"
	err = en.Append(0xb0, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.ExtraTableName)
	if err != nil {
		err = msgp.WrapError(err, "ExtraTableName")
		return
	}
	// write "schema_ids"
	err = en.Append(0xaa, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.SchemaIDs)))
	if err != nil {
		err = msgp.WrapError(err, "SchemaIDs")
		return
	}
	for za0001 := range z.SchemaIDs {
		err = en.WriteInt64(z.SchemaIDs[za0001])
		if err != nil {
			err = msgp.WrapError(err, "SchemaIDs", za0001)
			return
		}
	}
	// write "schema_names"
	err = en.Append(0xac, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.SchemaNames)))
	if err != nil {
		err = msgp.WrapError(err, "SchemaNames")
		return
	}
	for za0002 := range z.SchemaNames {
		err = en.WriteString(z.SchemaNames[za0002])
		if err != nil {
			err = msgp.WrapError(err, "SchemaNames", za0002)
			return
		}
	}
	// write "extra_schema_ids"
	err = en.Append(0xb0, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.ExtraSchemaIDs)))
	if err != nil {
		err = msgp.WrapError(err, "ExtraSchemaIDs")
		return
	}
	for za0003 := range z.ExtraSchemaIDs {
		err = en.WriteInt64(z.ExtraSchemaIDs[za0003])
		if err != nil {
			err = msgp.WrapError(err, "ExtraSchemaIDs", za0003)
			return
		}
	}
	// write "extra_schema_names"
	err = en.Append(0xb2, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.ExtraSchemaNames)))
	if err != nil {
		err = msgp.WrapError(err, "ExtraSchemaNames")
		return
	}
	for za0004 := range z.ExtraSchemaNames {
		err = en.WriteString(z.ExtraSchemaNames[za0004])
		if err != nil {
			err = msgp.WrapError(err, "ExtraSchemaNames", za0004)
			return
		}
	}
	// write "extra_table_names"
	err = en.Append(0xb1, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.ExtraTableNames)))
	if err != nil {
		err = msgp.WrapError(err, "ExtraTableNames")
		return
	}
	for za0005 := range z.ExtraTableNames {
		err = en.WriteString(z.ExtraTableNames[za0005])
		if err != nil {
			err = msgp.WrapError(err, "ExtraTableNames", za0005)
			return
		}
	}
	// write "prev_partitions"
	err = en.Append(0xaf, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.PrevPartitions)))
	if err != nil {
		err = msgp.WrapError(err, "PrevPartitions")
		return
	}
	for za0006 := range z.PrevPartitions {
		err = en.WriteInt64(z.PrevPartitions[za0006])
		if err != nil {
			err = msgp.WrapError(err, "PrevPartitions", za0006)
			return
		}
	}
	// write "query"
	err = en.Append(0xa5, 0x71, 0x75, 0x65, 0x72, 0x79)
	if err != nil {
		return
	}
	err = en.WriteString(z.Query)
	if err != nil {
		err = msgp.WrapError(err, "Query")
		return
	}
	// write "schema_version"
	err = en.Append(0xae, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.SchemaVersion)
	if err != nil {
		err = msgp.WrapError(err, "SchemaVersion")
		return
	}
	// write "finished_ts"
	err = en.Append(0xab, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.FinishedTs)
	if err != nil {
		err = msgp.WrapError(err, "FinishedTs")
		return
	}
	// write "table_info_value"
	err = en.Append(0xb0, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.TableInfoValue)
	if err != nil {
		err = msgp.WrapError(err, "TableInfoValue")
		return
	}
	// write "extra_table_info_value"
	err = en.Append(0xb6, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.ExtraTableInfoValue)
	if err != nil {
		err = msgp.WrapError(err, "ExtraTableInfoValue")
		return
	}
	// write "multi_table_info_value"
	err = en.Append(0xb6, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.MultipleTableInfosValue)))
	if err != nil {
		err = msgp.WrapError(err, "MultipleTableInfosValue")
		return
	}
	for za0007 := range z.MultipleTableInfosValue {
		err = en.WriteBytes(z.MultipleTableInfosValue[za0007])
		if err != nil {
			err = msgp.WrapError(err, "MultipleTableInfosValue", za0007)
			return
		}
	}
	// write "bdr_role"
	err = en.Append(0xa8, 0x62, 0x64, 0x72, 0x5f, 0x72, 0x6f, 0x6c, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.BDRRole)
	if err != nil {
		err = msgp.WrapError(err, "BDRRole")
		return
	}
	// write "cdc_write_source"
	err = en.Append(0xb0, 0x63, 0x64, 0x63, 0x5f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x5f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.CDCWriteSource)
	if err != nil {
		err = msgp.WrapError(err, "CDCWriteSource")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PersistedDDLEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 26
	// string "id"
	o = append(o, 0xde, 0x0, 0x1a, 0xa2, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.ID)
	// string "type"
	o = append(o, 0xa4, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendByte(o, z.Type)
	// string "table_name_in_ddl_job"
	o = append(o, 0xb5, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x69, 0x6e, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x6a, 0x6f, 0x62)
	o = msgp.AppendString(o, z.TableNameInDDLJob)
	// string "db_name_in_ddl_job"
	o = append(o, 0xb2, 0x64, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x69, 0x6e, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x6a, 0x6f, 0x62)
	o = msgp.AppendString(o, z.DBNameInDDLJob)
	// string "schema_id"
	o = append(o, 0xa9, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.SchemaID)
	// string "table_id"
	o = append(o, 0xa8, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.TableID)
	// string "schema_name"
	o = append(o, 0xab, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.SchemaName)
	// string "table_name"
	o = append(o, 0xaa, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.TableName)
	// string "extra_schema_id"
	o = append(o, 0xaf, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.ExtraSchemaID)
	// string "extra_table_id"
	o = append(o, 0xae, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.ExtraTableID)
	// string "extra_schema_name"
	o = append(o, 0xb1, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.ExtraSchemaName)
	// string "extra_table_name"
	o = append(o, 0xb0, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.ExtraTableName)
	// string "schema_ids"
	o = append(o, 0xaa, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.SchemaIDs)))
	for za0001 := range z.SchemaIDs {
		o = msgp.AppendInt64(o, z.SchemaIDs[za0001])
	}
	// string "schema_names"
	o = append(o, 0xac, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.SchemaNames)))
	for za0002 := range z.SchemaNames {
		o = msgp.AppendString(o, z.SchemaNames[za0002])
	}
	// string "extra_schema_ids"
	o = append(o, 0xb0, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.ExtraSchemaIDs)))
	for za0003 := range z.ExtraSchemaIDs {
		o = msgp.AppendInt64(o, z.ExtraSchemaIDs[za0003])
	}
	// string "extra_schema_names"
	o = append(o, 0xb2, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.ExtraSchemaNames)))
	for za0004 := range z.ExtraSchemaNames {
		o = msgp.AppendString(o, z.ExtraSchemaNames[za0004])
	}
	// string "extra_table_names"
	o = append(o, 0xb1, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.ExtraTableNames)))
	for za0005 := range z.ExtraTableNames {
		o = msgp.AppendString(o, z.ExtraTableNames[za0005])
	}
	// string "prev_partitions"
	o = append(o, 0xaf, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.PrevPartitions)))
	for za0006 := range z.PrevPartitions {
		o = msgp.AppendInt64(o, z.PrevPartitions[za0006])
	}
	// string "query"
	o = append(o, 0xa5, 0x71, 0x75, 0x65, 0x72, 0x79)
	o = msgp.AppendString(o, z.Query)
	// string "schema_version"
	o = append(o, 0xae, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendInt64(o, z.SchemaVersion)
	// string "finished_ts"
	o = append(o, 0xab, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.FinishedTs)
	// string "table_info_value"
	o = append(o, 0xb0, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendBytes(o, z.TableInfoValue)
	// string "extra_table_info_value"
	o = append(o, 0xb6, 0x65, 0x78, 0x74, 0x72, 0x61, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendBytes(o, z.ExtraTableInfoValue)
	// string "multi_table_info_value"
	o = append(o, 0xb6, 0x6d, 0x75, 0x6c, 0x74, 0x69, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendArrayHeader(o, uint32(len(z.MultipleTableInfosValue)))
	for za0007 := range z.MultipleTableInfosValue {
		o = msgp.AppendBytes(o, z.MultipleTableInfosValue[za0007])
	}
	// string "bdr_role"
	o = append(o, 0xa8, 0x62, 0x64, 0x72, 0x5f, 0x72, 0x6f, 0x6c, 0x65)
	o = msgp.AppendString(o, z.BDRRole)
	// string "cdc_write_source"
	o = append(o, 0xb0, 0x63, 0x64, 0x63, 0x5f, 0x77, 0x72, 0x69, 0x74, 0x65, 0x5f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65)
	o = msgp.AppendUint64(o, z.CDCWriteSource)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PersistedDDLEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "id":
			z.ID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ID")
				return
			}
		case "type":
			z.Type, bts, err = msgp.ReadByteBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Type")
				return
			}
		case "table_name_in_ddl_job":
			z.TableNameInDDLJob, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TableNameInDDLJob")
				return
			}
		case "db_name_in_ddl_job":
			z.DBNameInDDLJob, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "DBNameInDDLJob")
				return
			}
		case "schema_id":
			z.SchemaID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaID")
				return
			}
		case "table_id":
			z.TableID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TableID")
				return
			}
		case "schema_name":
			z.SchemaName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaName")
				return
			}
		case "table_name":
			z.TableName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TableName")
				return
			}
		case "extra_schema_id":
			z.ExtraSchemaID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaID")
				return
			}
		case "extra_table_id":
			z.ExtraTableID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableID")
				return
			}
		case "extra_schema_name":
			z.ExtraSchemaName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaName")
				return
			}
		case "extra_table_name":
			z.ExtraTableName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableName")
				return
			}
		case "schema_ids":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaIDs")
				return
			}
			if cap(z.SchemaIDs) >= int(zb0002) {
				z.SchemaIDs = (z.SchemaIDs)[:zb0002]
			} else {
				z.SchemaIDs = make([]int64, zb0002)
			}
			for za0001 := range z.SchemaIDs {
				z.SchemaIDs[za0001], bts, err = msgp.ReadInt64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "SchemaIDs", za0001)
					return
				}
			}
		case "schema_names":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaNames")
				return
			}
			if cap(z.SchemaNames) >= int(zb0003) {
				z.SchemaNames = (z.SchemaNames)[:zb0003]
			} else {
				z.SchemaNames = make([]string, zb0003)
			}
			for za0002 := range z.SchemaNames {
				z.SchemaNames[za0002], bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "SchemaNames", za0002)
					return
				}
			}
		case "extra_schema_ids":
			var zb0004 uint32
			zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaIDs")
				return
			}
			if cap(z.ExtraSchemaIDs) >= int(zb0004) {
				z.ExtraSchemaIDs = (z.ExtraSchemaIDs)[:zb0004]
			} else {
				z.ExtraSchemaIDs = make([]int64, zb0004)
			}
			for za0003 := range z.ExtraSchemaIDs {
				z.ExtraSchemaIDs[za0003], bts, err = msgp.ReadInt64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ExtraSchemaIDs", za0003)
					return
				}
			}
		case "extra_schema_names":
			var zb0005 uint32
			zb0005, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraSchemaNames")
				return
			}
			if cap(z.ExtraSchemaNames) >= int(zb0005) {
				z.ExtraSchemaNames = (z.ExtraSchemaNames)[:zb0005]
			} else {
				z.ExtraSchemaNames = make([]string, zb0005)
			}
			for za0004 := range z.ExtraSchemaNames {
				z.ExtraSchemaNames[za0004], bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ExtraSchemaNames", za0004)
					return
				}
			}
		case "extra_table_names":
			var zb0006 uint32
			zb0006, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableNames")
				return
			}
			if cap(z.ExtraTableNames) >= int(zb0006) {
				z.ExtraTableNames = (z.ExtraTableNames)[:zb0006]
			} else {
				z.ExtraTableNames = make([]string, zb0006)
			}
			for za0005 := range z.ExtraTableNames {
				z.ExtraTableNames[za0005], bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "ExtraTableNames", za0005)
					return
				}
			}
		case "prev_partitions":
			var zb0007 uint32
			zb0007, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PrevPartitions")
				return
			}
			if cap(z.PrevPartitions) >= int(zb0007) {
				z.PrevPartitions = (z.PrevPartitions)[:zb0007]
			} else {
				z.PrevPartitions = make([]int64, zb0007)
			}
			for za0006 := range z.PrevPartitions {
				z.PrevPartitions[za0006], bts, err = msgp.ReadInt64Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "PrevPartitions", za0006)
					return
				}
			}
		case "query":
			z.Query, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Query")
				return
			}
		case "schema_version":
			z.SchemaVersion, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaVersion")
				return
			}
		case "finished_ts":
			z.FinishedTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "FinishedTs")
				return
			}
		case "table_info_value":
			z.TableInfoValue, bts, err = msgp.ReadBytesBytes(bts, z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		case "extra_table_info_value":
			z.ExtraTableInfoValue, bts, err = msgp.ReadBytesBytes(bts, z.ExtraTableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "ExtraTableInfoValue")
				return
			}
		case "multi_table_info_value":
			var zb0008 uint32
			zb0008, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "MultipleTableInfosValue")
				return
			}
			if cap(z.MultipleTableInfosValue) >= int(zb0008) {
				z.MultipleTableInfosValue = (z.MultipleTableInfosValue)[:zb0008]
			} else {
				z.MultipleTableInfosValue = make([][]byte, zb0008)
			}
			for za0007 := range z.MultipleTableInfosValue {
				z.MultipleTableInfosValue[za0007], bts, err = msgp.ReadBytesBytes(bts, z.MultipleTableInfosValue[za0007])
				if err != nil {
					err = msgp.WrapError(err, "MultipleTableInfosValue", za0007)
					return
				}
			}
		case "bdr_role":
			z.BDRRole, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "BDRRole")
				return
			}
		case "cdc_write_source":
			z.CDCWriteSource, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "CDCWriteSource")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *PersistedDDLEvent) Msgsize() (s int) {
	s = 3 + 3 + msgp.Int64Size + 5 + msgp.ByteSize + 22 + msgp.StringPrefixSize + len(z.TableNameInDDLJob) + 19 + msgp.StringPrefixSize + len(z.DBNameInDDLJob) + 10 + msgp.Int64Size + 9 + msgp.Int64Size + 12 + msgp.StringPrefixSize + len(z.SchemaName) + 11 + msgp.StringPrefixSize + len(z.TableName) + 16 + msgp.Int64Size + 15 + msgp.Int64Size + 18 + msgp.StringPrefixSize + len(z.ExtraSchemaName) + 17 + msgp.StringPrefixSize + len(z.ExtraTableName) + 11 + msgp.ArrayHeaderSize + (len(z.SchemaIDs) * (msgp.Int64Size)) + 13 + msgp.ArrayHeaderSize
	for za0002 := range z.SchemaNames {
		s += msgp.StringPrefixSize + len(z.SchemaNames[za0002])
	}
	s += 17 + msgp.ArrayHeaderSize + (len(z.ExtraSchemaIDs) * (msgp.Int64Size)) + 19 + msgp.ArrayHeaderSize
	for za0004 := range z.ExtraSchemaNames {
		s += msgp.StringPrefixSize + len(z.ExtraSchemaNames[za0004])
	}
	s += 18 + msgp.ArrayHeaderSize
	for za0005 := range z.ExtraTableNames {
		s += msgp.StringPrefixSize + len(z.ExtraTableNames[za0005])
	}
	s += 16 + msgp.ArrayHeaderSize + (len(z.PrevPartitions) * (msgp.Int64Size)) + 6 + msgp.StringPrefixSize + len(z.Query) + 15 + msgp.Int64Size + 12 + msgp.Uint64Size + 17 + msgp.BytesPrefixSize + len(z.TableInfoValue) + 23 + msgp.BytesPrefixSize + len(z.ExtraTableInfoValue) + 23 + msgp.ArrayHeaderSize
	for za0007 := range z.MultipleTableInfosValue {
		s += msgp.BytesPrefixSize + len(z.MultipleTableInfosValue[za0007])
	}
	s += 9 + msgp.StringPrefixSize + len(z.BDRRole) + 17 + msgp.Uint64Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *PersistedTableInfoEntry) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "schema_id":
			z.SchemaID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "SchemaID")
				return
			}
		case "schema_name":
			z.SchemaName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "SchemaName")
				return
			}
		case "table_info_value":
			z.TableInfoValue, err = dc.ReadBytes(z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *PersistedTableInfoEntry) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "schema_id"
	err = en.Append(0x83, 0xa9, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.SchemaID)
	if err != nil {
		err = msgp.WrapError(err, "SchemaID")
		return
	}
	// write "schema_name"
	err = en.Append(0xab, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.SchemaName)
	if err != nil {
		err = msgp.WrapError(err, "SchemaName")
		return
	}
	// write "table_info_value"
	err = en.Append(0xb0, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.TableInfoValue)
	if err != nil {
		err = msgp.WrapError(err, "TableInfoValue")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *PersistedTableInfoEntry) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "schema_id"
	o = append(o, 0x83, 0xa9, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.SchemaID)
	// string "schema_name"
	o = append(o, 0xab, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.SchemaName)
	// string "table_info_value"
	o = append(o, 0xb0, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendBytes(o, z.TableInfoValue)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *PersistedTableInfoEntry) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "schema_id":
			z.SchemaID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaID")
				return
			}
		case "schema_name":
			z.SchemaName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaName")
				return
			}
		case "table_info_value":
			z.TableInfoValue, bts, err = msgp.ReadBytesBytes(bts, z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *PersistedTableInfoEntry) Msgsize() (s int) {
	s = 1 + 10 + msgp.Int64Size + 12 + msgp.StringPrefixSize + len(z.SchemaName) + 17 + msgp.BytesPrefixSize + len(z.TableInfoValue)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *UpperBoundMeta) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "finished_ddl_ts":
			z.FinishedDDLTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "FinishedDDLTs")
				return
			}
		case "schema_version":
			z.SchemaVersion, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "SchemaVersion")
				return
			}
		case "resolved_ts":
			z.ResolvedTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z UpperBoundMeta) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "finished_ddl_ts"
	err = en.Append(0x83, 0xaf, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.FinishedDDLTs)
	if err != nil {
		err = msgp.WrapError(err, "FinishedDDLTs")
		return
	}
	// write "schema_version"
	err = en.Append(0xae, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.SchemaVersion)
	if err != nil {
		err = msgp.WrapError(err, "SchemaVersion")
		return
	}
	// write "resolved_ts"
	err = en.Append(0xab, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x5f, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint64(z.ResolvedTs)
	if err != nil {
		err = msgp.WrapError(err, "ResolvedTs")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z UpperBoundMeta) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "finished_ddl_ts"
	o = append(o, 0x83, 0xaf, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x64, 0x64, 0x6c, 0x5f, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.FinishedDDLTs)
	// string "schema_version"
	o = append(o, 0xae, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendInt64(o, z.SchemaVersion)
	// string "resolved_ts"
	o = append(o, 0xab, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x64, 0x5f, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.ResolvedTs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *UpperBoundMeta) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "finished_ddl_ts":
			z.FinishedDDLTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "FinishedDDLTs")
				return
			}
		case "schema_version":
			z.SchemaVersion, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "SchemaVersion")
				return
			}
		case "resolved_ts":
			z.ResolvedTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "ResolvedTs")
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z UpperBoundMeta) Msgsize() (s int) {
	s = 1 + 16 + msgp.Uint64Size + 15 + msgp.Int64Size + 12 + msgp.Uint64Size
	return
}
