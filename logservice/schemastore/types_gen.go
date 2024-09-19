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
		case "prev_schema_id":
			z.PrevSchemaID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "PrevSchemaID")
				return
			}
		case "prev_table_id":
			z.PrevTableID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "PrevTableID")
				return
			}
		case "prev_schema_name":
			z.PrevSchemaName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "PrevSchemaName")
				return
			}
		case "prev_table_name":
			z.PrevTableName, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "PrevTableName")
				return
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
		case "table_info_value":
			z.TableInfoValue, err = dc.ReadBytes(z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		case "finished_ts":
			z.FinishedTs, err = dc.ReadUint64()
			if err != nil {
				err = msgp.WrapError(err, "FinishedTs")
				return
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
	// map header, size 16
	// write "id"
	err = en.Append(0xde, 0x0, 0x10, 0xa2, 0x69, 0x64)
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
	// write "prev_schema_id"
	err = en.Append(0xae, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.PrevSchemaID)
	if err != nil {
		err = msgp.WrapError(err, "PrevSchemaID")
		return
	}
	// write "prev_table_id"
	err = en.Append(0xad, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.PrevTableID)
	if err != nil {
		err = msgp.WrapError(err, "PrevTableID")
		return
	}
	// write "prev_schema_name"
	err = en.Append(0xb0, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.PrevSchemaName)
	if err != nil {
		err = msgp.WrapError(err, "PrevSchemaName")
		return
	}
	// write "prev_table_name"
	err = en.Append(0xaf, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.PrevTableName)
	if err != nil {
		err = msgp.WrapError(err, "PrevTableName")
		return
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
	// map header, size 16
	// string "id"
	o = append(o, 0xde, 0x0, 0x10, 0xa2, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.ID)
	// string "type"
	o = append(o, 0xa4, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendByte(o, z.Type)
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
	// string "prev_schema_id"
	o = append(o, 0xae, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.PrevSchemaID)
	// string "prev_table_id"
	o = append(o, 0xad, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.PrevTableID)
	// string "prev_schema_name"
	o = append(o, 0xb0, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.PrevSchemaName)
	// string "prev_table_name"
	o = append(o, 0xaf, 0x70, 0x72, 0x65, 0x76, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.PrevTableName)
	// string "query"
	o = append(o, 0xa5, 0x71, 0x75, 0x65, 0x72, 0x79)
	o = msgp.AppendString(o, z.Query)
	// string "schema_version"
	o = append(o, 0xae, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendInt64(o, z.SchemaVersion)
	// string "table_info_value"
	o = append(o, 0xb0, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendBytes(o, z.TableInfoValue)
	// string "finished_ts"
	o = append(o, 0xab, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x74, 0x73)
	o = msgp.AppendUint64(o, z.FinishedTs)
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
		case "prev_schema_id":
			z.PrevSchemaID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PrevSchemaID")
				return
			}
		case "prev_table_id":
			z.PrevTableID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PrevTableID")
				return
			}
		case "prev_schema_name":
			z.PrevSchemaName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PrevSchemaName")
				return
			}
		case "prev_table_name":
			z.PrevTableName, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "PrevTableName")
				return
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
		case "table_info_value":
			z.TableInfoValue, bts, err = msgp.ReadBytesBytes(bts, z.TableInfoValue)
			if err != nil {
				err = msgp.WrapError(err, "TableInfoValue")
				return
			}
		case "finished_ts":
			z.FinishedTs, bts, err = msgp.ReadUint64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "FinishedTs")
				return
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
	s = 3 + 3 + msgp.Int64Size + 5 + msgp.ByteSize + 10 + msgp.Int64Size + 9 + msgp.Int64Size + 12 + msgp.StringPrefixSize + len(z.SchemaName) + 11 + msgp.StringPrefixSize + len(z.TableName) + 15 + msgp.Int64Size + 14 + msgp.Int64Size + 17 + msgp.StringPrefixSize + len(z.PrevSchemaName) + 16 + msgp.StringPrefixSize + len(z.PrevTableName) + 6 + msgp.StringPrefixSize + len(z.Query) + 15 + msgp.Int64Size + 17 + msgp.BytesPrefixSize + len(z.TableInfoValue) + 12 + msgp.Uint64Size + 9 + msgp.StringPrefixSize + len(z.BDRRole) + 17 + msgp.Uint64Size
	return
}
