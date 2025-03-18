package common

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *TableName) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "db-name":
			z.Schema, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Schema")
				return
			}
		case "tbl-name":
			z.Table, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Table")
				return
			}
		case "tbl-id":
			z.TableID, err = dc.ReadInt64()
			if err != nil {
				err = msgp.WrapError(err, "TableID")
				return
			}
		case "is-partition":
			z.IsPartition, err = dc.ReadBool()
			if err != nil {
				err = msgp.WrapError(err, "IsPartition")
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
func (z *TableName) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "db-name"
	err = en.Append(0x84, 0xa7, 0x64, 0x62, 0x2d, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Schema)
	if err != nil {
		err = msgp.WrapError(err, "Schema")
		return
	}
	// write "tbl-name"
	err = en.Append(0xa8, 0x74, 0x62, 0x6c, 0x2d, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Table)
	if err != nil {
		err = msgp.WrapError(err, "Table")
		return
	}
	// write "tbl-id"
	err = en.Append(0xa6, 0x74, 0x62, 0x6c, 0x2d, 0x69, 0x64)
	if err != nil {
		return
	}
	err = en.WriteInt64(z.TableID)
	if err != nil {
		err = msgp.WrapError(err, "TableID")
		return
	}
	// write "is-partition"
	err = en.Append(0xac, 0x69, 0x73, 0x2d, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteBool(z.IsPartition)
	if err != nil {
		err = msgp.WrapError(err, "IsPartition")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TableName) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "db-name"
	o = append(o, 0x84, 0xa7, 0x64, 0x62, 0x2d, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Schema)
	// string "tbl-name"
	o = append(o, 0xa8, 0x74, 0x62, 0x6c, 0x2d, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Table)
	// string "tbl-id"
	o = append(o, 0xa6, 0x74, 0x62, 0x6c, 0x2d, 0x69, 0x64)
	o = msgp.AppendInt64(o, z.TableID)
	// string "is-partition"
	o = append(o, 0xac, 0x69, 0x73, 0x2d, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendBool(o, z.IsPartition)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TableName) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "db-name":
			z.Schema, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Schema")
				return
			}
		case "tbl-name":
			z.Table, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Table")
				return
			}
		case "tbl-id":
			z.TableID, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "TableID")
				return
			}
		case "is-partition":
			z.IsPartition, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "IsPartition")
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
func (z *TableName) Msgsize() (s int) {
	s = 1 + 8 + msgp.StringPrefixSize + len(z.Schema) + 9 + msgp.StringPrefixSize + len(z.Table) + 7 + msgp.Int64Size + 13 + msgp.BoolSize
	return
}
