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

package common

import (
	"bytes"
	"encoding/binary"
	"regexp"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/util/naming"
	"go.uber.org/zap"
)

const (
	// DefaultKeyspace is the default keyspace value,
	// all the old changefeed will be put into default keyspace
	DefaultKeyspace   = "default"
	DefaultKeyspaceID = 0
)

type (
	Ts      = uint64
	TableID = int64
)

type CoordinatorID string

func (id CoordinatorID) String() string { return string(id) }

type MaintainerID string

func (m MaintainerID) String() string { return string(m) }

type DispatcherID GID

func NewDispatcherID() DispatcherID {
	return DispatcherID(NewGID())
}

func NewDispatcherIDFromPB(pb *heartbeatpb.DispatcherID) DispatcherID {
	d := DispatcherID{Low: pb.Low, High: pb.High}
	return d
}

func (d DispatcherID) ToPB() *heartbeatpb.DispatcherID {
	return &heartbeatpb.DispatcherID{
		Low:  d.Low,
		High: d.High,
	}
}

func (d DispatcherID) String() string {
	return GID(d).String()
}

func (d *DispatcherID) GetSize() int {
	return 16
}

func (d DispatcherID) GetLow() uint64 {
	return d.Low
}

func (d *DispatcherID) Unmarshal(b []byte) error {
	gid := GID{}
	gid.Unmarshal(b)
	*d = DispatcherID(gid)
	return nil
}

func (d DispatcherID) Marshal() []byte {
	return GID(d).Marshal()
}

func (d DispatcherID) Equal(inferior any) bool {
	tbl := inferior.(DispatcherID)
	return d.Low == tbl.Low && d.High == tbl.High
}

func (d DispatcherID) Less(t any) bool {
	cf := t.(DispatcherID)
	return d.Low < cf.Low || d.Low == cf.Low && d.High < cf.High
}

type SchemaID int64

type GID struct {
	Low  uint64 `json:"low"`
	High uint64 `json:"high"`
}

func (g GID) IsZero() bool {
	return g.Low == 0 && g.High == 0
}

func (g GID) FastHash() uint64 {
	// Combine the two parts using XOR and a bit shift
	return g.Low ^ (g.High << 1)
}

func (g GID) Hash(mod uint64) int {
	return int(g.FastHash() % mod)
}

func (g GID) Marshal() []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b[0:8], g.Low)
	binary.LittleEndian.PutUint64(b[8:16], g.High)
	return b
}

func (g *GID) Unmarshal(b []byte) {
	g.Low = binary.LittleEndian.Uint64(b[0:8])
	g.High = binary.LittleEndian.Uint64(b[8:16])
}

func NewGID() GID {
	uuid := uuid.New()
	return GID{
		Low:  binary.LittleEndian.Uint64(uuid[0:8]),
		High: binary.LittleEndian.Uint64(uuid[8:16]),
	}
}

func (g *GID) PString() string {
	return g.String()
}

func (g GID) String() string {
	var buf bytes.Buffer
	buf.WriteString(strconv.FormatUint(g.Low, 10))
	buf.WriteString(strconv.FormatUint(g.High, 10))
	return buf.String()
}

func (g GID) GetSize() int {
	return 16
}

func NewGIDWithValue(Low uint64, High uint64) GID {
	return GID{
		Low:  Low,
		High: High,
	}
}

// ChangeFeedDisplayName represents the user-friendly name and keyspace of a changefeed.
// This structure is used for external queries and display purposes.
type ChangeFeedDisplayName struct {
	Name     string `json:"name"`
	Keyspace string `json:"keyspace"`
}

func NewChangeFeedDisplayName(name string, keyspace string) ChangeFeedDisplayName {
	return ChangeFeedDisplayName{
		Name:     name,
		Keyspace: keyspace,
	}
}

func (r ChangeFeedDisplayName) String() string {
	return r.Keyspace + "/" + r.Name
}

// ChangeFeedID is the unique identifier of a changefeed.
// GID is the inner unique identifier of a changefeed.
// we can use Id to represent the changefeedID in performance-critical scenarios.
// DisplayName is the user-friendly expression of a changefeed.
// ChangefeedID can be specified the name of changefeedID.
// If the name is not specified, it will be the id in string format.
// We ensure whether the id or the representation is both unique in the cluster.
type ChangeFeedID struct {
	Id          GID                   `json:"id"`
	DisplayName ChangeFeedDisplayName `json:"display"`
}

func NewChangefeedID(keyspace string) ChangeFeedID {
	cfID := ChangeFeedID{
		Id: NewGID(),
	}

	if keyspace == "" {
		keyspace = DefaultKeyspace
	}

	cfID.DisplayName = ChangeFeedDisplayName{
		Name:     cfID.Id.String(),
		Keyspace: keyspace,
	}
	return cfID
}

func NewChangeFeedIDWithName(name string, keyspace string) ChangeFeedID {
	if keyspace == "" {
		keyspace = DefaultKeyspace
	}

	return ChangeFeedID{
		Id: NewGID(),
		DisplayName: ChangeFeedDisplayName{
			Name:     name,
			Keyspace: keyspace,
		},
	}
}

func NewChangeFeedIDWithDisplayName(name ChangeFeedDisplayName) ChangeFeedID {
	return ChangeFeedID{
		Id:          NewGID(),
		DisplayName: name,
	}
}

func (cfID ChangeFeedID) String() string {
	return cfID.DisplayName.String()
}

func (cfID ChangeFeedID) Name() string {
	return cfID.DisplayName.Name
}

func (cfID ChangeFeedID) Keyspace() string {
	return cfID.DisplayName.Keyspace
}

func (cfID ChangeFeedID) ID() GID {
	return cfID.Id
}

func NewChangefeedIDFromPB(pb *heartbeatpb.ChangefeedID) ChangeFeedID {
	d := ChangeFeedID{
		Id: GID{
			Low:  pb.Low,
			High: pb.High,
		},
		DisplayName: ChangeFeedDisplayName{
			Name:     pb.Name,
			Keyspace: pb.Keyspace,
		},
	}
	return d
}

func NewChangefeedGIDFromPB(pb *heartbeatpb.ChangefeedID) GID {
	return GID{
		Low:  pb.Low,
		High: pb.High,
	}
}

func (c ChangeFeedID) ToPB() *heartbeatpb.ChangefeedID {
	return &heartbeatpb.ChangefeedID{
		Low:      c.Id.Low,
		High:     c.Id.High,
		Name:     c.Name(),
		Keyspace: c.Keyspace(),
	}
}

const changeFeedIDMaxLen = 128

var changeFeedIDRe = regexp.MustCompile(`^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$`)

// ValidateChangefeedID returns true if the changefeed ID matches
// the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", length no more than "changeFeedIDMaxLen", eg, "simple-changefeed-task".
func ValidateChangefeedID(changefeedID string) error {
	if !changeFeedIDRe.MatchString(changefeedID) || len(changefeedID) > changeFeedIDMaxLen {
		return errors.ErrInvalidChangefeedID.GenWithStackByArgs(changeFeedIDMaxLen)
	}
	return nil
}

// ValidateKeyspace use the naming rules of TiDB to check the validation of the keyspace
func ValidateKeyspace(keyspace string) error {
	return errors.Trace(naming.CheckKeyspaceName(keyspace))
}

func NewChangefeedID4Test(keyspace, name string) ChangeFeedID {
	return NewChangeFeedIDWithDisplayName(ChangeFeedDisplayName{
		Name:     name,
		Keyspace: keyspace,
	})
}

type SinkType int

const (
	MysqlSinkType SinkType = iota
	KafkaSinkType
	PulsarSinkType
	CloudStorageSinkType
	BlackHoleSinkType
	RedoSinkType
)

type RowType byte

const (
	// RowTypeDelete represents a delete row.
	RowTypeDelete RowType = iota
	// RowTypeInsert represents a insert row.
	RowTypeInsert
	// RowTypeUpdate represents a update row.
	RowTypeUpdate
)

func (r RowType) String() string {
	switch r {
	case RowTypeDelete:
		return "delete"
	case RowTypeInsert:
		return "insert"
	case RowTypeUpdate:
		return "update"
	default:
	}
	log.Panic("RowType: invalid row type", zap.Uint8("rowType", uint8(r)))
	return ""
}

const (
	DefaultMode int64 = iota
	RedoMode
)

func IsDefaultMode(mode int64) bool {
	return mode == DefaultMode
}

func IsRedoMode(mode int64) bool {
	return mode == RedoMode
}

func GetModeBySinkType(sinkType SinkType) int64 {
	if sinkType == RedoSinkType {
		return RedoMode
	}
	return DefaultMode
}

func StringMode(mode int64) string {
	if mode == RedoMode {
		return "redo"
	}
	return "default"
}
