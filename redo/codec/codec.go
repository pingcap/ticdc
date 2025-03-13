// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"encoding/binary"

	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/tinylib/msgp/msgp"
)

const (
	v1HeaderLength      int = 4
	versionPrefixLength int = 2
	versionFieldLength  int = 2

	latestVersion uint16 = 2
)

// NOTE: why we need this?
//
// Before this logic is introduced, redo log is encoded into byte slice without a version field.
// This makes it hard to extend in the future.
// However, in the old format (i.e. v1 format), the first 5 bytes are always same, which can be
// confirmed in v1/codec_gen.go. So we reuse those bytes, and add a version field in them.
var (
	versionPrefix = [versionPrefixLength]byte{0xff, 0xff}
)

// UnmarshalRedoLog unmarshals a RedoLog from the given byte slice.
func UnmarshalRedoLog(bts []byte) (r *pevent.RedoLog, o []byte, err error) {
	if len(bts) < versionPrefixLength {
		err = msgp.ErrShortBytes
		return
	}

	var version uint16 = 0
	for i := 0; i < versionPrefixLength; i++ {
		if bts[i] != versionPrefix[i] {
			version = uint16(1)
			break
		}
	}
	if version != uint16(1) {
		bts = bts[versionPrefixLength:]
		version, bts = decodeVersion(bts)
	}

	switch version {
	case 1:
		// FIXME: for redo v1, dml events need some transforms.
		if o, err = r.UnmarshalMsg(bts); err != nil {
			return
		}
	case latestVersion:
		if version == latestVersion {
			r = new(pevent.RedoLog)
			if o, err = r.UnmarshalMsg(bts); err != nil {
				return
			}
		} else {
			panic("unsupported codec version")
		}
	}
	return
}

// MarshalRedoLog marshals a RedoLog into bytes.
func MarshalRedoLog(r *pevent.RedoLog, b []byte) (o []byte, err error) {
	b = append(b, versionPrefix[:]...)
	b = binary.BigEndian.AppendUint16(b, latestVersion)
	o, err = r.MarshalMsg(b)
	return
}

// MarshalDDLAsRedoLog converts a DDLEvent into RedoLog, and then marshals it.
func MarshalDDLAsRedoLog(d *pevent.DDLEvent, b []byte) (o []byte, err error) {
	o, err = MarshalRedoLog(d.ToRedoLog(), b)
	return
}

func decodeVersion(bts []byte) (uint16, []byte) {
	version := binary.BigEndian.Uint16(bts[0:versionFieldLength])
	return version, bts[versionFieldLength:]
}
