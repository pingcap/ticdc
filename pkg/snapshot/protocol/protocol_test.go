// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoverageAndStableIdentity(t *testing.T) {
	spec := Spec{JobID: "j", SnapshotID: "s", SnapshotTS: 1, SelectedSpans: []Span{{TableID: 1, Start: []byte("a"), End: []byte("z")}}}
	good := Plan{ProtocolVersion: 1, JobID: "j", SnapshotID: "s", SnapshotTS: 1, Ranges: []Span{{"r0", 1, []byte("a"), []byte("m")}, {"r1", 1, []byte("m"), []byte("z")}}}
	require.NoError(t, ValidatePlan(spec, good))
	for _, r := range []Span{{"r1", 1, []byte("n"), []byte("z")}, {"r1", 1, []byte("l"), []byte("z")}, {"r0", 1, []byte("m"), []byte("z")}, {"r1", 2, []byte("m"), []byte("z")}} {
		p := good
		p.Ranges = append([]Span(nil), good.Ranges...)
		p.Ranges[1] = r
		require.Error(t, ValidatePlan(spec, p))
	}
	require.Equal(t, RecordID("s", 1, []byte("a")), RecordID("s", 1, []byte("a")))
	require.NotEqual(t, RecordID("s", 1, []byte("a")), RecordID("s", 2, []byte("a")))
	require.NotEqual(t, RecordID("ab", 1, []byte("c")), RecordID("a", 1, []byte("bc")))
}

func TestRustGoldenAndCorruption(t *testing.T) {
	path := "testdata/rust-v1.skv"
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	expected := Header{Version: 1, Compression: "none", JobID: "golden", SnapshotID: "golden", SnapshotTS: 42, PlanDigest: "golden", RangeID: "r0", AttemptID: "a0", TableID: 1}
	ref := Chunk{KVCount: 2, LogicalBytes: 12, First: []byte("a"), Last: []byte("b")}
	span := Span{Start: []byte("a"), End: []byte("z")}
	var got []string
	footer, err := decode(bytes.NewReader(b), expected, span, ref, func(k, v []byte) error { got = append(got, string(k)); return nil })
	require.NoError(t, err)
	require.Equal(t, uint64(2), footer.KVCount)
	require.Equal(t, []string{"a", "b"}, got)
	for _, bad := range [][]byte{b[:len(b)-1], append(append([]byte(nil), b...), 0), append([]byte("BADMAGIC"), b[8:]...)} {
		_, err := decode(bytes.NewReader(bad), expected, span, ref, func([]byte, []byte) error { return nil })
		require.Error(t, err)
	}
	bad := append([]byte(nil), b...)
	binary.LittleEndian.PutUint32(bad[8:12], ^uint32(0))
	_, err = decode(bytes.NewReader(bad), expected, span, ref, func([]byte, []byte) error { return nil })
	require.Error(t, err)
	f := filepath.Join(t.TempDir(), "chunk")
	require.NoError(t, os.WriteFile(f, b, 0o600))
	ref.ObjectRef, err = Ref(f)
	require.NoError(t, err)
	bad = append([]byte(nil), b...)
	bad[len(bad)/2] ^= 1
	require.NoError(t, os.WriteFile(f, bad, 0o600))
	called := false
	_, err = DecodeChunk(ref, expected, span, func([]byte, []byte) error { called = true; return nil })
	require.Error(t, err)
	require.False(t, called)
}

func TestStateRoundTrip(t *testing.T) {
	state := State{Generation: "g", Phase: "SNAPSHOT_COMMITTED", Marker: &Marker{SnapshotTS: 42}}
	b, e := json.Marshal(state)
	require.NoError(t, e)
	var out State
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, state, out)
}
