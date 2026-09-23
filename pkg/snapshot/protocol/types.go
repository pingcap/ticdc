// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/pingcap/ticdc/pkg/errors"
)

func Invalid(format string, args ...any) error {
	return errors.ErrSnapshotBootstrap.GenWithStackByArgs(fmt.Sprintf(format, args...))
}

func Wrap(err error) error {
	if err == nil {
		return nil
	}
	return errors.WrapError(errors.ErrSnapshotBootstrap, err, "I/O or encoding")
}

type ObjectRef struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}
type Span struct {
	RangeID string `json:"range_id,omitempty"`
	TableID int64  `json:"physical_table_id"`
	Start   []byte `json:"start_key"`
	End     []byte `json:"end_key"`
}
type Spec struct {
	ProtocolVersion int       `json:"protocol_version"`
	JobID           string    `json:"job_id"`
	SnapshotID      string    `json:"snapshot_id"`
	SnapshotTS      uint64    `json:"snapshot_ts"`
	KeyspaceID      uint32    `json:"keyspace_id"`
	BackupURI       string    `json:"backup_uri"`
	BackupDigest    string    `json:"backup_digest"`
	SelectedSpans   []Span    `json:"selected_spans"`
	SchemaRef       ObjectRef `json:"schema_ref"`
}
type Plan struct {
	ProtocolVersion int    `json:"protocol_version"`
	JobID           string `json:"job_id"`
	SnapshotID      string `json:"snapshot_id"`
	SnapshotTS      uint64 `json:"snapshot_ts"`
	Ranges          []Span `json:"ranges"`
}
type Chunk struct {
	ObjectRef
	KVCount      uint64 `json:"kv_count"`
	LogicalBytes uint64 `json:"logical_bytes"`
	First        []byte `json:"first_key"`
	Last         []byte `json:"last_key"`
}
type Export struct {
	ProtocolVersion int     `json:"protocol_version"`
	JobID           string  `json:"job_id"`
	SnapshotID      string  `json:"snapshot_id"`
	SnapshotTS      uint64  `json:"snapshot_ts"`
	PlanDigest      string  `json:"plan_digest"`
	Range           Span    `json:"range"`
	AttemptID       string  `json:"attempt_id"`
	Chunks          []Chunk `json:"chunks"`
	KVCount         uint64  `json:"kv_count"`
}
type Header struct {
	Version     int    `json:"chunk_format_version"`
	Compression string `json:"compression"`
	JobID       string `json:"job_id"`
	SnapshotID  string `json:"snapshot_id"`
	SnapshotTS  uint64 `json:"snapshot_ts"`
	PlanDigest  string `json:"plan_digest"`
	RangeID     string `json:"range_id"`
	AttemptID   string `json:"attempt_id"`
	TableID     int64  `json:"physical_table_id"`
	Seq         uint64 `json:"chunk_seq"`
}
type Footer struct {
	Blocks       uint64 `json:"block_count"`
	KVCount      uint64 `json:"kv_count"`
	LogicalBytes uint64 `json:"logical_bytes"`
	First        []byte `json:"first_key"`
	Last         []byte `json:"last_key"`
}

// State is the optional snapshot child of ChangeFeedStatus, never a second job key.
type State struct {
	Generation string    `json:"generation"`
	SnapshotID string    `json:"snapshot_id"`
	SnapshotTS uint64    `json:"snapshot_ts"`
	Phase      string    `json:"phase"`
	SpecRef    ObjectRef `json:"spec_ref"`
	PlanRef    ObjectRef `json:"plan_ref"`
	Cursor     string    `json:"ready_cursor"`
	Barrier    bool      `json:"sink_bootstrap_barrier"`
	Applied    int       `json:"applied"`
	Total      int       `json:"total"`
	Marker     *Marker   `json:"global_marker,omitempty"`
}
type Marker struct {
	SnapshotTS  uint64 `json:"snapshot_ts"`
	SnapshotID  string `json:"snapshot_id"`
	PlanDigest  string `json:"plan_digest"`
	OutputFence string `json:"output_fence"`
}

// Config is opt-in. The initial deployment uses one capture and a shared local
// artifact directory. Provider and capture are independent modules.
type Config struct {
	ApplyWorkers  int    `toml:"apply-workers" json:"apply-workers,omitempty"`
	InflightBytes int64  `toml:"inflight-bytes" json:"inflight-bytes,omitempty"`
	BackupURI     string `toml:"backup-uri" json:"backup-uri"`
	BackupDigest  string `toml:"backup-digest" json:"backup-digest"`
	ProviderURL   string `toml:"provider-url" json:"provider-url"`
	ArtifactDir   string `toml:"artifact-dir" json:"artifact-dir"`
}

// ApplyConcurrency and InflightLimit bound capture-side work independently of
// provider export workers. The byte budget covers mounted data awaiting ACK;
// each worker additionally owns at most one bounded input chunk and batch.
func (c *Config) ApplyConcurrency() int {
	if c == nil || c.ApplyWorkers == 0 {
		return 4
	}
	return c.ApplyWorkers
}
func (c *Config) InflightLimit() int64 {
	if c == nil || c.InflightBytes == 0 {
		return 64 << 20
	}
	return c.InflightBytes
}
func (c *Config) ValidateRuntime() error {
	if c.ApplyConcurrency() < 1 || c.ApplyConcurrency() > 32 {
		return Invalid("apply-workers must be between 1 and 32")
	}
	if c.InflightLimit() < 32<<20 || c.InflightLimit() > 1<<30 {
		return Invalid("inflight-bytes must be between 32 MiB and 1 GiB")
	}
	return nil
}

func Digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Ref(path string) (ObjectRef, error) {
	f, e := os.Open(path)
	if e != nil {
		return ObjectRef{}, Wrap(e)
	}
	defer f.Close()
	h := sha256.New()
	n, e := io.Copy(h, f)
	return ObjectRef{path, hex.EncodeToString(h.Sum(nil)), n}, Wrap(e)
}

func Read(ref ObjectRef, dst any) error {
	if ref.Size < 0 || ref.Size > 4<<20 {
		return Invalid("metadata size exceeds 4 MiB")
	}
	f, e := os.Open(ref.URI)
	if e != nil {
		return Wrap(e)
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	if e != nil {
		return Wrap(e)
	}
	if int64(len(b)) != ref.Size || Digest(b) != ref.Digest {
		return Invalid("metadata checksum mismatch")
	}
	return Wrap(json.Unmarshal(b, dst))
}

func SameSpan(a, b Span) bool {
	return a.RangeID == b.RangeID && a.TableID == b.TableID && bytes.Equal(a.Start, b.Start) && bytes.Equal(a.End, b.End)
}

func ValidatePlan(spec Spec, p Plan) error {
	if p.ProtocolVersion != 1 || p.JobID != spec.JobID || p.SnapshotID != spec.SnapshotID || p.SnapshotTS != spec.SnapshotTS {
		return Invalid("plan identity mismatch")
	}
	spans := append([]Span(nil), spec.SelectedSpans...)
	ranges := append([]Span(nil), p.Ranges...)
	less := func(a, b Span) bool {
		if a.TableID != b.TableID {
			return a.TableID < b.TableID
		}
		return bytes.Compare(a.Start, b.Start) < 0
	}
	sort.Slice(spans, func(i, j int) bool { return less(spans[i], spans[j]) })
	sort.Slice(ranges, func(i, j int) bool { return less(ranges[i], ranges[j]) })
	ids := map[string]bool{}
	idx := 0
	for i, s := range spans {
		if len(s.Start) == 0 || bytes.Compare(s.Start, s.End) >= 0 {
			return Invalid("invalid selection")
		}
		if i > 0 && s.TableID == spans[i-1].TableID && bytes.Compare(spans[i-1].End, s.Start) > 0 {
			return Invalid("overlapping selected spans")
		}
		next := s.Start
		for idx < len(ranges) && ranges[idx].TableID == s.TableID && bytes.Compare(ranges[idx].Start, s.End) < 0 {
			r := ranges[idx]
			if r.RangeID == "" || ids[r.RangeID] || !bytes.Equal(r.Start, next) || bytes.Compare(r.Start, r.End) >= 0 || bytes.Compare(r.End, s.End) > 0 {
				return Invalid("coverage hole, overlap, or duplicate range")
			}
			ids[r.RangeID] = true
			next = r.End
			idx++
		}
		if !bytes.Equal(next, s.End) {
			return Invalid("incomplete coverage")
		}
	}
	if idx != len(ranges) {
		return Invalid("unselected range")
	}
	return nil
}

func ValidateExport(spec Spec, plan ObjectRef, r Span, e Export) error {
	if e.ProtocolVersion != 1 || e.JobID != spec.JobID || e.SnapshotID != spec.SnapshotID || e.SnapshotTS != spec.SnapshotTS || e.PlanDigest != plan.Digest || !SameSpan(r, e.Range) || e.AttemptID == "" {
		return Invalid("export identity mismatch")
	}
	var count uint64
	var last []byte
	for _, c := range e.Chunks {
		if c.Size <= 0 || c.Size > 64<<20 || c.KVCount == 0 || len(c.Digest) != 64 || bytes.Compare(c.First, r.Start) < 0 || bytes.Compare(c.First, c.Last) > 0 || bytes.Compare(c.Last, r.End) >= 0 || last != nil && bytes.Compare(last, c.First) >= 0 {
			return Invalid("invalid chunk bounds or size")
		}
		last = c.Last
		count += c.KVCount
	}
	if count != e.KVCount {
		return Invalid("export count mismatch")
	}
	return nil
}

func RecordID(snapshot string, table int64, key []byte) string {
	h := sha256.New()
	h.Write([]byte("snapshot-key-v1\x00"))
	var b [8]byte
	binary.BigEndian.PutUint32(b[:4], uint32(len(snapshot)))
	h.Write(b[:4])
	h.Write([]byte(snapshot))
	binary.BigEndian.PutUint64(b[:], uint64(table))
	h.Write(b[:])
	binary.BigEndian.PutUint32(b[:4], uint32(len(key)))
	h.Write(b[:4])
	h.Write(key)
	return hex.EncodeToString(h.Sum(nil))
}
