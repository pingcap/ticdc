// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
// Package provider implements BR's export controller. It never decodes rows or
// opens a sink. The local Runner is confined to this package/process.
package provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/ticdc/pkg/snapshot/store"
	"golang.org/x/sync/errgroup"
)

type Job struct {
	Spec    protocol.Spec        `json:"spec"`
	SpecRef protocol.ObjectRef   `json:"spec_ref"`
	Phase   string               `json:"phase"`
	PlanRef protocol.ObjectRef   `json:"plan_ref"`
	Exports []protocol.ObjectRef `json:"exports"`
	Error   string               `json:"error,omitempty"`
}
type Page struct {
	Exports  []protocol.ObjectRef `json:"exports"`
	Cursor   string               `json:"next_cursor"`
	Complete bool                 `json:"source_complete"`
}
type Request struct {
	JobID   string             `json:"job_id"`
	SpecRef protocol.ObjectRef `json:"spec_ref"`
	Cursor  string             `json:"cursor"`
	Limit   int                `json:"limit"`
}
type Server struct {
	Objects store.Local
	Binary  string
	// Workers bounds concurrent local materialize-range processes per job.
	// Configure before serving requests; values below one retain serial execution.
	Workers int
	mu      sync.Mutex
	running map[string]context.CancelFunc
}

func New(root, binary string) *Server {
	return &Server{Objects: store.Local{Root: root}, Binary: binary, running: map[string]context.CancelFunc{}}
}
func key(id string) string { return "br/" + id + "/control.json" }
func validID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

func (s *Server) update(id string, fn func(*Job) error) error {
	for retry := 0; retry < 30; retry++ {
		var j Job
		v, e := s.Objects.Read(key(id), &j)
		if e != nil {
			return e
		}
		if v == "" {
			return protocol.Invalid("unknown job")
		}
		if e = fn(&j); e != nil {
			return e
		}
		ok, e := s.Objects.CAS(key(id), v, j)
		if e != nil {
			return e
		}
		if ok {
			return nil
		}
	}
	return protocol.Invalid("job CAS contention")
}

func (s *Server) Ensure(ref protocol.ObjectRef) (Job, error) {
	var spec protocol.Spec
	if e := protocol.Read(ref, &spec); e != nil {
		return Job{}, e
	}
	if !validID(spec.JobID) || spec.ProtocolVersion != 1 || spec.SnapshotTS == 0 || len(spec.SelectedSpans) > 256 {
		return Job{}, protocol.Invalid("invalid or oversized demo spec")
	}
	j := Job{Spec: spec, SpecRef: ref, Phase: "ACCEPTED"}
	_, e := s.Objects.CAS(key(spec.JobID), "", j)
	if e != nil {
		return j, e
	}
	_, e = s.Objects.Read(key(spec.JobID), &j)
	if e != nil {
		return j, e
	}
	if j.SpecRef != ref {
		return j, protocol.Invalid("Ensure identity conflict")
	}
	if j.Phase != "SOURCE_COMPLETE" && j.Phase != "FAILED" && j.Phase != "CANCELED" {
		s.start(spec.JobID)
	}
	return j, nil
}

func (s *Server) start(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.running[id]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.running[id] = cancel
	go func() {
		defer func() { cancel(); s.mu.Lock(); delete(s.running, id); s.mu.Unlock() }()
		if e := s.run(ctx, id); e != nil {
			_ = s.update(id, func(j *Job) error {
				if j.Phase != "CANCELED" {
					j.Phase = "FAILED"
					j.Error = e.Error()
				}
				return nil
			})
		}
	}()
}

func (s *Server) batch(ctx context.Context, id, command string, task map[string]any) (protocol.ObjectRef, error) {
	parent := filepath.Join(s.Objects.Root, "br", id, "attempts")
	if e := os.MkdirAll(parent, 0o700); e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	dir, e := os.MkdirTemp(parent, "")
	if e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	attempt := filepath.Base(dir)
	task["protocol_version"] = 1
	task["output_dir"] = dir
	task["attempt_id"] = attempt
	b, e := json.Marshal(task)
	if e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	taskFile := filepath.Join(dir, "task.json")
	if e = os.WriteFile(taskFile, b, 0o600); e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	log, e := os.Create(filepath.Join(dir, "worker.log"))
	if e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	defer log.Close()
	cmd := exec.CommandContext(ctx, s.Binary, "snapshot", command, "--task-spec", taskFile)
	cmd.Stdout = log
	cmd.Stderr = log
	started := time.Now()
	e = cmd.Run()
	// Observability only: worker resource accounting is not part of the export
	// protocol and must never turn a completed export into a failed attempt.
	if state := cmd.ProcessState; state != nil {
		stats := map[string]any{"command": command, "pid": state.Pid(), "started_at": started.UnixNano(), "finished_at": time.Now().UnixNano(), "user_cpu_seconds": state.UserTime().Seconds(), "system_cpu_seconds": state.SystemTime().Seconds(), "success": state.Success()}
		if usage, ok := state.SysUsage().(*syscall.Rusage); ok {
			peakRSS := usage.Maxrss
			if runtime.GOOS != "darwin" {
				peakRSS *= 1024
			}
			stats["peak_rss_bytes"] = peakRSS
		}
		if data, err := json.Marshal(stats); err == nil {
			_ = os.WriteFile(filepath.Join(dir, "worker-stats.json"), data, 0o600)
		}
	}
	if e != nil {
		return protocol.ObjectRef{}, protocol.Wrap(e)
	}
	name := "candidate.json"
	if command == "plan" {
		name = "plan.json"
	}
	return protocol.Ref(filepath.Join(dir, name))
}

func (s *Server) run(ctx context.Context, id string) error {
	var j Job
	if _, e := s.Objects.Read(key(id), &j); e != nil {
		return e
	}

	if j.PlanRef.URI == "" {
		if e := s.update(id, func(j *Job) error {
			if e := activeJob(j); e != nil {
				return e
			}
			j.Phase = "PLANNING"
			return nil
		}); e != nil {
			return e
		}
		ref, e := s.batch(ctx, id, "plan", map[string]any{"spec": j.Spec})
		if e != nil {
			return e
		}
		var plan protocol.Plan
		if e = protocol.Read(ref, &plan); e != nil {
			return e
		}
		if e = protocol.ValidatePlan(j.Spec, plan); e != nil {
			return e
		}
		if len(plan.Ranges) > 256 {
			return protocol.Invalid("demo supports at most 256 ranges")
		}
		if e = s.update(id, func(j *Job) error {
			if e := activeJob(j); e != nil {
				return e
			}
			if j.PlanRef.URI == "" {
				j.PlanRef = ref
			}
			j.Phase = "MATERIALIZING"
			return nil
		}); e != nil {
			return e
		}
	}
	if _, e := s.Objects.Read(key(id), &j); e != nil {
		return e
	}
	var plan protocol.Plan
	if e := protocol.Read(j.PlanRef, &plan); e != nil {
		return e
	}
	workers := s.Workers
	if workers < 1 {
		workers = 1
	}
	if e := materializeRanges(ctx, workers, plan.Ranges, func(ctx context.Context, r protocol.Span) error {
		return s.materialize(ctx, id, j, r)
	}); e != nil {
		return e
	}
	return s.update(id, func(j *Job) error {
		if e := activeJob(j); e != nil {
			return e
		}
		if len(j.Exports) != len(plan.Ranges) {
			return protocol.Invalid("source coverage mismatch")
		}
		j.Phase = "SOURCE_COMPLETE"
		return nil
	})
}

func activeJob(j *Job) error {
	if j.Phase == "CANCELED" || j.Phase == "FAILED" {
		return protocol.Invalid("terminal source job")
	}
	return nil
}

// Cancel and join all workers before the caller records a terminal job state.
func materializeRanges(ctx context.Context, workers int, ranges []protocol.Span, run func(context.Context, protocol.Span) error) error {
	group, ctx := errgroup.WithContext(ctx)
	jobs := make(chan protocol.Span)
	group.Go(func() error {
		defer close(jobs)
		for _, r := range ranges {
			select {
			case <-ctx.Done():
				return protocol.Wrap(ctx.Err())
			case jobs <- r:
			}
		}
		return nil
	})
	for i := 0; i < workers; i++ {
		group.Go(func() error {
			for r := range jobs {
				if e := ctx.Err(); e != nil {
					return protocol.Wrap(e)
				}
				if e := run(ctx, r); e != nil {
					return e
				}
			}
			return nil
		})
	}
	return group.Wait()
}

func (s *Server) materialize(ctx context.Context, id string, j Job, r protocol.Span) error {
	// A deterministic URI is the completion authority. Recover a published
	// winner before launching work, then reconcile the append-only journal.
	exportKey := "br/" + id + "/range-exports/" + r.RangeID + ".json"
	var ex protocol.Export
	v, e := s.Objects.Read(exportKey, &ex)
	if e != nil {
		return e
	}
	if v == "" {
		ref, e := s.batch(ctx, id, "materialize-range", map[string]any{"spec": j.Spec, "range": r, "plan_digest": j.PlanRef.Digest, "chunk_bytes": 4 << 20})
		if e != nil {
			return e
		}
		if e = protocol.Read(ref, &ex); e != nil {
			return e
		}
		if e = protocol.ValidateExport(j.Spec, j.PlanRef, r, ex); e != nil {
			return e
		}
		for _, chunk := range ex.Chunks {
			info, e := os.Stat(chunk.URI)
			if e != nil {
				return protocol.Wrap(e)
			}
			if info.Size() != chunk.Size {
				return protocol.Invalid("incomplete upload")
			}
		}
		_, e = s.Objects.CAS(exportKey, "", ex)
		if e != nil {
			return e
		}
		if _, e = s.Objects.Read(exportKey, &ex); e != nil {
			return e
		}
	}
	if e = protocol.ValidateExport(j.Spec, j.PlanRef, r, ex); e != nil {
		return e
	}
	path, _ := s.Objects.Path(exportKey)
	ref, e := protocol.Ref(path)
	if e != nil {
		return e
	}
	if e = s.update(id, func(j *Job) error {
		if e := activeJob(j); e != nil {
			return e
		}
		for _, old := range j.Exports {
			if old.URI == ref.URI {
				if old != ref {
					return protocol.Invalid("export winner changed")
				}
				return nil
			}
		}
		j.Exports = append(j.Exports, ref)
		return nil
	}); e != nil {
		return e
	}
	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var req Request
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); e != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	var out any
	var err error
	if r.URL.Path == "/ensure" {
		out, err = s.Ensure(req.SpecRef)
	} else if !validID(req.JobID) {
		err = protocol.Invalid("invalid job ID")
	} else {
		var j Job
		var v string
		v, err = s.Objects.Read(key(req.JobID), &j)
		if err == nil && v == "" {
			err = protocol.Invalid("unknown job")
		}
		if err == nil {
			// A Provider can restart between CDC polls without losing an HTTP
			// request. Reconcile durable work even when Ensure is not repeated.
			if (r.URL.Path == "/status" || r.URL.Path == "/list") && j.Phase != "SOURCE_COMPLETE" && j.Phase != "FAILED" && j.Phase != "CANCELED" {
				s.start(req.JobID)
			}
			switch r.URL.Path {
			case "/status":
				out = j
			case "/list":
				var offset int
				if req.Cursor != "" {
					b, e := base64.RawURLEncoding.DecodeString(req.Cursor)
					if e != nil {
						err = protocol.Wrap(e)
						break
					}
					parts := strings.Split(string(b), ":")
					if len(parts) != 3 || parts[0] != req.JobID || parts[1] != j.PlanRef.Digest {
						err = protocol.Invalid("cursor identity mismatch")
						break
					}
					offset, e = strconv.Atoi(parts[2])
					if e != nil {
						err = protocol.Wrap(e)
						break
					}
				}
				if offset < 0 || offset > len(j.Exports) {
					err = protocol.Invalid("cursor outside journal")
					break
				}
				limit := req.Limit
				if limit < 1 || limit > 256 {
					limit = 64
				}
				end := min(offset+limit, len(j.Exports))
				cursor := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s:%d", req.JobID, j.PlanRef.Digest, end)))
				out = Page{j.Exports[offset:end], cursor, j.Phase == "SOURCE_COMPLETE" && end == len(j.Exports)}
			case "/cancel":
				err = s.update(req.JobID, func(j *Job) error { j.Phase = "CANCELED"; return nil })
				s.mu.Lock()
				if cancel := s.running[req.JobID]; cancel != nil {
					cancel()
				}
				s.mu.Unlock()
				out = map[string]bool{"canceled": true}
			default:
				http.NotFound(w, r)
				return
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	_ = json.NewEncoder(w).Encode(out)
}
