// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/ticdc/pkg/snapshot/provider"
	"github.com/pingcap/ticdc/pkg/snapshot/store"
	"golang.org/x/sync/errgroup"
)

type Controller struct {
	ID         common.ChangeFeedID
	Epoch      uint64
	TS         uint64
	Keyspace   uint32
	Config     *protocol.Config
	Repository Repository
	Capture    Capture
	Freeze     func() (SchemaBundle, []protocol.Span, error)
}

func (c Controller) Run(ctx context.Context) (result error) {
	if err := c.Config.ValidateRuntime(); err != nil {
		return err
	}
	objects := store.Local{Root: c.Config.ArtifactDir}
	dir := filepath.Join(objects.Root, "cdc", c.ID.ID().String())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return protocol.Wrap(err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, "writer.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return protocol.Wrap(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return protocol.Invalid("previous snapshot writer has not drained")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	// An aborted writer must stop actual sink mutation before a new owner can
	// acquire the lock. Process death releases flock; old process cannot retry.
	defer func() {
		if result != nil {
			c.Capture.AbortSnapshot(c.ID)
		}
	}()
	state, err := c.Repository.LoadSnapshot(ctx, c.ID, c.Epoch)
	if err != nil {
		return err
	}
	if state != nil && state.Marker != nil {
		return nil
	}
	if state == nil {
		gen := uuid.NewString()
		state = &protocol.State{Generation: gen, SnapshotID: gen, SnapshotTS: c.TS, Phase: "PREPARING"}
		if err = c.Repository.SaveSnapshot(ctx, c.ID, c.Epoch, state); err != nil {
			return err
		}
	}
	save := func() error { return c.Repository.SaveSnapshot(ctx, c.ID, c.Epoch, state) }
	if state.SpecRef.URI == "" {
		bundle, spans, err := c.Freeze()
		if err != nil {
			return err
		}
		schema, err := objects.Put("cdc/"+state.Generation+"/schema.json", bundle)
		if err != nil {
			return err
		}
		spec := protocol.Spec{ProtocolVersion: 1, JobID: state.Generation, SnapshotID: state.SnapshotID, SnapshotTS: c.TS, KeyspaceID: c.Keyspace, BackupURI: c.Config.BackupURI, BackupDigest: c.Config.BackupDigest, SelectedSpans: spans, SchemaRef: schema}
		state.SpecRef, err = objects.Put("cdc/"+state.Generation+"/spec.json", spec)
		if err != nil {
			return err
		}
		state.Phase = "PLANNING"
		if err = save(); err != nil {
			return err
		}
	}
	var spec protocol.Spec
	if err = protocol.Read(state.SpecRef, &spec); err != nil {
		return err
	}
	client := provider.Client{URL: c.Config.ProviderURL}
	var job provider.Job
	if err = client.Call(ctx, "ensure", provider.Request{SpecRef: state.SpecRef}, &job); err != nil {
		return err
	}
	wait := func() error {
		select {
		case <-ctx.Done():
			return protocol.Wrap(ctx.Err())
		case <-time.After(200 * time.Millisecond):
			return nil
		}
	}
	for job.PlanRef.URI == "" {
		if job.Phase == "FAILED" || job.Phase == "CANCELED" {
			return protocol.Invalid("export failed: %s", job.Error)
		}
		if err = wait(); err != nil {
			return err
		}
		if err = client.Call(ctx, "status", provider.Request{JobID: spec.JobID}, &job); err != nil {
			return err
		}
	}
	var plan protocol.Plan
	if err = protocol.Read(job.PlanRef, &plan); err != nil {
		return err
	}
	if err = protocol.ValidatePlan(spec, plan); err != nil {
		return err
	}
	if state.PlanRef.URI != "" && state.PlanRef != job.PlanRef {
		return protocol.Invalid("accepted plan changed")
	}
	state.PlanRef = job.PlanRef
	state.Total = len(plan.Ranges)
	state.Barrier = true
	state.Phase = "APPLYING"
	if err = save(); err != nil {
		return err
	}
	tasks := store.Tasks{Objects: objects, Job: spec.JobID, Digest: job.PlanRef.Digest, Epoch: c.Epoch, Plan: plan}
	if err = tasks.Init(); err != nil {
		return err
	}
	complete := false
	for {
		all, err := tasks.List()
		if err != nil {
			return err
		}
		applied := 0
		var ready []store.Task
		for _, task := range all {
			if task.State == "APPLIED" {
				applied++
			} else if task.State == "READY" {
				ready = append(ready, task)
			}
		}
		if err = c.applyReady(ctx, tasks, spec, state.PlanRef, ready); err != nil {
			return err
		}
		applied += len(ready)
		state.Applied = applied
		if err = save(); err != nil {
			return err
		}
		if complete && applied == len(plan.Ranges) {
			break
		}
		var page provider.Page
		if err = client.Call(ctx, "list", provider.Request{JobID: spec.JobID, Cursor: state.Cursor, Limit: 64}, &page); err != nil {
			return err
		}
		for _, ref := range page.Exports {
			var ex protocol.Export
			if err = protocol.Read(ref, &ex); err != nil {
				return err
			}
			found := false
			for _, r := range plan.Ranges {
				if r.RangeID == ex.Range.RangeID {
					found = true
					if err = protocol.ValidateExport(spec, state.PlanRef, r, ex); err != nil {
						return err
					}
					break
				}
			}
			if !found {
				return protocol.Invalid("provider returned unknown range")
			}
			if err = tasks.Ready(ex.Range.RangeID, ref); err != nil {
				return err
			}
		}
		// Task pages are durable before advancing the runtime cursor.
		state.Cursor = page.Cursor
		complete = page.Complete
		if err = save(); err != nil {
			return err
		}
		if len(page.Exports) == 0 {
			if err = client.Call(ctx, "status", provider.Request{JobID: spec.JobID}, &job); err != nil {
				return err
			}
			if job.Phase == "FAILED" || job.Phase == "CANCELED" {
				return protocol.Invalid("source job failed: %s", job.Error)
			}
			if err = wait(); err != nil {
				return err
			}
		}
	}
	state.Phase = "DRAINING"
	if err = save(); err != nil {
		return err
	}
	if err = c.Capture.DrainSnapshot(ctx, c.ID, c.Epoch); err != nil {
		return err
	}
	all, err := tasks.List()
	if err != nil {
		return err
	}
	for i, t := range all {
		if !protocol.SameSpan(t.Range, plan.Ranges[i]) || t.State != "APPLIED" {
			return protocol.Invalid("incomplete exact range receipts")
		}
	}
	state.Marker = &protocol.Marker{SnapshotTS: spec.SnapshotTS, SnapshotID: spec.SnapshotID, PlanDigest: state.PlanRef.Digest, OutputFence: "single-capture-all-broker-acks"}
	state.Phase = "SNAPSHOT_COMMITTED"
	return save()
}

// All workers are joined before Run can abort the sink or release writer.lock.
// Task page writes stay serialized; data application is bounded and concurrent.
func (c Controller) applyReady(ctx context.Context, tasks store.Tasks, spec protocol.Spec, ref protocol.ObjectRef, ready []store.Task) error {
	if len(ready) == 0 {
		return nil
	}
	group, ctx := errgroup.WithContext(ctx)
	jobs := make(chan store.Task)
	var taskMu sync.Mutex
	group.Go(func() error {
		defer close(jobs)
		for _, task := range ready {
			select {
			case jobs <- task:
			case <-ctx.Done():
				return protocol.Wrap(ctx.Err())
			}
		}
		return nil
	})
	for i := 0; i < min(c.Config.ApplyConcurrency(), len(ready)); i++ {
		group.Go(func() error {
			for task := range jobs {
				if err := ctx.Err(); err != nil {
					return protocol.Wrap(err)
				}
				taskMu.Lock()
				assigned, err := tasks.Assign(task.Range.RangeID)
				taskMu.Unlock()
				if err != nil {
					return err
				}
				var ex protocol.Export
				if err = protocol.Read(assigned.Export, &ex); err != nil {
					return err
				}
				if err = protocol.ValidateExport(spec, ref, task.Range, ex); err != nil {
					return err
				}
				rows, err := c.Capture.ApplySnapshot(ctx, c.ID, c.Epoch, spec, ref, ex)
				if err != nil {
					return err
				}
				if err = ctx.Err(); err != nil {
					return protocol.Wrap(err)
				}
				taskMu.Lock()
				err = tasks.Commit(task.Range.RangeID, assigned.Lease, rows)
				taskMu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return group.Wait()
}
