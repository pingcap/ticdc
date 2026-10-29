// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package store

import (
	"fmt"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

const PageSize = 256

type Task struct {
	Range  protocol.Span      `json:"range"`
	State  string             `json:"state"`
	Export protocol.ObjectRef `json:"export"`
	Lease  uint64             `json:"lease"`
	Rows   uint64             `json:"rows"`
	Ack    string             `json:"ack,omitempty"`
}
type Page struct {
	Job        string `json:"job"`
	PlanDigest string `json:"plan_digest"`
	Epoch      uint64 `json:"epoch"`
	Tasks      []Task `json:"tasks"`
}
type Tasks struct {
	Objects Local
	Job     string
	Digest  string
	Epoch   uint64
	Plan    protocol.Plan
}

func (t Tasks) key(page int) string { return fmt.Sprintf("cdc/%s/tasks/%06d.json", t.Job, page) }
func (t Tasks) Init() error {
	for start := 0; start < len(t.Plan.Ranges); start += PageSize {
		page := Page{Job: t.Job, PlanDigest: t.Digest, Epoch: t.Epoch}
		end := min(start+PageSize, len(t.Plan.Ranges))
		for _, r := range t.Plan.Ranges[start:end] {
			page.Tasks = append(page.Tasks, Task{Range: r, State: "WAIT_SOURCE"})
		}
		_, e := t.Objects.CAS(t.key(start/PageSize), "", page)
		if e != nil {
			return e
		}
		e = t.edit(start/PageSize, func(p *Page) error {
			if p.Epoch > t.Epoch {
				return protocol.Invalid("stale task owner")
			}
			p.Epoch = t.Epoch
			for i := range p.Tasks {
				if p.Tasks[i].State == "APPLYING" {
					p.Tasks[i].State = "READY"
				}
			}
			return nil
		}, true)
		if e != nil {
			return e
		}
	}
	return nil
}

func (t Tasks) edit(index int, fn func(*Page) error, adopt bool) error {
	for retry := 0; retry < 20; retry++ {
		var p Page
		v, e := t.Objects.Read(t.key(index), &p)
		if e != nil {
			return e
		}
		if v == "" || p.Job != t.Job || p.PlanDigest != t.Digest || !adopt && p.Epoch != t.Epoch {
			return protocol.Invalid("task page identity/epoch mismatch")
		}
		if e = fn(&p); e != nil {
			return e
		}
		ok, e := t.Objects.CAS(t.key(index), v, p)
		if e != nil {
			return e
		}
		if ok {
			return nil
		}
	}
	return protocol.Invalid("task CAS contention")
}

func (t Tasks) locate(id string) (int, int, error) {
	for i, r := range t.Plan.Ranges {
		if r.RangeID == id {
			return i / PageSize, i % PageSize, nil
		}
	}
	return 0, 0, protocol.Invalid("unknown range %s", id)
}

func (t Tasks) Ready(id string, ref protocol.ObjectRef) error {
	p, i, e := t.locate(id)
	if e != nil {
		return e
	}
	return t.edit(p, func(p *Page) error {
		x := &p.Tasks[i]
		if x.State != "WAIT_SOURCE" {
			if x.Export != ref {
				return protocol.Invalid("contradictory ready export")
			}
			return nil
		}
		x.Export = ref
		x.State = "READY"
		return nil
	}, false)
}

func (t Tasks) Assign(id string) (Task, error) {
	var out Task
	p, i, e := t.locate(id)
	if e != nil {
		return out, e
	}
	e = t.edit(p, func(p *Page) error {
		x := &p.Tasks[i]
		if x.State != "READY" {
			return protocol.Invalid("range is not ready")
		}
		x.Lease++
		x.State = "APPLYING"
		out = *x
		return nil
	}, false)
	return out, e
}

func (t Tasks) Commit(id string, lease, rows uint64) error {
	p, i, e := t.locate(id)
	if e != nil {
		return e
	}
	return t.edit(p, func(p *Page) error {
		x := &p.Tasks[i]
		if x.Lease != lease {
			return protocol.Invalid("stale receipt")
		}
		if x.State == "APPLIED" && x.Rows == rows {
			return nil
		}
		if x.State != "APPLYING" {
			return protocol.Invalid("receipt without assignment")
		}
		x.State = "APPLIED"
		x.Rows = rows
		x.Ack = "kafka-broker-ack"
		return nil
	}, false)
}

func (t Tasks) List() ([]Task, error) {
	var tasks []Task
	for i := 0; i < len(t.Plan.Ranges); i += PageSize {
		var p Page
		v, e := t.Objects.Read(t.key(i/PageSize), &p)
		if e != nil {
			return nil, e
		}
		if v == "" || p.Epoch != t.Epoch || p.PlanDigest != t.Digest {
			return nil, protocol.Invalid("invalid task page")
		}
		tasks = append(tasks, p.Tasks...)
	}
	return tasks, nil
}
