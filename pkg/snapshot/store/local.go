// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

// Local is the single-host demo ObjectCASStore. flock serializes comparisons;
// fsync + rename + directory fsync makes a successful update durable. All
// participants must share this filesystem. This is not an S3 CAS emulation.
type Local struct{ Root string }

func (s Local) Path(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) || strings.Contains(key, "..") {
		return "", protocol.Invalid("invalid object key")
	}
	return filepath.Join(s.Root, key), nil
}

func (s Local) Read(key string, dst any) (string, error) {
	p, e := s.Path(key)
	if e != nil {
		return "", e
	}
	b, e := os.ReadFile(p)
	if os.IsNotExist(e) {
		return "", nil
	}
	if e != nil {
		return "", protocol.Wrap(e)
	}
	return protocol.Digest(b), protocol.Wrap(json.Unmarshal(b, dst))
}

func (s Local) CAS(key, expected string, value any) (bool, error) {
	p, e := s.Path(key)
	if e != nil {
		return false, e
	}
	if e = os.MkdirAll(filepath.Dir(p), 0o700); e != nil {
		return false, protocol.Wrap(e)
	}
	lock, e := os.OpenFile(p+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if e != nil {
		return false, protocol.Wrap(e)
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return false, protocol.Wrap(e)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	old, e := os.ReadFile(p)
	if e != nil && !os.IsNotExist(e) {
		return false, protocol.Wrap(e)
	}
	version := ""
	if e == nil {
		version = protocol.Digest(old)
	}
	if version != expected {
		return false, nil
	}
	b, e := json.Marshal(value)
	if e != nil {
		return false, protocol.Wrap(e)
	}
	f, e := os.CreateTemp(filepath.Dir(p), ".cas-")
	if e != nil {
		return false, protocol.Wrap(e)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return false, protocol.Wrap(e)
	}
	if e = f.Sync(); e != nil {
		return false, protocol.Wrap(e)
	}
	if e = f.Close(); e != nil {
		return false, protocol.Wrap(e)
	}
	if e = os.Rename(f.Name(), p); e != nil {
		return false, protocol.Wrap(e)
	}
	dir, e := os.Open(filepath.Dir(p))
	if e != nil {
		return false, protocol.Wrap(e)
	}
	defer dir.Close()
	return true, protocol.Wrap(dir.Sync())
}

func (s Local) Put(key string, value any) (protocol.ObjectRef, error) {
	ok, e := s.CAS(key, "", value)
	if e != nil {
		return protocol.ObjectRef{}, e
	}
	p, e := s.Path(key)
	if e != nil {
		return protocol.ObjectRef{}, e
	}
	if !ok {
		b, e := os.ReadFile(p)
		if e != nil {
			return protocol.ObjectRef{}, protocol.Wrap(e)
		}
		wanted, e := json.Marshal(value)
		if e != nil {
			return protocol.ObjectRef{}, protocol.Wrap(e)
		}
		if protocol.Digest(b) != protocol.Digest(wanted) {
			return protocol.ObjectRef{}, protocol.Invalid("immutable object conflict")
		}
	}
	return protocol.Ref(p)
}
