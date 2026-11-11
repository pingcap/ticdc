// Copyright 2026 PingCAP, Inc.
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

// Package metering uploads caller-defined records through Metering SDK.
package metering

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering/traffic"
	"go.uber.org/zap"
)

const (
	reportCategory = "ticdc"
	writeTimeout   = 10 * time.Second
	writeAttempts  = 3
)

// Reporter uploads caller-provided changefeed records synchronously. It owns no collection
// loop or pending queue. The caller retains responsibility for durable state and
// scheduling, including recovery after an unsuccessful Report.
type Reporter struct {
	mu     sync.Mutex
	inner  *meteringwriter.MeteringWriter
	closed bool
}

// NewReporter initializes the SDK without starting a goroutine. It reuses the
// SDK metering configuration, including provider-specific fields and
// credentials, and does not interpret them itself. The owner must call Close
// after all Report calls have returned.
func NewReporter(c *config.MeteringConfig) (*Reporter, error) {
	if err := ValidateConfig(c); err != nil {
		return nil, err
	}
	if c == nil || c.Type == "" {
		return nil, errors.ErrInvalidServerOption.GenWithStack("metering: destination is required")
	}
	provider, err := storage.NewObjectStorageProvider(c.ToProviderConfig())
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	return &Reporter{
		inner: meteringwriter.NewMeteringWriterFromConfig(provider, config.DefaultConfig().WithOverwriteExisting(true), c),
	}, nil
}

// Report groups changefeed records by keyspace and uploads one object per keyspace.
// It stops at the first error.
// Calls are serialized; callers must not mutate records until Report returns.
// Retries preserve the path and contents. Same-minute reports overwrite existing objects.
func (r *Reporter) Report(ctx context.Context, records []traffic.ChangefeedMetering) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.ErrMeteringReporterClosed.GenWithStackByArgs()
	}
	if len(records) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	var keyspaces []traffic.KeyspaceMetering
	keyspaceIndex := make(map[uint32]int)
	for _, record := range records {
		name := record.ChangefeedID.Keyspace()
		if name == "" {
			return errors.ErrMeteringInvalidBatch.GenWithStackByArgs("keyspace name must not be empty")
		}
		index, ok := keyspaceIndex[record.KeyspaceID]
		if !ok {
			index = len(keyspaces)
			keyspaceIndex[record.KeyspaceID] = index
			keyspaces = append(keyspaces, traffic.KeyspaceMetering{KeyspaceID: record.KeyspaceID, KeyspaceName: name})
		}
		keyspaces[index].Changefeeds = append(keyspaces[index].Changefeeds, record)
	}
	ts := time.Now().Truncate(time.Minute).Unix()
	for _, record := range keyspaces {
		data := &common.MeteringData{
			SelfID:    strconv.FormatUint(uint64(record.KeyspaceID), 10),
			Timestamp: ts,
			Category:  reportCategory,
			Data: []map[string]any{{
				"keyspace_name": record.KeyspaceName,
				"changefeeds":   record.Changefeeds,
			}},
		}
		if err := r.write(ctx, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reporter) write(ctx context.Context, data *common.MeteringData) error {
	var err error
	for attempt := range writeAttempts {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(attempt) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.WrapError(errors.ErrExternalStorageAPI, ctx.Err())
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, err)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = r.inner.Write(attemptCtx, data)
		cancel()
		if err == nil {
			break
		}
	}
	return errors.WrapError(errors.ErrExternalStorageAPI, err)
}

// Close releases the SDK after an in-flight Report finishes. It is idempotent.
// It does not cancel an in-flight Report; cancel its context first if shutdown
// needs to be bounded.
func (r *Reporter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		if err := r.inner.Close(); err != nil {
			log.Warn("metering writer close failed", zap.Error(errors.WrapError(errors.ErrExternalStorageAPI, err)))
		}
	}
}
