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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const (
	reportInterval = time.Minute
	writeTimeout   = 30 * time.Second
	writeAttempts  = 6
)

// Batch is one caller-defined metering batch. The reporter adds only SDK batch
// metadata; it does not interpret or aggregate the records.
type Batch struct {
	// Category is the SDK category and must be a single path component.
	Category string
	// Timestamp selects the SDK minute directory. It is truncated to the minute.
	Timestamp time.Time
	// Records are serialized as provided. The caller must not mutate them until
	// Report returns.
	Records []map[string]any
}

// Reporter uploads caller-provided batches synchronously. It owns no collection
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
		// SDK defaults disable pagination and refuse overwrites.
		inner: meteringwriter.NewMeteringWriterFromConfig(provider, config.DefaultConfig(), c),
	}, nil
}

// Report uploads one batch synchronously. Retries within this call preserve
// batch identity and contents. Separate calls create separate objects, even with
// identical inputs. A nil error means the SDK reported a successful upload.
//
// A non-nil error means success was not confirmed; it is not proof that nothing
// was written. If an upload response is lost, a retry can find the object and
// return writer.ErrFileExists, which does not compare the existing contents.
// Callers must treat such errors as indeterminate and reconcile from their own
// durable state. Success does not acknowledge producer state.
//
// The caller's context bounds the whole call, including retries and backoff.
// Concurrent calls are serialized. After Close, Report returns
// errors.ErrMeteringReporterClosed.
func (r *Reporter) Report(ctx context.Context, batch Batch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.ErrMeteringReporterClosed.GenWithStackByArgs()
	}
	if len(batch.Records) == 0 {
		return nil
	}
	if !validPathComponent(batch.Category) {
		return errors.ErrMeteringInvalidBatch.GenWithStackByArgs(
			"category must be a single path component")
	}
	ts := batch.Timestamp.Truncate(reportInterval).Unix()
	if ts <= 0 {
		return errors.ErrMeteringInvalidBatch.GenWithStackByArgs("timestamp must be positive")
	}
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	data := &common.MeteringData{
		SelfID:    "ticdc" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		Timestamp: ts,
		Category:  batch.Category,
		Data:      batch.Records,
	}
	err := retry.Do(ctx, func() error {
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		defer cancel()
		return r.inner.Write(attemptCtx, data)
	}, retry.WithMaxTries(writeAttempts),
		retry.WithBackoffBaseDelay(1000), retry.WithBackoffMaxDelay(10000),
		retry.WithIsRetryableErr(func(err error) bool {
			// Existence alone cannot prove that this payload was uploaded.
			return !errors.Is(err, writer.ErrFileExists)
		}))
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
