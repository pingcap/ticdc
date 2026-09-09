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

// Package metering uploads traffic records through Metering SDK.
package metering

import (
	"context"
	"strconv"
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
	"go.uber.org/zap"
)

const (
	reportInterval = time.Minute
	writeTimeout   = 30 * time.Second
	writeAttempts  = 6
)

// TrafficRecord is a changefeed's metering result. The producer defines whether
// TrafficBytes represents a total or a delta; the reporter does not aggregate it.
// Additional business fields belong here when the metering contract is finalized.
type TrafficRecord struct {
	ChangefeedGID string
	TrafficBytes  uint64
}

// TrafficReporter uploads caller-provided records synchronously. It owns no
// collection loop or pending queue. The caller retains responsibility for durable
// state and scheduling, including recovery after an unsuccessful Report.
type TrafficReporter struct {
	mu       sync.Mutex
	inner    *meteringwriter.MeteringWriter
	id       string
	sequence uint64
	closed   bool
}

// NewTrafficReporter initializes the SDK without starting a goroutine. The owner
// must call Close after all Report calls have returned.
func NewTrafficReporter(c *config.MeteringConfig) (*TrafficReporter, error) {
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
	return &TrafficReporter{
		// SDK defaults disable pagination and refuse overwrites. The wrapper logs
		// one result per batch; SDK per-page logs are unnecessary here.
		inner: meteringwriter.NewMeteringWriterFromConfig(provider, config.DefaultConfig(), c),
		id:    "ticdc" + strings.ReplaceAll(uuid.NewString(), "-", ""),
	}, nil
}

// Report uploads one batch. Records must not be mutated until this call returns.
// Retries within this call preserve batch identity and contents. Separate calls
// create separate batches, even with identical inputs; success does not acknowledge
// producer state. Errors, including ambiguous upload outcomes, are returned to the
// caller. The timestamp selects the SDK minute directory, not a billing interval.
func (w *TrafficReporter) Report(ctx context.Context, reportedAt time.Time, records []TrafficRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.ErrInvalidServerOption.GenWithStack("metering: traffic reporter is closed")
	}
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	if len(records) == 0 {
		return nil
	}
	ts := reportedAt.Truncate(reportInterval).Unix()
	if ts <= 0 {
		return errors.ErrInvalidServerOption.GenWithStack("metering: sampling timestamp must be positive")
	}
	data := make([]map[string]any, len(records))
	for i, record := range records {
		data[i] = map[string]any{
			"changefeed_gid": record.ChangefeedGID,
			"traffic_bytes":  common.MeteringValue{Value: record.TrafficBytes, Unit: "Bytes"},
		}
	}
	w.sequence++
	batch := &common.MeteringData{
		SelfID:    w.id + "b" + strconv.FormatUint(w.sequence, 16),
		Timestamp: ts,
		Category:  "ticdc",
		Data:      data,
	}
	return writeWithRetry(ctx, func(attemptCtx context.Context) error {
		return w.inner.Write(attemptCtx, batch)
	}, time.Second)
}

func writeWithRetry(ctx context.Context, write func(context.Context) error, baseDelay time.Duration) error {
	var err error
	for attempt := 0; attempt < writeAttempts; attempt++ {
		if ctx.Err() != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, ctx.Err())
		}
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = write(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		// Existence alone cannot prove that this payload was uploaded. Leave the
		// result failed and let the caller decide how to recover.
		if errors.Is(err, writer.ErrFileExists) || attempt == writeAttempts-1 {
			break
		}
		delay := min(baseDelay<<attempt, 10*baseDelay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.WrapError(errors.ErrExternalStorageAPI, ctx.Err())
		case <-timer.C:
		}
	}
	return errors.WrapError(errors.ErrExternalStorageAPI, err)
}

// Close releases the SDK after an in-flight Report finishes. It is idempotent.
func (w *TrafficReporter) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		if err := w.inner.Close(); err != nil {
			log.Warn("metering writer close failed", zap.Error(errors.WrapError(errors.ErrExternalStorageAPI, err)))
		}
	}
}
