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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package metering provides configuration validation, canonical SQL value
// sizing, and object-storage reporting for TiCDC changefeed traffic metering.
package metering

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/metering_sdk/writer/metering"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	// RecordVersion1 identifies the flat v1 changefeed metering record format.
	RecordVersion1 = "1"
	// BytesUnit is the MeteringValue unit for byte-denominated traffic metrics.
	BytesUnit = "Bytes"

	reportCategory = "ticdc"
	writeTimeout   = 10 * time.Second
	writeAttempts  = 3
)

// Record represents one changefeed traffic delta uploaded per minute.
type Record struct {
	KeyspaceName   string
	ChangefeedName string
	EffectiveBytes uint64
}

// Reporter uploads per-changefeed traffic deltas synchronously via Metering SDK.
// The caller retains responsibility for in-memory accumulation, scheduling, and
// retaining deltas across unsuccessful Report attempts.
type Reporter struct {
	mu     sync.Mutex
	inner  *meteringwriter.MeteringWriter
	selfID string
	closed bool
}

// NewReporter initializes the SDK writer without starting background goroutines.
// It validates the config locally and configures OverwriteExisting = false to
// protect per-minute delta objects. The owner must call Close after all Report
// calls return.
func NewReporter(c *config.MeteringConfig, clusterID string) (*Reporter, error) {
	if c == nil || c.Type == "" {
		return nil, errors.ErrInvalidServerOption.GenWithStack("metering: destination is required")
	}
	if err := ValidateConfig(c); err != nil {
		return nil, err
	}
	// Replace '-' with '_' because the SDK forbids dashes in self_id path segments.
	selfID := strings.ReplaceAll(strings.TrimSpace(clusterID), "-", "_")
	if selfID == "" {
		return nil, errors.ErrInvalidServerOption.GenWithStack("metering: cluster-id must not be empty")
	}
	provider, err := storage.NewObjectStorageProvider(c.ToProviderConfig())
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, util.MaskSensitiveDataInURLError(err))
	}
	return &Reporter{
		inner:  meteringwriter.NewMeteringWriterFromConfig(provider, config.DefaultConfig().WithOverwriteExisting(false), c),
		selfID: selfID,
	}, nil
}

// Report uploads all changefeed delta records for timestamp into one merged object.
// Overwriting an existing object at the same timestamp is disabled; if the object
// already exists on the initial attempt, Report fails immediately with ErrFileExists.
func (r *Reporter) Report(ctx context.Context, timestamp int64, records []Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.ErrMeteringReporterClosed.GenWithStackByArgs()
	}
	if len(records) == 0 {
		return nil
	}

	items := make([]map[string]any, 0, len(records))
	for _, record := range records {
		if record.KeyspaceName == "" {
			return errors.ErrMeteringInvalidBatch.GenWithStackByArgs("keyspace name must not be empty")
		}
		if record.ChangefeedName == "" {
			return errors.ErrMeteringInvalidBatch.GenWithStackByArgs("changefeed name must not be empty")
		}
		items = append(items, map[string]any{
			"version":    RecordVersion1,
			"cluster_id": record.KeyspaceName,
			"changefeed": record.ChangefeedName,
			"effective_bytes": common.MeteringValue{
				Value: record.EffectiveBytes,
				Unit:  BytesUnit,
			},
		})
	}

	return r.write(ctx, &common.MeteringData{
		SelfID:    r.selfID,
		Timestamp: timestamp,
		Category:  reportCategory,
		Data:      items,
	})
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
		if err = ctx.Err(); err != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, err)
		}
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = r.inner.Write(attemptCtx, data)
		cancel()
		if err == nil {
			return nil
		}
		if errors.Is(err, writer.ErrFileExists) {
			if attempt > 0 {
				// attempt==0 HEAD confirmed absence before our PUT, so ErrFileExists on retry
				// means the earlier PUT succeeded on storage; avoid re-uploading under next minute.
				return nil
			}
			break
		}
	}
	return errors.WrapError(errors.ErrExternalStorageAPI, util.MaskSensitiveDataInURLError(err))
}

// Close releases the SDK writer after an in-flight Report finishes. It is idempotent.
func (r *Reporter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.closed = true
		if err := r.inner.Close(); err != nil {
			log.Warn("metering writer close failed",
				zap.Error(errors.WrapError(errors.ErrExternalStorageAPI, util.MaskSensitiveDataInURLError(err))))
		}
	}
}
