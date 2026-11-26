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
	stderrors "errors"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriterapi "github.com/pingcap/metering_sdk/writer"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
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

// ToMeteringMap converts the record into the flat map consumed by MeteringData.Data.
func (r Record) ToMeteringMap() map[string]any {
	return map[string]any{
		"version":    RecordVersion1,
		"cluster_id": r.KeyspaceName,
		"changefeed": r.ChangefeedName,
		"effective_bytes": common.MeteringValue{
			Value: r.EffectiveBytes,
			Unit:  BytesUnit,
		},
	}
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
	selfID := normalizeSelfID(clusterID)
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

// normalizeSelfID derives the SDK self_id from clusterID by replacing '-' with '_'
// because the SDK forbids dashes in self_id path segments.
func normalizeSelfID(clusterID string) string {
	return strings.ReplaceAll(strings.TrimSpace(clusterID), "-", "_")
}

// SelfID returns the normalized cluster identifier used in uploaded object names.
func (r *Reporter) SelfID() string {
	return r.selfID
}

// Report uploads all changefeed delta records for timestamp into one merged object.
// Overwriting an existing object at the same timestamp is disabled; if the object
// already exists, Report fails immediately with ErrFileExists without retrying.
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
		items = append(items, record.ToMeteringMap())
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
				return errors.WrapError(errors.ErrExternalStorageAPI, util.MaskSensitiveDataInURLError(ctx.Err()))
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, util.MaskSensitiveDataInURLError(err))
		}
		attemptCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err = r.inner.Write(attemptCtx, data)
		cancel()
		if err == nil {
			return nil
		}
		if stderrors.Is(err, meteringwriterapi.ErrFileExists) {
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
