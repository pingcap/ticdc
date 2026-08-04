// Copyright 2025 PingCAP, Inc.
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

package eventservice

import "github.com/pingcap/ticdc/pkg/metrics"

type pendingBigTxnMetric struct {
	startTs    uint64
	commitTs   uint64
	rawKVBytes int64
}

// bigTxnMetricTracker combines fragments from a logical transaction that spans
// multiple scan attempts, then reports that transaction exactly once.
type bigTxnMetricTracker struct {
	pending *pendingBigTxnMetric
}

func (t *bigTxnMetricTracker) addFragment(
	startTs, commitTs uint64,
	rawKVBytes, largeTxnThresholdInBytes int64,
) {
	t.flushBefore(startTs, commitTs)
	if rawKVBytes <= largeTxnThresholdInBytes {
		return
	}
	if t.pending == nil {
		t.pending = &pendingBigTxnMetric{
			startTs:  startTs,
			commitTs: commitTs,
		}
	}
	t.pending.rawKVBytes += rawKVBytes
}

func (t *bigTxnMetricTracker) finishTxn(
	startTs, commitTs uint64,
	rawKVBytes, largeTxnThresholdInBytes int64,
) {
	t.flushBefore(startTs, commitTs)
	if t.pending != nil {
		rawKVBytes += t.pending.rawKVBytes
		t.pending = nil
	}
	if rawKVBytes > largeTxnThresholdInBytes {
		observeBigTxnMetric(rawKVBytes)
	}
}

func (t *bigTxnMetricTracker) flushBefore(startTs, commitTs uint64) {
	if t.pending == nil ||
		(t.pending.startTs == startTs && t.pending.commitTs == commitTs) {
		return
	}
	t.flush()
}

func (t *bigTxnMetricTracker) flush() {
	if t.pending == nil {
		return
	}
	// A pending sample is created only after one fragment exceeds the threshold,
	// so it remains a big transaction without storing or rechecking the threshold.
	rawKVBytes := t.pending.rawKVBytes
	t.pending = nil
	observeBigTxnMetric(rawKVBytes)
}

func observeBigTxnMetric(rawKVBytes int64) {
	if rawKVBytes <= 0 {
		return
	}
	metrics.EventServiceBigTxnSize.Observe(float64(rawKVBytes))
	metrics.EventServiceBigTxnCount.Inc()
}
