// Copyright 2022 PingCAP, Inc.
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

package codec

import (
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	encoderGroupInputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_input_chan_size",
			Help:      "The size of input channel of encoder group",
		}, []string{"namespace", "changefeed", "index"})
	// encoderGroupOutputChanSizeGauge tracks the size of output channel of encoder group
	encoderGroupOutputChanSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "encoder_group_output_chan_size",
			Help:      "The size of output channel of encoder group",
		}, []string{"namespace", "changefeed"})
	encoderGroupEncodeDuration = newEncoderGroupDurationHistogram(
		"encoder_group_encode_duration_seconds", "Time spent encoding a sampled encoder group future.")
	encoderGroupInputBlockDuration = newEncoderGroupDurationHistogram(
		"encoder_group_input_block_duration_seconds", "Time a sampled encoder group future waits to enter an encoder input channel.")
	encoderGroupOutputBlockDuration = newEncoderGroupDurationHistogram(
		"encoder_group_output_block_duration_seconds", "Time a sampled encoder group future waits to enter the output channel.")
	encoderGroupReadyWaitDuration = newEncoderGroupDurationHistogram(
		"encoder_group_ready_wait_duration_seconds", "Time the sink waits for a sampled encoder group future to finish encoding.")
)

func newEncoderGroupDurationHistogram(name, help string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      name,
		Help:      help + " One in every 257 futures is sampled.",
		Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 22),
	}, []string{"namespace", "changefeed"})
}

// InitMetrics registers all metrics in this file
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(encoderGroupInputChanSizeGauge)
	registry.MustRegister(encoderGroupOutputChanSizeGauge)
	registry.MustRegister(encoderGroupEncodeDuration)
	registry.MustRegister(encoderGroupInputBlockDuration)
	registry.MustRegister(encoderGroupOutputBlockDuration)
	registry.MustRegister(encoderGroupReadyWaitDuration)
	common.InitMetrics(registry)
}
