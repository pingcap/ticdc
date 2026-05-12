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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	pulsarPublishedDDLSchemaTableCount      = "published_DDL_schema_table_count"
	pulsarPublishedDMLSchemaTableCount      = "published_DML_schema_table_count"
	pulsarPublishedMessageTypeResolvedCount = "published_message_type_resolved_count"
)

var (
	// pulsar producer DDL scheme, type: count/success/fail, May there are too many tables
	PublishedDDLSchemaTableCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedDDLSchemaTableCount,
			Help:      "pulsar published schema count",
		}, []string{"topic", "changefeed", "type"})

	// pulsar producer DML scheme, type:count/success/fail , May there are too many tables
	PublishedDMLSchemaTableCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedDMLSchemaTableCount,
			Help:      "pulsar published dml message count",
		}, []string{"topic", "changefeed", "type"})

	// pulsar producer WATER count, type:count/success/fail , May there are too many tables
	PulsarPublishedMessageTypeResolvedCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "pulsar",
			Name:      pulsarPublishedMessageTypeResolvedCount,
			Help:      "pulsar published message type resolved count ",
		}, []string{"topic", "changefeed", "type"})
)

func initPulsarMetrics(registry *prometheus.Registry) {
	registry.MustRegister(PublishedDDLSchemaTableCount)
	registry.MustRegister(PublishedDMLSchemaTableCount)
	registry.MustRegister(PulsarPublishedMessageTypeResolvedCount)
}
