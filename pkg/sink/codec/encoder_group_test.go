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

package codec

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/stretchr/testify/require"
)

func TestAvroDDLAndDMLEncodersShareSchemaManager(t *testing.T) {
	var connectionChecks atomic.Int32
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		connectionChecks.Add(1)
		_, _ = w.Write([]byte("{}"))
	}))
	defer registry.Close()

	encoderConfig := common.NewConfig(config.ProtocolDebeziumAvro)
	encoderConfig.AvroConfluentSchemaRegistry = registry.URL
	schemaM, err := schemamanager.NewSchemaManager(t.Context(), encoderConfig)
	require.NoError(t, err)

	concurrency := 4
	sinkConfig := &config.SinkConfig{EncoderConcurrency: &concurrency}
	encoderGroup, err := NewEncoderGroup(
		sinkConfig, encoderConfig, nil, schemaM, encoderConfig.ChangefeedID)
	require.NoError(t, err)
	require.Len(t, encoderGroup.rowEventEncoders, concurrency)

	ddlEncoder, err := NewEventEncoder(encoderConfig, nil, schemaM)
	require.NoError(t, err)
	require.NotNil(t, ddlEncoder)
	require.Equal(t, int32(1), connectionChecks.Load())
}
