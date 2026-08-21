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

package franz

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestFactoryCreatesAllClients(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	defer cluster.Close()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "factory")
	factory := NewFactory(testConfig(cluster.ListenAddrs()), changefeedID)

	admin, err := factory.Admin(context.Background())
	require.NoError(t, err)
	admin.Close()

	syncProducer, err := factory.SyncProducer(context.Background())
	require.NoError(t, err)
	syncProducer.Close()

	asyncProducer, err := factory.AsyncProducer(context.Background())
	require.NoError(t, err)
	asyncProducer.Close()

	factory.CleanupMetrics()
}

func TestFactoryCleansMetricsAfterProducerConstructionFailure(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "factory-error")
	config := testConfig([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = maxProducerBatchBytes + 1
	factory := NewFactory(config, changefeedID)

	_, err := factory.SyncProducer(context.Background())
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	require.False(t, recordsPerBatch.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.Name()))

	_, err = factory.AsyncProducer(context.Background())
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	require.False(t, recordsPerBatch.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.Name()))
}
