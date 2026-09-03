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

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestSharedClientLifecycle(t *testing.T) {
	const topic = "shared-client"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	o := testOptions(cluster.ListenAddrs())
	o.Topic = topic
	created, err := newFranzFactory(
		t.Context(),
		o,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "shared-client"),
	)
	require.NoError(t, err)
	factory := created.(*franzFactory)
	defer factory.Close()

	adminClient, err := factory.AdminClient(t.Context())
	require.NoError(t, err)

	asyncClient, err := factory.AsyncProducer(t.Context())
	require.NoError(t, err)
	asyncProducer := asyncClient.(*asyncProducer)

	syncClient, err := factory.SyncProducer(t.Context())
	require.NoError(t, err)
	syncProducer := syncClient.(*syncProducer)

	require.Same(t, factory.client, asyncProducer.client)
	require.Same(t, factory.client, syncProducer.client)
	require.NoError(t, syncProducer.SendMessage(t.Context(), topic, 0, &codeccommon.Message{Value: []byte("sync")}))

	syncProducer.Close()
	require.NoError(t, asyncProducer.AsyncSend(t.Context(), topic, 0, &codeccommon.Message{Value: []byte("value")}))
	require.Eventually(t, func() bool {
		return asyncProducer.client.BufferedProduceRecords() == 0
	}, time.Second, time.Millisecond)

	asyncProducer.Close()
	topics, err := adminClient.GetTopicsMeta(context.Background(), []string{topic}, false)
	require.NoError(t, err)
	require.Contains(t, topics, topic)

	adminClient.Close()
	require.NoError(t, factory.client.Context().Err())
	factory.Close()
	require.ErrorIs(t, asyncProducer.client.Context().Err(), context.Canceled)
	factory.Close()
}
