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

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestStorageSinkIcebergAppendBasic(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=iceberg&namespace=ns&commit-interval=200ms", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go func() {
		_ = cloudStorageSink.Run(ctx)
	}()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table t_iceberg (id int primary key, v varchar(32))")
	require.NotNil(t, job)
	helper.ApplyJob(job)

	dmlEvent := helper.DML2Event("test", "t_iceberg",
		"insert into t_iceberg values (1, 'a')",
		"insert into t_iceberg values (2, 'b')")
	dmlEvent.TableInfoVersion = job.BinlogInfo.FinishedTS
	cloudStorageSink.AddDMLEvent(dmlEvent)

	metadataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "metadata", "v*.metadata.json")
	dataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "data", "snap-*.parquet")

	require.Eventually(t, func() bool {
		metas, _ := filepath.Glob(metadataGlob)
		dataFiles, _ := filepath.Glob(dataGlob)
		return len(metas) > 0 && len(dataFiles) > 0
	}, 20*time.Second, 200*time.Millisecond)

	metas, err := filepath.Glob(metadataGlob)
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	metadataBytes, err := os.ReadFile(metas[len(metas)-1])
	require.NoError(t, err)
	require.Contains(t, string(metadataBytes), "\"format-version\":2")
	require.Contains(t, string(metadataBytes), "\"tidb.committed_resolved_ts\"")
}
