// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package writer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	pclock "github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/stretchr/testify/require"
)

func getTableFiles(t *testing.T, tableDir string) []string {
	files, err := os.ReadDir(tableDir)
	require.Nil(t, err)

	fileNames := []string{}
	for _, f := range files {
		fileName := f.Name()
		if f.IsDir() {
			metaFiles, err := os.ReadDir(path.Join(tableDir, f.Name()))
			require.Nil(t, err)
			require.Len(t, metaFiles, 1)
			fileName = metaFiles[0].Name()
		}
		fileNames = append(fileNames, fileName)
	}
	return fileNames
}

func testWriter(ctx context.Context, t *testing.T, dir string) *Writer {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	storage, err := helper.GetExternalStorageFromURI(ctx, uri)
	require.Nil(t, err)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	err = cfg.Apply(context.TODO(), sinkURI, replicaConfig.Sink)
	cfg.FileIndexWidth = 6
	require.Nil(t, err)

	changefeedID := commonType.NewChangefeedID4Test("test", "dml-worker-test")
	statistics := metrics.NewStatistics(changefeedID, "dml-worker-test")
	pdlock := pdutil.NewMonotonicClock(pclock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	d := NewWriter(1, changefeedID, storage,
		cfg, ".json", chann.NewAutoDrainChann[EventFragment](), statistics)
	return d
}

func TestWriterRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
	fragCh := d.inputCh
	table1Dir := path.Join(parentDir, "test/table1/99")
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)
	dmlEvent := helper.DML2Event(job.SchemaName, job.TableName, "insert into table1 values(100, 'hello world')")

	for i := 0; i < 5; i++ {
		frag := EventFragment{
			seqNumber: uint64(i),
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: commonType.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				TableInfoVersion: 99,
			},
			event: dmlEvent,
			encodedMsgs: []*common.Message{
				{
					Value: []byte(fmt.Sprintf(`{"id":%d,"database":"test","table":"table1","pkNames":[],"isDdl":false,`+
						`"type":"INSERT","es":0,"ts":1663572946034,"sql":"","sqlType":{"c1":12,"c2":12},`+
						`"data":[{"c1":"100","c2":"hello world"}],"old":null}`, i)),
				},
			},
		}
		fragCh.In() <- frag
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.Run(ctx)
	}()

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	fileNames := getTableFiles(t, table1Dir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{"CDC000001.json", "CDC.index"}, fileNames)
	cancel()
	d.Close()
	wg.Wait()
	fragCh.CloseAndDrain()
}
