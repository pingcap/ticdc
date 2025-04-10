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
package worker

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/util"
	putil "github.com/pingcap/ticdc/pkg/util"
	pclock "github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func setClock(s *CloudStorageDMLWorker, clock pclock.Clock) {
	for _, w := range s.writers {
		w.SetClock(pdutil.NewMonotonicClock(clock))
	}
}

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

func newCloudStorageDMLWorkerForTest(parentDir string, flushInterval int, sinkConfig *config.SinkConfig) (*CloudStorageDMLWorker, error) {
	ctx := context.Background()
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	uri := fmt.Sprintf("file:///%s?protocol=csv&flush-interval=%ds", parentDir, flushInterval)
	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	if err != nil {
		return nil, err
	}
	changefeedID := common.NewChangefeedID4Test("test", "test")

	cfg := cloudstorage.NewConfig()
	err = cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return nil, err
	}
	protocol, err := helper.GetProtocol(
		putil.GetOrZero(sinkConfig.Protocol),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get cloud storage file extension according to the specific protocol.
	ext := helper.GetFileExtension(protocol)
	// the last param maxMsgBytes is mainly to limit the size of a single message for
	// batch protocols in mq scenario. In cloud storage sink, we just set it to max int.
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storage, err := helper.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}
	sink, err := NewCloudStorageDMLWorker(changefeedID, storage, cfg, encoderConfig, ext, metrics.NewStatistics(changefeedID, "CloudStorageSink"))
	if err != nil {
		return nil, err
	}
	go sink.Run(ctx)
	return sink, nil
}

func TestCloudStorageWriteEventsWithoutDateSeparator(t *testing.T) {
	parentDir := t.TempDir()
	csvProtocol := "csv"
	sinkConfig := &config.SinkConfig{Protocol: &csvProtocol, DateSeparator: putil.AddressOf(config.DateSeparatorNone.String()), FileIndexWidth: putil.AddressOf(6)}
	s, err := newCloudStorageDMLWorkerForTest(parentDir, 2, sinkConfig)
	require.NoError(t, err)
	var cnt uint64 = 0
	batch := 100
	var tableInfoVersion uint64 = 33

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)
	dmls := make([]string, 0, batch)
	for j := 0; j < batch; j++ {
		dmls = append(dmls, fmt.Sprintf("insert into table1 values (%d, 'hello world')", j))
	}
	event := helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	event.TableInfoVersion = tableInfoVersion
	s.AddDMLEvent(event)
	time.Sleep(3 * time.Second)
	metaDir := path.Join(parentDir, "test/table1/meta")
	files, err := os.ReadDir(metaDir)
	require.Nil(t, err)
	require.Len(t, files, 1)

	tableDir := path.Join(parentDir, fmt.Sprintf("%s/%s/%d", job.SchemaName, job.TableName, tableInfoVersion))
	fileNames := getTableFiles(t, tableDir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err := os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(100), atomic.LoadUint64(&cnt))

	// generating another dml file.
	event = helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	s.AddDMLEvent(event)
	time.Sleep(3 * time.Second)

	fileNames = getTableFiles(t, tableDir)
	require.Len(t, fileNames, 3)
	require.ElementsMatch(t, []string{
		"CDC000001.csv", "CDC000002.csv", "CDC.index",
	}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(200), atomic.LoadUint64(&cnt))

	s.Close()
}

func TestCloudStorageWriteEventsWithDateSeparator(t *testing.T) {
	parentDir := t.TempDir()
	csvProtocol := "csv"
	sinkConfig := &config.SinkConfig{Protocol: &csvProtocol, DateSeparator: putil.AddressOf(config.DateSeparatorDay.String()), FileIndexWidth: putil.AddressOf(6)}
	s, err := newCloudStorageDMLWorkerForTest(parentDir, 4, sinkConfig)
	require.Nil(t, err)

	var cnt uint64 = 0
	batch := 100
	var tableInfoVersion uint64 = 33

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)
	dmls := make([]string, 0, batch)
	for j := 0; j < batch; j++ {
		dmls = append(dmls, fmt.Sprintf("insert into table1 values (%d, 'hello world')", j))
	}

	mockClock := pclock.NewMock()
	mockClock.Set(time.Date(2023, 3, 8, 23, 59, 58, 0, time.UTC))
	setClock(s, mockClock)
	event := helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	event.TableInfoVersion = tableInfoVersion
	s.AddDMLEvent(event)
	time.Sleep(5 * time.Second)

	tableDir := path.Join(parentDir, fmt.Sprintf("%s/%s/%d/2023-03-08", job.SchemaName, job.TableName, tableInfoVersion))
	fileNames := getTableFiles(t, tableDir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err := os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(100), atomic.LoadUint64(&cnt))

	// test date (day) is NOT changed.
	mockClock.Set(time.Date(2023, 3, 8, 23, 59, 59, 0, time.UTC))
	setClock(s, mockClock)
	event = helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	event.TableInfoVersion = tableInfoVersion
	s.AddDMLEvent(event)
	time.Sleep(5 * time.Second)

	fileNames = getTableFiles(t, tableDir)
	require.Len(t, fileNames, 3)
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC000002.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(200), atomic.LoadUint64(&cnt))

	// test date (day) is changed.
	mockClock.Set(time.Date(2023, 3, 9, 0, 0, 10, 0, time.UTC))
	setClock(s, mockClock)

	failpoint.Enable("github.com/pingcap/ticdc/downstreamadapter/worker/writer/passTickerOnce", "1*return")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/downstreamadapter/worker/writer/passTickerOnce")
	}()

	event = helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	event.TableInfoVersion = tableInfoVersion
	s.AddDMLEvent(event)
	time.Sleep(10 * time.Second)

	tableDir = path.Join(parentDir, "test/table1/33/2023-03-09")
	fileNames = getTableFiles(t, tableDir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(300), atomic.LoadUint64(&cnt))
	s.Close()

	// test table is scheduled from one node to another
	cnt = 0
	s, err = newCloudStorageDMLWorkerForTest(parentDir, 4, sinkConfig)
	require.NoError(t, err)

	mockClock = pclock.NewMock()
	mockClock.Set(time.Date(2023, 3, 9, 0, 1, 10, 0, time.UTC))
	setClock(s, mockClock)

	event = helper.DML2Event(job.SchemaName, job.TableName, dmls...)
	event.AddPostFlushFunc(func() {
		atomic.AddUint64(&cnt, uint64(len(dmls)))
	})
	event.TableInfoVersion = tableInfoVersion
	s.AddDMLEvent(event)
	time.Sleep(5 * time.Second)

	fileNames = getTableFiles(t, tableDir)
	require.Len(t, fileNames, 3)
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC000002.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "meta/CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(100), atomic.LoadUint64(&cnt))

	s.Close()
}
