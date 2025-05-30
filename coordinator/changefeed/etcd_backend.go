// Copyright 2024 PingCAP, Inc.
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

package changefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// EtcdBackend is the changefeed meta store using etcd as the storage
// todo: compares when commit transaction
type EtcdBackend struct {
	etcdClient etcd.CDCEtcdClient
}

// NewEtcdBackend creates a EtcdBackend
func NewEtcdBackend(etcdClient etcd.CDCEtcdClient) *EtcdBackend {
	b := &EtcdBackend{
		etcdClient: etcdClient,
	}
	return b
}

func (b *EtcdBackend) GetAllChangefeeds(ctx context.Context) (map[common.ChangeFeedID]*ChangefeedMetaWrapper, error) {
	changefeedPrefix := etcd.NamespacedPrefix(b.etcdClient.GetClusterID(), common.DefaultNamespace) + "/changefeed"

	resp, err := b.etcdClient.GetEtcdClient().Get(ctx, changefeedPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}

	statusMap := make(map[common.ChangeFeedDisplayName]*config.ChangeFeedStatus)
	cfMap := make(map[common.ChangeFeedID]*ChangefeedMetaWrapper)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		ns, cf, isStatus := extractKeySuffix(key)
		if isStatus {
			status := &config.ChangeFeedStatus{}
			err = status.Unmarshal(kv.Value)
			if err != nil {
				log.Warn("failed to unmarshal change feed Status, ignore",
					zap.String("key", key), zap.Error(err))
				continue
			}
			statusMap[common.NewChangeFeedDisplayName(cf, ns)] = status
		} else {
			detail := &config.ChangeFeedInfo{}
			err = detail.Unmarshal(kv.Value)
			if err != nil {
				log.Warn("failed to unmarshal change feed Info, ignore",
					zap.String("key", key), zap.Error(err))
				continue
			}
			// we can not load the changefeed name from the value, it must an old version info
			if detail.ChangefeedID.Name() == "" {
				log.Warn("load a old version change feed Info, migrate it to new version",
					zap.String("key", key))
				detail.ChangefeedID = common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
					Name:      cf,
					Namespace: ns,
				})
				if data, err := detail.Marshal(); err != nil {
					log.Warn("failed to marshal change feed Info, ignore",
						zap.Error(err))
				} else {
					_, _ = b.etcdClient.GetEtcdClient().Put(ctx, key, data)
				}
			}

			cfMap[detail.ChangefeedID] = &ChangefeedMetaWrapper{Info: detail}
		}
	}
	for id, wrapper := range cfMap {
		wrapper.Status = statusMap[id.DisplayName]
	}

	// check the invalid cf without Info, add a new Status
	for id, meta := range cfMap {
		if meta.Status == nil {
			log.Warn("failed to load change feed Status, add a new one")
			status := &config.ChangeFeedStatus{
				CheckpointTs: meta.Info.StartTs,
				Progress:     config.ProgressNone,
			}
			data, err := json.Marshal(status)
			if err != nil {
				log.Warn("failed to marshal change feed Status, ignore", zap.Error(err))
				delete(cfMap, id)
				continue
			}
			_, err = b.etcdClient.GetEtcdClient().Put(ctx, etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), id.DisplayName), string(data))
			if err != nil {
				log.Warn("failed to save change feed Status, ignore", zap.Error(err))
				delete(cfMap, id)
				continue
			}
			meta.Status = status
		}
	}

	return cfMap, nil
}

func (b *EtcdBackend) CreateChangefeed(ctx context.Context,
	info *config.ChangeFeedInfo,
) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), info.ChangefeedID.DisplayName)
	infoValue, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	status := &config.ChangeFeedStatus{
		CheckpointTs: info.StartTs,
		Progress:     config.ProgressNone,
	}
	jobValue, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), info.ChangefeedID.DisplayName)

	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpPut(infoKey, infoValue))
	opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))

	resp, err := b.etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{
		clientv3.Compare(clientv3.CreateRevision(infoKey), "=", 0),
		clientv3.Compare(clientv3.CreateRevision(jobKey), "=", 0),
	}, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("create changefeed %s", info.ChangefeedID.Name()))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) UpdateChangefeed(ctx context.Context, info *config.ChangeFeedInfo, checkpointTs uint64, progress config.Progress) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), info.ChangefeedID.DisplayName)
	newStr, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	status := &config.ChangeFeedStatus{
		CheckpointTs: checkpointTs,
		Progress:     progress,
	}
	statusStr, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), info.ChangefeedID.DisplayName)
	opsThen := []clientv3.Op{}
	opsThen = append(opsThen,
		clientv3.OpPut(infoKey, newStr),
		clientv3.OpPut(jobKey, statusStr),
	)

	putResp, err := b.etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{}, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("update changefeed %s failed", info.ChangefeedID.Name()))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error {
	info, err := b.etcdClient.GetChangeFeedInfo(ctx, id.DisplayName)
	if err != nil {
		return errors.Trace(err)
	}
	info.State = config.StateStopped
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), id.DisplayName)
	inforValue, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	status, _, err := b.etcdClient.GetChangeFeedStatus(ctx, id)
	status.Progress = config.ProgressStopping
	if err != nil {
		return errors.Trace(err)
	}
	jobValue, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), id.DisplayName)
	putResp, err := b.etcdClient.GetEtcdClient().Txn(ctx, nil,
		[]clientv3.Op{
			clientv3.OpPut(jobKey, jobValue),
			clientv3.OpPut(infoKey, inforValue),
		},
		[]clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("pause changefeed %s failed", id.DisplayName))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) DeleteChangefeed(ctx context.Context,
	changefeedID common.ChangeFeedID,
) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), changefeedID.DisplayName)
	jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), changefeedID.DisplayName)
	opsThen := []clientv3.Op{}
	opsThen = append(opsThen, clientv3.OpDelete(infoKey))
	opsThen = append(opsThen, clientv3.OpDelete(jobKey))
	resp, err := b.etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{}, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("delete changefeed %s", changefeedID.Name()))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) ResumeChangefeed(ctx context.Context,
	id common.ChangeFeedID, newCheckpointTs uint64,
) error {
	info, err := b.etcdClient.GetChangeFeedInfo(ctx, id.DisplayName)
	if err != nil {
		return errors.Trace(err)
	}
	info.State = config.StateNormal
	newStr, err := info.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), id.DisplayName)
	opsThen := []clientv3.Op{
		clientv3.OpPut(infoKey, newStr),
	}
	if newCheckpointTs > 0 {
		status, _, err := b.etcdClient.GetChangeFeedStatus(ctx, id)
		if err != nil {
			return errors.Trace(err)
		}
		status.CheckpointTs = newCheckpointTs
		jobValue, err := status.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), id.DisplayName)
		opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))
	}

	putResp, err := b.etcdClient.GetEtcdClient().Txn(ctx, nil, opsThen, []clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("resume changefeed %s", info.ChangefeedID.Name()))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) SetChangefeedProgress(ctx context.Context, id common.ChangeFeedID, progress config.Progress) error {
	status, modVersion, err := b.etcdClient.GetChangeFeedStatus(ctx, id)
	if err != nil {
		return errors.Trace(err)
	}
	status.Progress = progress
	jobValue, err := status.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), id.DisplayName)
	putResp, err := b.etcdClient.GetEtcdClient().Txn(ctx,
		[]clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(jobKey), "=", modVersion)},
		[]clientv3.Op{clientv3.OpPut(jobKey, jobValue)},
		[]clientv3.Op{})
	if err != nil {
		return errors.Trace(err)
	}
	if !putResp.Succeeded {
		err = cerror.ErrMetaOpFailed.GenWithStackByArgs(fmt.Sprintf("update changefeed to %s-%d", id.DisplayName, progress))
		return errors.Trace(err)
	}
	return nil
}

func (b *EtcdBackend) UpdateChangefeedCheckpointTs(ctx context.Context, cps map[common.ChangeFeedID]uint64) error {
	opsThen := make([]clientv3.Op, 0, 128)
	batchSize := 0

	txnFunc := func() error {
		putResp, err := b.etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{}, opsThen, []clientv3.Op{})
		if err != nil {
			return errors.Trace(err)
		}
		logEtcdOps(opsThen, putResp.Succeeded)
		if !putResp.Succeeded {
			return errors.New("commit failed")
		}
		return err
	}
	for cfID, checkpointTs := range cps {
		status := &config.ChangeFeedStatus{CheckpointTs: checkpointTs, Progress: config.ProgressNone}
		jobValue, err := status.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		jobKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), cfID.DisplayName)
		opsThen = append(opsThen, clientv3.OpPut(jobKey, jobValue))
		batchSize++
		if batchSize >= 128 {
			if err := txnFunc(); err != nil {
				return errors.Trace(err)
			}
			opsThen = opsThen[:0]
			batchSize = 0
		}
	}
	if batchSize > 0 {
		if err := txnFunc(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// extractKeySuffix extracts the suffix of an etcd key, such as extracting
// "6a6c6dd290bc8732" from /tidb/cdc/cluster/namespace/changefeed/info/6a6c6dd290bc8732
// or from /tidb/cdc/cluster/namespace/changefeed/status/6a6c6dd290bc8732
func extractKeySuffix(key string) (string, string, bool) {
	subs := strings.Split(key, "/")
	return subs[len(subs)-4], subs[len(subs)-1], subs[len(subs)-2] == "status"
}

func logEtcdOps(ops []clientv3.Op, committed bool) {
	if committed && (log.GetLevel() != zapcore.DebugLevel || len(ops) == 0) {
		return
	}
	logFn := log.Debug
	if !committed {
		logFn = log.Info
	}
	logFn("[etcd] ==========Update State to ETCD==========")
	for _, op := range ops {
		if op.IsDelete() {
			logFn("[etcd] delete key", zap.ByteString("key", op.KeyBytes()))
		} else {
			logFn("[etcd] put key", zap.ByteString("key", op.KeyBytes()), zap.ByteString("value", op.ValueBytes()))
		}
	}
	logFn("[etcd] ============State Commit=============", zap.Bool("committed", committed))
}
