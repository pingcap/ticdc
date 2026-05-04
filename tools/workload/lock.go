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

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	plog "github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const maxPrewriteBatchBytes = 16 * 1024

type lockGenerator struct {
	tableID       int64
	rowCount      int64
	lockTTL       time.Duration
	maxTxnSize    int
	pdcli         pd.Client
	tikvStorage   tikv.Storage
	generatedKeys uint64
}

func (app *WorkloadApp) handleLockAction() error {
	if err := app.prepareLockTables(); err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pdcli, store, err := openTiKVStorage(ctx, app.Config.PDAddr)
	if err != nil {
		return errors.Trace(err)
	}
	defer store.Close()

	for _, db := range app.DBManager.GetDBs() {
		for tableIndex := 0; tableIndex < app.Config.TableCount; tableIndex++ {
			tableID, err := app.getTableID(db.Name, tableIndex+app.Config.TableStartIndex)
			if err != nil {
				return errors.Trace(err)
			}

			generator := lockGenerator{
				tableID:     tableID,
				rowCount:    app.Config.LockRowCount,
				lockTTL:     app.Config.LockTTL,
				maxTxnSize:  app.Config.LockMaxTxnSize,
				pdcli:       pdcli,
				tikvStorage: store,
			}
			if err := generator.generateLocks(ctx, app.Config.LockDuration); err != nil {
				return errors.Trace(err)
			}
			plog.Info("residual lock workload finished",
				zap.String("database", db.Name),
				zap.Int("tableIndex", tableIndex),
				zap.Int64("tableID", tableID),
				zap.Uint64("generatedKeys", generator.generatedKeys))
		}
	}

	return nil
}

func openTiKVStorage(ctx context.Context, pdAddr string) (pd.Client, *tikv.KVStore, error) {
	pdAddrs := strings.Split(pdAddr, ",")
	pdcli, err := pd.NewClientWithContext(
		ctx, "ticdc-workload-residual-lock", pdAddrs, pd.SecurityOption{})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	spkv, err := tikv.NewEtcdSafePointKV(pdAddrs, nil)
	if err != nil {
		pdcli.Close()
		return nil, nil, errors.Trace(err)
	}

	codecPDClient := tikv.NewCodecPDClient(tikv.ModeTxn, pdcli)
	store, err := tikv.NewKVStore(
		fmt.Sprintf("ticdc-workload-%d", pdcli.GetClusterID(ctx)),
		codecPDClient,
		spkv,
		tikv.NewRPCClient(),
	)
	if err != nil {
		pdcli.Close()
		return nil, nil, errors.Trace(err)
	}
	return codecPDClient, store, nil
}

func (app *WorkloadApp) prepareLockTables() error {
	if app.Config.SkipCreateTable {
		return nil
	}
	for _, db := range app.DBManager.GetDBs() {
		for tableIndex := 0; tableIndex < app.Config.TableCount; tableIndex++ {
			sql := app.Workload.BuildCreateTableStatement(tableIndex + app.Config.TableStartIndex)
			if _, err := db.DB.Exec(sql); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (app *WorkloadApp) getTableID(dbName string, tableIndex int) (int64, error) {
	tableName, err := app.getLockTableName(tableIndex)
	if err != nil {
		return 0, errors.Trace(err)
	}

	statusAddr := net.JoinHostPort(app.Config.DBHost, fmt.Sprintf("%d", app.Config.DBStatusPort))
	url := fmt.Sprintf("http://%s/schema/%s/%s", statusAddr, dbName, tableName)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(url) // #nosec G107
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, errors.Errorf("query table ID failed, status: %d, body: %s", resp.StatusCode, string(body))
	}

	var tableInfo struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(body, &tableInfo); err != nil {
		return 0, errors.Trace(err)
	}
	return tableInfo.ID, nil
}

func (app *WorkloadApp) getLockTableName(tableIndex int) (string, error) {
	switch app.Config.WorkloadType {
	case hotspotWorkload:
		return fmt.Sprintf("hotspot_%d", tableIndex), nil
	case sysbench:
		return fmt.Sprintf("sbtest%d", tableIndex), nil
	default:
		return "", errors.Errorf("lock action only supports workload types with int handle tables: %s, %s",
			hotspotWorkload, sysbench)
	}
}

func (g *lockGenerator) generateLocks(ctx context.Context, duration time.Duration) error {
	plog.Info("residual lock generation started",
		zap.Int64("tableID", g.tableID),
		zap.Int64("rowCount", g.rowCount),
		zap.String("duration", duration.String()),
		zap.String("lockTTL", g.lockTTL.String()))

	nextTxnSize := rand.Intn(g.maxTxnSize) + 1
	batch := make([]int64, 0, nextTxnSize)
	timer := time.After(duration)

	for rowID := int64(0); ; rowID = (rowID + 1) % g.rowCount {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-timer:
			plog.Info("residual lock generation stopped", zap.Uint64("generatedKeys", g.generatedKeys))
			return nil
		default:
		}

		if rand.Intn(2) != 0 {
			continue
		}

		batch = append(batch, rowID)
		if len(batch) < nextTxnSize {
			continue
		}

		if err := g.lockKeys(ctx, batch); err != nil {
			return errors.Trace(err)
		}

		g.generatedKeys += uint64(len(batch))
		nextTxnSize = rand.Intn(g.maxTxnSize) + 1
		batch = batch[:0]
	}
}

func (g *lockGenerator) lockKeys(ctx context.Context, rowIDs []int64) error {
	keys := make([][]byte, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		keys = append(keys, encodeRecordKey(g.tableID, rowID))
	}

	primary := keys[0]
	for len(keys) > 0 {
		lockedKeys, err := g.lockBatch(ctx, keys, primary)
		if err != nil {
			return errors.Trace(err)
		}
		if lockedKeys == 0 {
			return errors.Errorf("no keys were locked")
		}
		keys = keys[lockedKeys:]
	}
	return nil
}

func encodeRecordKey(tableID int64, rowID int64) []byte {
	key := make([]byte, 0, 19)
	key = append(key, 't')
	key = encodeComparableInt(key, tableID)
	key = append(key, '_', 'r')
	key = encodeComparableInt(key, rowID)
	return key
}

func encodeComparableInt(buf []byte, value int64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], uint64(value)^(1<<63))
	return append(buf, data[:]...)
}

func (g *lockGenerator) lockBatch(ctx context.Context, keys [][]byte, primary []byte) (int, error) {
	bo := tikv.NewBackoffer(ctx, 20000)
	for {
		loc, err := g.tikvStorage.GetRegionCache().LocateKey(bo, keys[0])
		if err != nil {
			return 0, errors.Trace(err)
		}

		physical, logical, err := g.pdcli.GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		startTs := oracle.ComposeTS(physical, logical)

		mutations := make([]*kvrpcpb.Mutation, 0, len(keys))
		batchSize := 0
		for _, key := range keys {
			if len(loc.EndKey) > 0 && bytes.Compare(key, loc.EndKey) >= 0 {
				break
			}
			if bytes.Compare(key, loc.StartKey) < 0 {
				break
			}

			value := randLockValue()
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Op_Put,
				Key:   key,
				Value: []byte(value),
			})
			batchSize += len(key) + len(value)
			if batchSize >= maxPrewriteBatchBytes {
				break
			}
		}

		lockedKeys := len(mutations)
		if lockedKeys == 0 {
			return 0, nil
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  primary,
			StartVersion: startTs,
			LockTtl:      uint64(g.lockTTL.Milliseconds()),
		})

		resp, err := g.tikvStorage.SendReq(bo, req, loc.Region, 20*time.Second)
		if err != nil {
			return 0, errors.Annotatef(err, "send prewrite request failed, region: %+v", loc.Region)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String())); err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}

		return lockedKeys, nil
	}
}

func randLockValue() string {
	var buf strings.Builder
	length := rand.Intn(128)
	for i := 0; i < length; i++ {
		buf.WriteByte(byte('a' + rand.Intn(26)))
	}
	return buf.String()
}
