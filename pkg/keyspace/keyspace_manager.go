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

package keyspace

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/upstream"
	"github.com/pingcap/tidb/pkg/kv"
	"go.uber.org/zap"
)

const (
	updateDuration = 60 * time.Second
)

type Manager interface {
	// Run starts the manager
	Run()
	// LoadKeyspace loads keyspace metadata by name from local
	// If the local cache is not found, it will load from pd
	LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error)
	// ForceLoadKeyspace force loads keyspace metadata from pd and update local cache
	ForceLoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error)
	// GetKeyspaceByID loads keyspace metadata by id
	GetKeyspaceByID(ctx context.Context, keyspaceID uint32) (*keyspacepb.KeyspaceMeta, error)
	// GetStorage get a storag for the keyspace
	GetStorage(keyspace string) (kv.Storage, error)
	// Close close the manager
	Close()
}

func NewManager(pdEndpoints []string) Manager {
	m := &manager{
		pdEndpoints:   pdEndpoints,
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}

	return m
}

type manager struct {
	pdEndpoints []string

	keyspaceMap   map[string]*keyspacepb.KeyspaceMeta
	keyspaceIDMap map[uint32]*keyspacepb.KeyspaceMeta
	keyspaceMu    sync.Mutex

	storageMap map[string]kv.Storage
	storageMu  sync.Mutex

	ticker   *time.Ticker
	updateMu sync.Mutex
}

func (k *manager) Run() {
	k.ticker = time.NewTicker(updateDuration)
	go k.updatePeriodicity()
}

func (k *manager) LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	if kerneltype.IsClassic() {
		return &keyspacepb.KeyspaceMeta{
			Name: common.DefaultKeyspace,
		}, nil
	}

	k.keyspaceMu.Lock()
	meta := k.keyspaceMap[keyspace]
	k.keyspaceMu.Unlock()
	if meta != nil {
		return meta, nil
	}

	return k.ForceLoadKeyspace(ctx, keyspace)
}

func (k *manager) ForceLoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	var meta *keyspacepb.KeyspaceMeta
	var err error
	pdAPIClient := appcontext.GetService[pdutil.PDAPIClient](appcontext.PDAPIClient)
	err = retry.Do(ctx, func() error {
		meta, err = pdAPIClient.LoadKeyspace(ctx, keyspace)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(1000), retry.WithMaxTries(6))
	if err != nil {
		log.Error("retry to load keyspace from pd", zap.String("keyspace", keyspace), zap.Error(err))
		return nil, errors.Trace(err)
	}

	k.keyspaceMu.Lock()
	defer k.keyspaceMu.Unlock()

	k.keyspaceMap[keyspace] = meta
	k.keyspaceIDMap[meta.Id] = meta

	return meta, nil
}

func (k *manager) GetKeyspaceByID(ctx context.Context, keyspaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	if kerneltype.IsClassic() {
		return &keyspacepb.KeyspaceMeta{
			Name: common.DefaultKeyspace,
		}, nil
	}

	k.keyspaceMu.Lock()
	meta := k.keyspaceIDMap[keyspaceID]
	k.keyspaceMu.Unlock()
	if meta != nil {
		return meta, nil
	}

	var err error
	pdAPIClient := appcontext.GetService[pdutil.PDAPIClient](appcontext.PDAPIClient)
	err = retry.Do(ctx, func() error {
		meta, err = pdAPIClient.GetKeyspaceMetaByID(ctx, keyspaceID)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(1000), retry.WithMaxTries(6))
	if err != nil {
		log.Error("retry to load keyspace from pd", zap.Uint32("keyspaceID", keyspaceID), zap.Error(err))
		return nil, errors.Trace(err)
	}

	k.keyspaceMu.Lock()
	defer k.keyspaceMu.Unlock()
	// Double check, another goroutine might have fetched and stored it.
	if meta, ok := k.keyspaceIDMap[keyspaceID]; ok {
		return meta, nil
	}

	k.keyspaceMap[meta.Name] = meta
	k.keyspaceIDMap[keyspaceID] = meta

	return meta, nil
}

func (k *manager) GetStorage(keyspace string) (kv.Storage, error) {
	k.storageMu.Lock()
	defer k.storageMu.Unlock()

	if s := k.storageMap[keyspace]; s != nil {
		return s, nil
	}

	conf := config.GetGlobalServerConfig()
	kvStorage, err := upstream.CreateTiStore(strings.Join(k.pdEndpoints, ","), conf.Security, keyspace)
	if err != nil {
		return nil, errors.Trace(err)
	}

	k.storageMap[keyspace] = kvStorage

	return kvStorage, nil
}

func (k *manager) Close() {
	k.storageMu.Lock()
	defer k.storageMu.Unlock()

	for _, storage := range k.storageMap {
		err := storage.Close()
		if err != nil {
			log.Error("close storage", zap.String("keyspace", storage.GetKeyspace()), zap.Error(err))
		}
	}

	k.storageMap = make(map[string]kv.Storage)

	if k.ticker != nil {
		k.ticker.Stop()
		k.ticker = nil
	}
}

func (k *manager) updatePeriodicity() {
	if kerneltype.IsClassic() {
		return
	}

	for range k.ticker.C {
		k.update()
	}
}

func (k *manager) update() {
	// If we cannot get the lock, we don't need to do anything
	// because that means the previous process is still running.
	if !k.updateMu.TryLock() {
		log.Info("update keyspace lock failed")
		return
	}

	defer k.updateMu.Unlock()

	k.keyspaceMu.Lock()
	keyspaces := make([]string, 0, len(k.keyspaceMap))
	for _, keyspace := range k.keyspaceMap {
		keyspaces = append(keyspaces, keyspace.Name)
	}
	k.keyspaceMu.Unlock()

	ctx := context.Background()
	for _, keyspace := range keyspaces {
		_, err := k.ForceLoadKeyspace(ctx, keyspace)
		if err != nil {
			log.Warn("force load keyspace", zap.String("keyspace", keyspace), zap.Error(err))
		}
	}
}
