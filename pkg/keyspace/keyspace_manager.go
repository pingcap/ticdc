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

type Manager interface {
	LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error)
	GetKeyspaceByID(ctx context.Context, keyspaceID uint32) (*keyspacepb.KeyspaceMeta, error)
	GetStorage(ctx context.Context, keyspace string) (kv.Storage, error)
	Close()
}

func NewManager(pdEndpoints []string) Manager {
	return &manager{
		urls:       strings.Join(pdEndpoints, ","),
		storageMap: make(map[string]kv.Storage),
	}
}

type manager struct {
	urls string

	storageMap map[string]kv.Storage
	storageMu  sync.Mutex
}

var defaultKeyspaceMeta = &keyspacepb.KeyspaceMeta{
	Name: common.DefaultKeyspaceNamme,
	Id:   common.DefaultKeyspaceID,
}

func (k *manager) LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	if kerneltype.IsClassic() {
		return defaultKeyspaceMeta, nil
	}

	var meta *keyspacepb.KeyspaceMeta
	var err error

	pdAPIClient := appcontext.GetService[pdutil.PDAPIClient](appcontext.PDAPIClient)
	err = retry.Do(ctx, func() error {
		meta, err = pdAPIClient.LoadKeyspace(ctx, keyspace)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(500),
		retry.WithBackoffMaxDelay(1000),
		retry.WithMaxTries(6))
	if err != nil {
		log.Error("retry to load keyspace from pd", zap.String("keyspace", keyspace), zap.Error(err))
		return nil, errors.Trace(err)
	}

	return meta, nil
}

func (k *manager) GetKeyspaceByID(ctx context.Context, keyspaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	if kerneltype.IsClassic() {
		return defaultKeyspaceMeta, nil
	}

	var meta *keyspacepb.KeyspaceMeta
	var err error

	pdAPIClient := appcontext.GetService[pdutil.PDAPIClient](appcontext.PDAPIClient)
	err = retry.Do(ctx, func() error {
		meta, err = pdAPIClient.GetKeyspaceMetaByID(ctx, keyspaceID)
		if err != nil {
			return err
		}
		return nil
	}, retry.WithBackoffBaseDelay(500),
		retry.WithBackoffMaxDelay(1000),
		retry.WithMaxTries(6))
	if err != nil {
		log.Error("retry to load keyspace from pd", zap.Uint32("keyspaceID", keyspaceID), zap.Error(err))
		return nil, errors.Trace(err)
	}

	return meta, nil
}

func (k *manager) GetStorage(ctx context.Context, keyspace string) (kv.Storage, error) {
	k.storageMu.Lock()
	defer k.storageMu.Unlock()

	if s := k.storageMap[keyspace]; s != nil {
		return s, nil
	}

	conf := config.GetGlobalServerConfig()
	kvStorage, err := upstream.CreateTiStore(ctx, k.urls, conf.Security, keyspace)
	if err != nil {
		return nil, err
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
}
