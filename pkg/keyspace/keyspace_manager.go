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

type KeyspaceManager interface {
	LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error)
	GetKeyspaceByID(keyspaceID uint32) *keyspacepb.KeyspaceMeta
	GetStorage(keyspace string) (kv.Storage, error)
	Close()
}

func NewKeyspaceManager(pdEndpoints []string) KeyspaceManager {
	return &keyspaceManager{
		pdEndpoints:   pdEndpoints,
		keyspaceMap:   make(map[string]*keyspacepb.KeyspaceMeta),
		keyspaceIDMap: make(map[uint32]*keyspacepb.KeyspaceMeta),
		storageMap:    make(map[string]kv.Storage),
	}
}

type keyspaceManager struct {
	pdEndpoints []string

	// TODO tenfyzhong 2025-09-16 23:46:01 update keyspaceMeta periodicity
	keyspaceMap   map[string]*keyspacepb.KeyspaceMeta
	keyspaceIDMap map[uint32]*keyspacepb.KeyspaceMeta
	keyspaceMu    sync.Mutex

	storageMap map[string]kv.Storage
	storageMu  sync.Mutex
}

func (k *keyspaceManager) LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	if kerneltype.IsClassic() {
		return &keyspacepb.KeyspaceMeta{
			Name: common.DefaultKeyspace,
		}, nil
	}

	k.keyspaceMu.Lock()
	defer k.keyspaceMu.Unlock()
	meta := k.keyspaceMap[keyspace]
	if meta != nil {
		return meta, nil
	}

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

	k.keyspaceMap[keyspace] = meta
	k.keyspaceIDMap[meta.Id] = meta

	return meta, nil
}

func (k *keyspaceManager) GetKeyspaceByID(keyspaceID uint32) *keyspacepb.KeyspaceMeta {
	if kerneltype.IsClassic() {
		return &keyspacepb.KeyspaceMeta{
			Name: common.DefaultKeyspace,
		}
	}

	k.keyspaceMu.Lock()
	defer k.keyspaceMu.Unlock()
	return k.keyspaceIDMap[keyspaceID]
}

func (k *keyspaceManager) GetStorage(keyspace string) (kv.Storage, error) {
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

func (k *keyspaceManager) Close() {
	k.storageMu.Lock()
	defer k.storageMu.Unlock()

	for _, storage := range k.storageMap {
		err := storage.Close()
		log.Error("close storage", zap.String("keyspace", storage.GetKeyspace()), zap.Error(err))
	}
}
