// Copyright 2020 PingCAP, Inc.
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

package schemamanager

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func TestSchemaRegistry(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	topic := "cdctest"

	err = manager.ClearRegistry(ctx, topic)
	require.NoError(t, err)

	_, err = manager.Lookup(ctx, topic, SchemaID{confluentSchemaID: 1})
	require.Regexp(t, `.*not\sfound.*`, err)

	schemaDefinition := `{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
          }
          ]
     }`

	schemaID, err := manager.Register(ctx, topic, schemaDefinition)
	require.NoError(t, err)

	lookedUpSchema, err := manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, schemaDefinition, lookedUpSchema)

	schemaDefinition = `{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           },
           {
             "type": [
      			"null",
      			"string"
             ],
             "default": null,
             "name": "field2"
		   },
		   {
			"type": [
				"string",
				"null"
			],
			"default": "null",
			"name": "field3"
          }
          ]
     }`
	schemaID, err = manager.Register(ctx, topic, schemaDefinition)
	require.NoError(t, err)

	lookedUpSchema, err = manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, schemaDefinition, lookedUpSchema)
}

func TestSchemaRegistryBad(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	_, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:808", nil)
	require.Error(t, err)

	_, err = NewConfluentSchemaManager(ctx, "https://127.0.0.1:8080", nil)
	require.Error(t, err)
}

func TestRegisterReturnsServerError(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	schemaDefinition := `{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
          }
          ]
     }`

	schemaID, err := manager.Register(ctx, "server-error", schemaDefinition)
	require.ErrorIs(t, err, errors.ErrAvroSchemaAPIError)
	require.Zero(t, schemaID.confluentSchemaID)
}

func TestSchemaRegistryIdempotent(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	topic := "cdctest"

	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(ctx, topic)
		require.NoError(t, err)
	}

	schemaDefinition := `{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           },
           {
             "type": [
      			"null",
      			"string"
             ],
             "default": null,
             "name": "field2"
          }
          ]
     }`

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(ctx, topic, schemaDefinition)
		require.NoError(t, err)
		require.True(t, id == 0 || id == id1.confluentSchemaID)
		id = id1.confluentSchemaID
	}
}

func TestGetCachedOrRegister(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	schemaDefinition := `{
       "type": "record",
       "name": "test1",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           },
           {
             "type": [
      			"null",
      			"string"
             ],
             "default": null,
             "name": "field2"
          }
          ]
     }`
	topic := "cdctest"
	initialCalls := httpmock.GetTotalCallCount()

	header, err := manager.GetCachedOrRegister(ctx, topic, "test1", 1, schemaDefinition)
	require.NoError(t, err)
	cID, err := GetConfluentSchemaIDFromHeader(header)
	require.NoError(t, err)
	require.Greater(t, cID, uint32(0))
	require.Equal(t, initialCalls+1, httpmock.GetTotalCallCount())

	header1, err := manager.GetCachedOrRegister(ctx, topic, "test1", 1, schemaDefinition)
	require.NoError(t, err)
	require.Equal(t, header, header1)
	require.Equal(t, initialCalls+1, httpmock.GetTotalCallCount())

	header2, err := manager.GetCachedOrRegister(ctx, topic, "test1", 2, schemaDefinition)
	require.NoError(t, err)
	require.Equal(t, header, header2)
	require.Equal(t, initialCalls+2, httpmock.GetTotalCallCount())
}

func TestGetCachedOrRegisterWithDifferentSchemaIdentity(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	const (
		subject      = "table-route-value"
		tableVersion = uint64(1)
	)
	firstSchema := `{"type":"record","name":"source_table","fields":[{"name":"id","type":"int"}]}`
	secondSchema := `{"type":"record","name":"target_table","fields":[{"name":"id","type":"int"}]}`

	firstHeader, err := manager.GetCachedOrRegister(
		ctx, subject, "source_table", tableVersion, firstSchema)
	require.NoError(t, err)

	secondHeader, err := manager.GetCachedOrRegister(
		ctx, subject, "target_table", tableVersion, secondSchema)
	require.NoError(t, err)
	require.NotEqual(t, firstHeader, secondHeader)
	schemaID, err := GetConfluentSchemaIDFromHeader(secondHeader)
	require.NoError(t, err)
	lookedUpSchema, err := manager.Lookup(ctx, subject, NewConfluentSchemaID(int(schemaID)))
	require.NoError(t, err)
	require.Equal(t, secondSchema, lookedUpSchema)
}

func TestGetCachedOrRegisterDeduplicatesConcurrentRegistration(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	const concurrency = 32
	start := make(chan struct{})
	results := make(chan error, concurrency)
	initialCalls := httpmock.GetTotalCallCount()
	schemaDefinition := `{"type":"record","name":"table","fields":[{"name":"id","type":"int"}]}`
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			<-start
			_, err := manager.GetCachedOrRegister(
				ctx, "table-route-value", "target.table", 1, schemaDefinition)
			results <- err
		})
	}

	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, initialCalls+1, httpmock.GetTotalCallCount())
}

func TestSchemaCacheSerializesCreationBySubject(t *testing.T) {
	cache := newSchemaCache()
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		_, _, err := cache.getOrCreate("table-route-value", "target.first", 1, func() (*schemaCacheEntry, error) {
			close(firstStarted)
			<-releaseFirst
			return &schemaCacheEntry{schemaVersion: 1}, nil
		})
		results <- err
	}()
	<-firstStarted
	go func() {
		_, _, err := cache.getOrCreate("table-route-value", "target.second", 1, func() (*schemaCacheEntry, error) {
			close(secondStarted)
			return &schemaCacheEntry{schemaVersion: 1}, nil
		})
		results <- err
	}()

	secondStartedBeforeRelease := false
	select {
	case <-secondStarted:
		secondStartedBeforeRelease = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.False(t, secondStartedBeforeRelease)
}

func TestHTTPRetryReturnsServerError(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	payload := []byte("test")
	req, err := http.NewRequestWithContext(ctx,
		"POST", "http://127.0.0.1:8081/may-fail", bytes.NewReader(payload))
	require.NoError(t, err)

	resp, err := httpRetry(ctx, nil, req)
	require.NoError(t, err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	_ = resp.Body.Close()
}
