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
	"sync/atomic"
	"testing"
	"time"

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

	codec, err := GenCodec(`{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           }
          ]
     }`)
	require.NoError(t, err)

	schemaID, err := manager.Register(ctx, topic, codec.Schema())
	require.NoError(t, err)

	codec2, err := manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, codec.CanonicalSchema(), codec2.CanonicalSchema())

	codec, err = GenCodec(`{
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
     }`)
	require.NoError(t, err)
	schemaID, err = manager.Register(ctx, topic, codec.Schema())
	require.NoError(t, err)

	codec2, err = manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, codec.CanonicalSchema(), codec2.CanonicalSchema())
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

	codec, err := GenCodec(`{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           }
          ]
     }`)
	require.NoError(t, err)

	schemaID, err := manager.Register(ctx, "server-error", codec.Schema())
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

	codec, err := GenCodec(`{
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
     }`)
	require.NoError(t, err)

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(ctx, topic, codec.Schema())
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

	called := 0
	// nolint:unparam
	// NOTICE:This is a function parameter definition, so it cannot be modified.
	schemaGen := func() (string, error) {
		called++
		return `{
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
     }`, nil
	}
	topic := "cdctest"

	codec, header, err := manager.GetCachedOrRegister(ctx, topic, "test1", 1, schemaGen)
	require.NoError(t, err)
	cID, err := GetConfluentSchemaIDFromHeader(header)
	require.NoError(t, err)
	require.Greater(t, cID, uint32(0))
	require.NotNil(t, codec)
	require.Equal(t, 1, called)

	codec1, _, err := manager.GetCachedOrRegister(ctx, topic, "test1", 1, schemaGen)
	require.NoError(t, err)
	require.True(t, codec == codec1) // check identity
	require.Equal(t, 1, called)

	codec2, _, err := manager.GetCachedOrRegister(ctx, topic, "test1", 2, schemaGen)
	require.NoError(t, err)
	require.NotEqual(t, codec, codec2)
	require.Equal(t, 2, called)

	schemaGen = func() (string, error) {
		return `{
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
     }`, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		finalI := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				codec, header, err := manager.GetCachedOrRegister(
					ctx,
					topic,
					"test1",
					uint64(finalI),
					schemaGen,
				)
				require.NoError(t, err)
				cID, err := GetConfluentSchemaIDFromHeader(header)
				require.NoError(t, err)
				require.Greater(t, cID, uint32(0))
				require.NotNil(t, codec)
			}
		}()
	}
	wg.Wait()
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

	_, firstHeader, err := manager.GetCachedOrRegister(ctx, subject, "source_table", tableVersion, func() (string, error) {
		return firstSchema, nil
	})
	require.NoError(t, err)

	secondCodec, secondHeader, err := manager.GetCachedOrRegister(ctx, subject, "target_table", tableVersion, func() (string, error) {
		return secondSchema, nil
	})
	require.NoError(t, err)
	require.JSONEq(t, secondSchema, secondCodec.Schema())
	require.NotEqual(t, firstHeader, secondHeader)
}

func TestGetCachedOrRegisterDeduplicatesConcurrentRegistration(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	const concurrency = 32
	start := make(chan struct{})
	releaseSchemaGen := make(chan struct{})
	results := make(chan error, concurrency)
	var generated atomic.Int32
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		wg.Go(func() {
			defer wg.Done()
			<-start
			_, _, err := manager.GetCachedOrRegister(ctx, "table-route-value", "target.table", 1, func() (string, error) {
				generated.Add(1)
				<-releaseSchemaGen
				return `{"type":"record","name":"table","fields":[{"name":"id","type":"int"}]}`, nil
			})
			results <- err
		})
	}
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseSchemaGen) })
	}
	defer wg.Wait()
	defer release()

	close(start)
	require.Eventually(t, func() bool { return generated.Load() > 0 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	generatedBeforeRelease := generated.Load()
	release()
	wg.Wait()
	close(results)

	require.Equal(t, int32(1), generatedBeforeRelease)
	for err := range results {
		require.NoError(t, err)
	}
}

func TestGetCachedOrRegisterSerializesRegistrationBySubject(t *testing.T) {
	SetupTestingRegistry()
	defer TeardownTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		_, _, err := manager.GetCachedOrRegister(ctx, "table-route-value", "target.first", 1, func() (string, error) {
			close(firstStarted)
			<-releaseFirst
			return `{"type":"record","name":"first","fields":[{"name":"id","type":"int"}]}`, nil
		})
		results <- err
	}()
	<-firstStarted
	go func() {
		_, _, err := manager.GetCachedOrRegister(ctx, "table-route-value", "target.second", 1, func() (string, error) {
			close(secondStarted)
			return `{"type":"record","name":"second","fields":[{"name":"id","type":"int"}]}`, nil
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
