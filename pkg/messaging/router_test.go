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

package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRegisterAndDeRegisterHandler(t *testing.T) {
	r := newRouter()
	testTopic := "test-topic"

	handler := func(ctx context.Context, msg *TargetMessage) error { return nil }
	r.registerHandler(testTopic, handler)

	assert.Len(t, r.handlers, 1)
	assert.Contains(t, r.handlers, testTopic)

	r.deRegisterHandler(testTopic)
	assert.Len(t, r.handlers, 0)
	assert.NotContains(t, r.handlers, testTopic)
}

type mockIOTypeT struct {
	payload []byte
}

func (m *mockIOTypeT) Unmarshal(data []byte) error {
	m.payload = data
	return nil
}

func (m *mockIOTypeT) Marshal() ([]byte, error) {
	return m.payload, nil
}

func newMockIOTypeT(payload []byte) []IOTypeT {
	return []IOTypeT{&mockIOTypeT{payload: payload}}
}

func TestRunDispatchSuccessful(t *testing.T) {
	t.Parallel()
	handledMsg := make([]*TargetMessage, 0)
	r := newRouter()
	r.registerHandler("topic1", func(ctx context.Context, msg *TargetMessage) error {
		handledMsg = append(handledMsg, msg)
		return nil
	})

	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "topic1", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	r.runDispatch(ctx, msgChan)

	assert.Len(t, handledMsg, 1)
	assert.Equal(t, "topic1", handledMsg[0].Topic)
}

func TestRunDispatchWithError(t *testing.T) {
	t.Parallel()
	r := newRouter()
	r.registerHandler("topic1", func(ctx context.Context, msg *TargetMessage) error {
		return errors.New("handler error")
	})

	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "topic1", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	r.runDispatch(ctx, msgChan)
}

func TestRunDispatchNoHandler(t *testing.T) {
	t.Parallel()
	r := newRouter()
	msgChan := make(chan *TargetMessage, 1)
	msgChan <- &TargetMessage{Topic: "unknown-topic", Message: newMockIOTypeT([]byte("test"))}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	r.runDispatch(ctx, msgChan)
}

func TestRunDispatchDropsEmptyMessage(t *testing.T) {
	t.Parallel()
	r := newRouter()
	handled := false
	r.registerHandler("topic1", func(context.Context, *TargetMessage) error {
		handled = true
		return nil
	})

	msgChan := make(chan *TargetMessage, 2)
	msgChan <- nil
	msgChan <- &TargetMessage{Topic: "topic1"}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r.runDispatch(ctx, msgChan)
	assert.False(t, handled)
}

func TestRunDispatchStopsWhenMessageChannelCloses(t *testing.T) {
	r := newRouter()
	msgChan := make(chan *TargetMessage)
	close(msgChan)

	done := make(chan struct{})
	go func() {
		r.runDispatch(context.Background(), msgChan)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("router did not stop after the message channel was closed")
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := newRouter()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(2)
		topic := fmt.Sprintf("topic-%d", i)

		go func() {
			defer wg.Done()
			handler := func(ctx context.Context, msg *TargetMessage) error { return nil }
			r.registerHandler(topic, handler)
		}()

		go func() {
			defer wg.Done()
			r.deRegisterHandler(topic)
		}()
	}

	wg.Wait()
}
