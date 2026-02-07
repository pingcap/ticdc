// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

// mockRoundTripper blocks until the context is done.
type mockRoundTripper struct {
	blockUntilContextDone bool
	err                   error
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.blockUntilContextDone {
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	// Return immediately for success case
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, m.err
}

// mockExternalStorage is a mock implementation for testing timeouts via http client.
type mockExternalStorage struct {
	storage.ExternalStorage // Embed the interface to satisfy it easily
	httpClient              *http.Client
}

// Implement Open so tests can simulate readers that bind to the Open() context.
func (m *mockExternalStorage) Open(ctx context.Context, path string, option *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return &ctxBoundReader{ctx: ctx}, nil
}

func (m *mockExternalStorage) URI() string { return "mock://" }

func (m *mockExternalStorage) Close() {}

// WriteFile simulates a write operation by making an HTTP request that respects context cancellation.
func (m *mockExternalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if m.httpClient == nil {
		panic("httpClient not set in mockExternalStorage") // Should be set in tests
	}
	// Create a dummy request. The URL doesn't matter as the RoundTripper is mocked.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://mock/"+name, http.NoBody)
	if err != nil {
		return err // Should not happen with valid inputs
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err // This will include context errors like DeadlineExceeded
	}
	resp.Body.Close() // Important to close the body
	if resp.StatusCode != http.StatusOK {
		return errors.New("mock http request failed") // Or handle specific statuses
	}
	return nil
}

func TestExtStorageWithTimeoutWriteFileTimeout(t *testing.T) {
	testTimeout := 50 * time.Millisecond

	// Create a mock HTTP client that blocks until context is done
	mockClient := &http.Client{
		Transport: &mockRoundTripper{blockUntilContextDone: true},
	}

	mockStore := &mockExternalStorage{
		httpClient: mockClient,
	}

	// Wrap the mock store with the timeout logic
	timedStore := &extStorageWithTimeout{
		ExternalStorage: mockStore,
		timeout:         testTimeout,
	}

	startTime := time.Now()
	// Use context.Background() as the base context
	err := timedStore.WriteFile(context.Background(), "testfile", []byte("data"))
	duration := time.Since(startTime)

	// 1. Assert that an error occurred
	require.Error(t, err, "Expected an error due to timeout")

	// 2. Assert that the error is context.DeadlineExceeded
	require.True(t, errors.Is(err, context.DeadlineExceeded), "Expected context.DeadlineExceeded error, got: %v", err)

	// 3. Assert that the function returned quickly (around the timeout duration)
	require.InDelta(t, testTimeout, duration, float64(testTimeout)*0.5, "Duration (%v) should be close to the timeout (%v)", duration, testTimeout)
}

func TestExtStorageWithTimeoutWriteFileSuccess(t *testing.T) {
	testTimeout := 100 * time.Millisecond

	// Create a mock HTTP client that returns success immediately
	mockClient := &http.Client{
		Transport: &mockRoundTripper{blockUntilContextDone: false, err: nil},
	}

	mockStore := &mockExternalStorage{
		httpClient: mockClient,
	}

	timedStore := &extStorageWithTimeout{
		ExternalStorage: mockStore,
		timeout:         testTimeout,
	}

	// Use context.Background() as the base context
	err := timedStore.WriteFile(context.Background(), "testfile", []byte("data"))

	// Assert success
	require.NoError(t, err, "Expected no error for successful write within timeout")
}

// ctxBoundReader is a reader that checks the context passed to Open().
// It simulates backends (e.g., Azure) that bind reader lifetime to the Open() context.
type ctxBoundReader struct {
	ctx context.Context
}

func (r *ctxBoundReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	return 1, nil
}

func (r *ctxBoundReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (r *ctxBoundReader) Close() error { return nil }

func (r *ctxBoundReader) GetFileSize() (int64, error) { return 1, nil }

func TestExtStorageOpenDoesNotCancelReaderContext(t *testing.T) {
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockExternalStorage{},
		timeout:         100 * time.Millisecond,
	}

	rd, err := timedStore.Open(context.Background(), "file", nil)
	require.NoError(t, err)
	defer rd.Close()

	// If Open() had used a derived context with immediate cancel, this would fail with context canceled.
	_, err = rd.Read(make([]byte, 1))
	require.NoError(t, err)
}

func TestExtStorageOpenReaderRespectsCallerCancel(t *testing.T) {
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockExternalStorage{},
		timeout:         10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	rd, err := timedStore.Open(ctx, "file", nil)
	require.NoError(t, err)
	defer rd.Close()

	// This should cause the reader to fail with context canceled.
	cancel()
	_, err = rd.Read(make([]byte, 1))

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

// blockingCtxWriter blocks in Write/Close until the passed ctx is done.
// It is used to verify extStorageWithTimeout wraps streaming writer operations
// with default deadlines when callers use context without deadline.
type blockingCtxWriter struct{}

func (*blockingCtxWriter) Write(ctx context.Context, _ []byte) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}

func (*blockingCtxWriter) Close(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// blockingCreateCtxWriter blocks in Write/Close until the ctx passed to Create() is done.
// This simulates multipart backends (e.g., S3 uploader) where background work is bound to
// the Create() context rather than the Write/Close context.
type blockingCreateCtxWriter struct {
	createCtx context.Context
}

func (w *blockingCreateCtxWriter) Write(_ context.Context, _ []byte) (int, error) {
	<-w.createCtx.Done()
	return 0, w.createCtx.Err()
}

func (w *blockingCreateCtxWriter) Close(_ context.Context) error {
	<-w.createCtx.Done()
	return w.createCtx.Err()
}

type mockCreateExternalStorage struct {
	storage.ExternalStorage
	writer storage.ExternalFileWriter
}

func (m *mockCreateExternalStorage) Create(ctx context.Context, _ string, _ *storage.WriterOption) (storage.ExternalFileWriter, error) {
	if w, ok := m.writer.(*blockingCreateCtxWriter); ok {
		w.createCtx = ctx
	}
	return m.writer, nil
}

func TestExtStorageCreateWriterWriteTimeout(t *testing.T) {
	// Scenario: a streaming writer should not hang forever when the caller passes
	// a context without deadline.
	//
	// Steps:
	// 1) Use a writer that blocks until the Write() ctx is done.
	// 2) Call extStorageWithTimeout.Create and then writer.Write with context.Background().
	// 3) Verify the call fails within the default timeout.
	testTimeout := 50 * time.Millisecond
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockCreateExternalStorage{writer: &blockingCtxWriter{}},
		timeout:         testTimeout,
	}

	w, err := timedStore.Create(context.Background(), "file", &storage.WriterOption{Concurrency: 1})
	require.NoError(t, err)

	start := time.Now()
	_, err = w.Write(context.Background(), []byte("x"))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
	require.InDelta(t, testTimeout, elapsed, float64(testTimeout)*0.5)
}

func TestExtStorageCreateMultipartWriteCancelsCreateCtxOnTimeout(t *testing.T) {
	// Scenario: multipart backends can bind background work to the Create() context.
	// When Write() times out, TiCDC should cancel that Create() context so the call unblocks.
	//
	// Steps:
	// 1) Use a writer that blocks until the Create() ctx is canceled.
	// 2) Call extStorageWithTimeout.Create with Concurrency > 1.
	// 3) Call Write() with a ctx without deadline and verify it returns in time.
	testTimeout := 50 * time.Millisecond
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockCreateExternalStorage{writer: &blockingCreateCtxWriter{}},
		timeout:         testTimeout,
	}

	w, err := timedStore.Create(context.Background(), "file", &storage.WriterOption{Concurrency: 2})
	require.NoError(t, err)

	start := time.Now()
	_, err = w.Write(context.Background(), []byte("x"))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), "got %v", err)
	require.InDelta(t, testTimeout, elapsed, float64(testTimeout)*0.5)
}
