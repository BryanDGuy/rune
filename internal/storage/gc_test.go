// Copyright 2026 BryanDGuy
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newGCTestStore creates a store with a short GC interval so tickers fire quickly.
func newGCTestStore(t *testing.T) *BadgerStore {
	t.Helper()
	cfg := baseStorageTestConfig(t)
	cfg.GCInterval = 100 * time.Millisecond
	s, err := NewBadgerStore(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestGCRunsOnEmptyDB verifies that runGC on a brand-new (empty) database
// returns without error. BadgerDB returns ErrNoRewrite for an empty vlog,
// which our implementation treats as a successful no-op.
func TestGCRunsOnEmptyDB(t *testing.T) {
	s := newGCTestStore(t)
	// Should return immediately without panicking or returning an error.
	// runGC has no return value; absence of panic/hang is the assertion.
	done := make(chan struct{})
	go func() {
		s.runGC(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("runGC on empty DB did not return within timeout")
	}
}

// TestGCConcurrentCallsSkipped verifies that when gcRunning is already set,
// a second call to runGC returns immediately without blocking or panicking.
func TestGCConcurrentCallsSkipped(t *testing.T) {
	s := newGCTestStore(t)

	// Simulate a GC pass already in progress.
	s.gcRunning.Store(true)
	defer s.gcRunning.Store(false)

	done := make(chan struct{})
	go func() {
		s.runGC(context.Background())
		close(done)
	}()

	select {
	case <-done:
		// The call was correctly skipped — returned immediately.
	case <-time.After(time.Second):
		t.Fatal("runGC did not skip immediately when gcRunning was true")
	}
}

// TestGCLoopStopsOnCancel verifies that the maintenanceLoop goroutine exits
// when the store is closed (context cancelled). If Close() returns within the
// timeout the loop has exited cleanly.
func TestGCLoopStopsOnCancel(t *testing.T) {
	s, err := NewBadgerStore(baseStorageTestConfig(t))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- s.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within timeout — maintenanceLoop may be stuck")
	}
}
