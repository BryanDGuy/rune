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

package main_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/bryandguy/rune/internal/server"
	"github.com/bryandguy/rune/internal/storage"
	"github.com/bryandguy/rune/internal/testutil"
	runesdk "github.com/bryandguy/rune/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
)

func newIntegrationClient(t *testing.T) (*runesdk.Client, func()) {
	t.Helper()
	conn, cleanup := testutil.NewBufconnConn(t, 4<<20)
	return runesdk.NewFromConn(conn), cleanup
}

func TestIntegration20MB(t *testing.T) {
	client, cleanup := newIntegrationClient(t)
	defer cleanup()
	ctx := context.Background()

	const size = 20 << 20 // 20MB
	payload := bytes.Repeat([]byte("rune"), size/4)

	require.NoError(t, client.Set(ctx, "big-key", bytes.NewReader(payload)))

	r, err := client.Get(ctx, "big-key")
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, r.Close())
	require.NoError(t, err)

	require.Equal(t, size, len(got), "retrieved size mismatch")
	assert.True(t, bytes.Equal(payload, got), "payload content mismatch")
}

func TestIntegrationGetNotFound(t *testing.T) {
	client, cleanup := newIntegrationClient(t)
	defer cleanup()

	_, err := client.Get(context.Background(), "nonexistent")
	require.ErrorIs(t, err, runesdk.ErrNotFound)
}

func TestIntegrationWithTTL(t *testing.T) {
	client, cleanup := newIntegrationClient(t)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "ttl-key", bytes.NewReader([]byte("value")), runesdk.WithTTL(60*time.Second)))

	r, err := client.Get(ctx, "ttl-key")
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, r.Close())
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
}

func TestIntegrationGracefulShutdown(t *testing.T) {
	cfg := testutil.BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)

	lis := bufconn.Listen(1 << 20)
	srv := server.New(cfg, store)
	srv.StartOnListener(lis)

	// Stop should complete without hanging.
	done := make(chan struct{})
	go func() {
		srv.Stop()
		_ = store.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("graceful shutdown timed out")
	}
}
