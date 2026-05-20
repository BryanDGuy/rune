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
	"net"
	"testing"
	"time"

	"github.com/runicsigil/rune/internal/config"
	"github.com/runicsigil/rune/internal/server"
	"github.com/runicsigil/rune/internal/storage"
	runesdk "github.com/runicsigil/rune/sdk/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func newIntegrationClient(t *testing.T) (*runesdk.Client, func()) {
	t.Helper()
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30, // 1GB
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20, // 1MB chunks
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)

	lis := bufconn.Listen(4 << 20) // 4MB bufconn buffer
	srv := server.New(cfg, store)
	srv.StartOnListener(lis)

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := runesdk.NewFromConn(conn)
	cleanup := func() {
		_ = client.Close()
		srv.Stop()
		_ = store.Close()
	}
	return client, cleanup
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
	cfg := &config.Config{
		DataDir:            t.TempDir(),
		MaxStorageBytes:    1 << 30,
		EvictionThreshold:  0.8,
		EvictionSizeWeight: 1.0,
		EvictionAgeWeight:  1.0,
		StreamChunkSize:    1 << 20,
		GCInterval:         time.Hour,
		GCDiscardRatio:     0.5,
		TTLSweepInterval:   time.Hour,
	}
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
