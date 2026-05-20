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

package testutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/runicsigil/rune/internal/config"
	"github.com/runicsigil/rune/internal/server"
	"github.com/runicsigil/rune/internal/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// BaseConfig returns a standard test config with sane defaults and a temp DataDir.
func BaseConfig(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
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
}

// NewBufconnConn starts a Rune server over an in-process bufconn listener and
// returns a dialed gRPC connection plus a cleanup function. bufSize controls
// the in-memory buffer size.
func NewBufconnConn(t *testing.T, bufSize int) (*grpc.ClientConn, func()) {
	t.Helper()
	cfg := BaseConfig(t)
	store, err := storage.NewBadgerStore(cfg)
	require.NoError(t, err)
	lis := bufconn.Listen(bufSize)
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
	return conn, func() {
		srv.Stop()
		_ = conn.Close()
		_ = store.Close()
	}
}
