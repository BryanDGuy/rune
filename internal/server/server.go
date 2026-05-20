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

package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	runev1 "github.com/runicsigil/rune/gen/rune/v1"
	"github.com/runicsigil/rune/internal/config"
	"github.com/runicsigil/rune/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// Server wraps the gRPC server and its dependencies.
type Server struct {
	cfg        *config.Config
	store      storage.Storage
	grpcServer *grpc.Server
	tracker    *connTracker
}

// New creates a Server. Call Start to begin accepting connections.
func New(cfg *config.Config, store storage.Storage) *Server {
	s := &Server{cfg: cfg, store: store}
	s.tracker = &connTracker{}
	s.grpcServer = grpc.NewServer(grpc.StatsHandler(s.tracker))
	runev1.RegisterRuneServiceServer(s.grpcServer, &handler{srv: s})
	registerHealth(s)
	return s
}

// Start listens on cfg.Port and serves gRPC in a goroutine.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	go s.grpcServer.Serve(lis) //nolint:errcheck
	return nil
}

// StartOnListener serves on an existing listener (useful for testing with bufconn).
func (s *Server) StartOnListener(lis net.Listener) {
	go s.grpcServer.Serve(lis) //nolint:errcheck
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

// ActiveConns returns the number of currently active connections.
func (s *Server) ActiveConns() int64 {
	return s.tracker.conns.Load()
}

// connTracker implements stats.Handler to count active connections.
type connTracker struct {
	conns atomic.Int64
}

func (t *connTracker) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (t *connTracker) HandleRPC(_ context.Context, _ stats.RPCStats) {}
func (t *connTracker) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (t *connTracker) HandleConn(_ context.Context, cs stats.ConnStats) {
	switch cs.(type) {
	case *stats.ConnBegin:
		t.conns.Add(1)
	case *stats.ConnEnd:
		t.conns.Add(-1)
	}
}
