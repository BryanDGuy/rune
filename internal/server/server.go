package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type Server struct {
	cfg        *config.Config
	store      storage.Storage
	membership cluster.MembershipIface // nil in single-node mode
	dialer     *cluster.PeerDialer     // nil in single-node mode
	grpcServer *grpc.Server
	tracker    *connTracker
}

// New creates a single-node Server (no cluster).
func New(cfg *config.Config, store storage.Storage) *Server {
	return newServer(cfg, store, nil, nil)
}

// NewCluster creates a Server wired into a cluster.
func NewCluster(cfg *config.Config, store storage.Storage, m cluster.MembershipIface, d *cluster.PeerDialer) *Server {
	return newServer(cfg, store, m, d)
}

func newServer(cfg *config.Config, store storage.Storage, m cluster.MembershipIface, d *cluster.PeerDialer) *Server {
	s := &Server{cfg: cfg, store: store, membership: m, dialer: d}
	s.tracker = &connTracker{}
	s.grpcServer = grpc.NewServer(grpc.StatsHandler(s.tracker))
	runev1.RegisterRuneServiceServer(s.grpcServer, &handler{srv: s})
	registerHealth(s)
	return s
}

func (s *Server) clusterMode() bool {
	return s.membership != nil
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", fmt.Sprintf(":%d", s.cfg.Port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	go func() { _ = s.grpcServer.Serve(lis) }()
	return nil
}

// StartOnListener serves on an existing listener (useful for testing with bufconn).
func (s *Server) StartOnListener(lis net.Listener) {
	go func() { _ = s.grpcServer.Serve(lis) }()
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}

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
