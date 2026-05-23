package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"github.com/bryandguy/rune/internal/cluster"
	"github.com/bryandguy/rune/internal/config"
	"github.com/bryandguy/rune/internal/logging"
	"github.com/bryandguy/rune/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
)

type Server struct {
	cfg          *config.Config
	store        storage.Storage
	logger       *logging.Logger
	membership   cluster.MembershipIface // nil in single-node mode
	dialer       *cluster.PeerDialer     // nil in single-node mode
	grpcServer   *grpc.Server
	healthServer *health.Server
	tracker      *connTracker
}

// ClusterOptions wires a Server into a cluster. Membership and Dialer must both
// be set together. A nil *ClusterOptions means single-node mode.
type ClusterOptions struct {
	Membership cluster.MembershipIface
	Dialer     *cluster.PeerDialer
}

// New creates a Server. Pass a non-nil clusterOpts to run in cluster mode; nil is
// single-node. A nil logger discards all log output.
func New(cfg *config.Config, store storage.Storage, logger *logging.Logger, clusterOpts *ClusterOptions) *Server {
	if logger == nil {
		logger = logging.Discard()
	}
	s := &Server{cfg: cfg, store: store, logger: logger}
	if clusterOpts != nil {
		s.membership = clusterOpts.Membership
		s.dialer = clusterOpts.Dialer
	}
	s.tracker = &connTracker{}
	s.grpcServer = grpc.NewServer(
		grpc.StatsHandler(s.tracker),
		grpc.ChainUnaryInterceptor(s.logUnary, s.recoverUnary),
		grpc.ChainStreamInterceptor(s.logStream, s.recoverStream),
	)
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
	s.healthServer.SetServingStatus("rune", healthpb.HealthCheckResponse_SERVING)
	return nil
}

// StartOnListener serves on an existing listener (useful for testing with bufconn).
func (s *Server) StartOnListener(lis net.Listener) {
	go func() { _ = s.grpcServer.Serve(lis) }()
	s.healthServer.SetServingStatus("rune", healthpb.HealthCheckResponse_SERVING)
}

func (s *Server) Stop() {
	s.healthServer.SetServingStatus("rune", healthpb.HealthCheckResponse_NOT_SERVING)
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
