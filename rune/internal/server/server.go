package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/bryandguy/rune/rune/internal/cluster"
	"github.com/bryandguy/rune/rune/internal/config"
	runev1 "github.com/bryandguy/rune/rune/internal/gen/rune/v1"
	"github.com/bryandguy/rune/rune/internal/logging"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/bryandguy/rune/rune/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
)

type Server struct {
	cfg          *config.Config
	store        storage.Storage
	logger       *logging.Logger
	m            *telemetry.Metrics
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
// single-node. A nil logger discards all log output. A nil m disables telemetry.
func New(cfg *config.Config, store storage.Storage, logger *logging.Logger, clusterOpts *ClusterOptions, m *telemetry.Metrics) *Server {
	if logger == nil {
		logger = logging.Discard()
	}
	s := &Server{cfg: cfg, store: store, logger: logger, m: m}
	if clusterOpts != nil {
		s.membership = clusterOpts.Membership
		s.dialer = clusterOpts.Dialer
	}
	s.tracker = &connTracker{m: m}

	unaryInterceptors := []grpc.UnaryServerInterceptor{s.logUnary, s.recoverUnary}
	streamInterceptors := []grpc.StreamServerInterceptor{s.logStream, s.recoverStream}
	if m != nil {
		unaryInterceptors = append([]grpc.UnaryServerInterceptor{m.GRPC.UnaryServerInterceptor()}, unaryInterceptors...)
		streamInterceptors = append([]grpc.StreamServerInterceptor{m.GRPC.StreamServerInterceptor()}, streamInterceptors...)
	}

	s.grpcServer = grpc.NewServer(
		grpc.StatsHandler(s.tracker),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	)
	runev1.RegisterRuneServiceServer(s.grpcServer, &handler{srv: s})
	registerHealth(s)

	if m != nil {
		m.GRPC.InitializeMetrics(s.grpcServer)
	}

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
	s.healthServer.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_SERVING)
	return nil
}

// StartOnListener serves on an existing listener (useful for testing with bufconn).
func (s *Server) StartOnListener(lis net.Listener) {
	go func() { _ = s.grpcServer.Serve(lis) }()
	s.healthServer.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_SERVING)
}

func (s *Server) Stop() {
	s.healthServer.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()
	t := time.NewTimer(30 * time.Second)
	defer t.Stop()
	select {
	case <-stopped:
	case <-t.C:
		s.grpcServer.Stop()
	}
}

func (s *Server) ActiveConns() int64 {
	return s.tracker.conns.Load()
}

// connTracker implements stats.Handler to count active connections.
type connTracker struct {
	m     *telemetry.Metrics
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
		if t.m != nil {
			t.m.ActiveConns.Inc()
		}
	case *stats.ConnEnd:
		t.conns.Add(-1)
		if t.m != nil {
			t.m.ActiveConns.Dec()
		}
	}
}
