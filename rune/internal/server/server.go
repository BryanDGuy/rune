package server

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/bryandguy/rune/rune/internal/cluster"
	"github.com/bryandguy/rune/rune/internal/config"
	"github.com/bryandguy/rune/rune/internal/logging"
	"github.com/bryandguy/rune/rune/internal/storage"
	"github.com/bryandguy/rune/rune/internal/telemetry"
	runev1 "github.com/bryandguy/rune/sdk/go/gen/rune/v1"
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
	inflight     atomic.Int64 // value bytes held by Set buffers and Get copies

	// Memory limits from cfg, with zero fields replaced by the config defaults.
	maxValueBytes    int64
	maxInflightBytes int64
	getIdleTimeout   time.Duration
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
	s := &Server{
		cfg: cfg, store: store, logger: logger, m: m,
		maxValueBytes:    cmp.Or(cfg.MaxValueBytes, config.DefaultMaxValueBytes),
		maxInflightBytes: cmp.Or(cfg.MaxInflightBytes, config.DefaultMaxInflightBytes),
		getIdleTimeout:   cmp.Or(cfg.GetIdleTimeout, config.DefaultGetIdleTimeout),
	}
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
		grpc.WaitForHandlers(true), // hard Stop must not return while a handler can still touch the store
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
	return nil
}

// MarkReady flips readiness to SERVING. Call it once the node can route keys
// (after joining the ring in cluster mode).
func (s *Server) MarkReady() {
	s.healthServer.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_SERVING)
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

// reserve claims n bytes of the in-flight budget, or reports false (claiming
// nothing) when that would exceed MaxInflightBytes. It never waits: Sets grow
// incrementally, so waiting could deadlock streams that each hold a part.
func (s *Server) reserve(n int64) bool {
	if s.inflight.Add(n) > s.maxInflightBytes {
		s.inflight.Add(-n)
		return false
	}
	return true
}

func (s *Server) release(n int64) {
	s.inflight.Add(-n)
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
