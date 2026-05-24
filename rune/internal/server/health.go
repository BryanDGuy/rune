package server

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ReadinessService is the gRPC health service name used for Kubernetes readiness probes.
const ReadinessService = "rune"

// registerHealth exposes the gRPC health protocol. Liveness ("") is immediately
// SERVING — the process is alive. Readiness starts NOT_SERVING and is flipped to
// SERVING by Start/StartOnListener once the listener is accepting connections, and
// back to NOT_SERVING by Stop before draining begins.
func registerHealth(s *Server) {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s.grpcServer, hs)
	s.healthServer = hs
}
