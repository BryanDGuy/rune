package server

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Liveness ("") is always SERVING. Readiness ("rune") can be set to
// NOT_SERVING on graceful shutdown or mid-rebalance.
func registerHealth(s *Server) {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("rune", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s.grpcServer, hs)
}
