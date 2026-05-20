package server

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// registerHealth registers the gRPC health service on the server.
// Liveness ("") is always SERVING. Readiness ("rune") is set to SERVING
// after the server is wired up; callers can set it to NOT_SERVING on
// graceful shutdown or mid-rebalance.
func registerHealth(s *Server) *health.Server {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("rune", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s.grpcServer, hs)
	return hs
}
