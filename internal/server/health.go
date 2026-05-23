package server

import (
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// registerHealth exposes the gRPC health protocol. Liveness ("") is immediately
// SERVING — the process is alive. Readiness ("rune") starts NOT_SERVING and is
// flipped to SERVING by Start/StartOnListener once the listener is accepting
// connections, and back to NOT_SERVING by Stop before draining begins.
func registerHealth(s *Server) {
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("rune", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(s.grpcServer, hs)
	s.healthServer = hs
}
