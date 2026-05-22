package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	s.logRPC(info.FullMethod, start, err)
	return resp, err
}

func (s *Server) logStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	s.logRPC(info.FullMethod, start, err)
	return err
}

func (s *Server) logRPC(method string, start time.Time, err error) {
	attrs := []any{
		"method", method,
		"request_id", newRequestID(),
		"duration_ms", time.Since(start).Milliseconds(),
		"code", status.Code(err).String(),
	}
	if err != nil {
		s.logger.Warn("rpc failed", attrs...)
		return
	}
	s.logger.Debug("rpc served", attrs...)
}
