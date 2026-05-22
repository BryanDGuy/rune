package server

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type accessLog struct {
	start  time.Time
	err    error
	method string
	key    string
	peer   string
}

// messageKey extracts the cache key(s) a request targets, or "" if it has none.
func messageKey(m any) string {
	switch r := m.(type) {
	case *runev1.GetRequest:
		return r.Key
	case *runev1.SetRequest:
		if h := r.GetHeader(); h != nil {
			return h.Key
		}
	case *runev1.DeleteRequest:
		return strings.Join(r.Keys, ",")
	case *runev1.ExistsRequest:
		return strings.Join(r.Keys, ",")
	}
	return ""
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return ""
}

func (s *Server) logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	s.logRPC(accessLog{start: start, err: err, method: info.FullMethod, key: messageKey(req), peer: peerAddr(ctx)})
	return resp, err
}

func (s *Server) logStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	ks := &keyCapturingStream{ServerStream: ss}
	err := handler(srv, ks)
	s.logRPC(accessLog{start: start, err: err, method: info.FullMethod, key: ks.key, peer: peerAddr(ss.Context())})
	return err
}

func (s *Server) logRPC(a accessLog) {
	attrs := []any{
		"method", a.method,
		"duration_ms", time.Since(a.start).Milliseconds(),
	}
	if a.key != "" {
		attrs = append(attrs, "key", a.key)
	}
	if a.peer != "" {
		attrs = append(attrs, "peer", a.peer)
	}
	if a.err != nil {
		attrs = append(attrs, "code", status.Code(a.err).String())
		s.logger.Warn("rpc failed", attrs...)
		return
	}
	s.logger.Debug("rpc served", attrs...)
}

func (s *Server) recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logPanic(info.FullMethod, r)
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

func (s *Server) recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logPanic(info.FullMethod, r)
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(srv, ss)
}

func (s *Server) logPanic(method string, r any) {
	s.logger.Error("panic recovered", "method", method, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
}

// keyCapturingStream records the cache key from the first message a streaming
// RPC receives, so the access log can report which key the request touched.
type keyCapturingStream struct {
	grpc.ServerStream
	key      string
	captured bool
}

func (k *keyCapturingStream) RecvMsg(m any) error {
	err := k.ServerStream.RecvMsg(m)
	if err == nil && !k.captured {
		k.captured = true
		k.key = messageKey(m)
	}
	return err
}
