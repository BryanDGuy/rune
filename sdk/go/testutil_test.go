package runesdk_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	runev1 "github.com/bryandguy/rune/sdk/go/internal/gen/rune/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var testSeq atomic.Uint64

// BufconnOptions configures NewBufconnConn.
type BufconnOptions struct {
	// ForwardTo makes this server forward all key operations to the given peer
	// and set x-rune-owner to the peer's target address.
	ForwardTo *grpc.ClientConn
}

// NewBufconnConn starts a minimal in-process Rune server and returns a dialed
// connection and a cleanup function. Pass opts.ForwardTo to simulate a proxy
// that forwards to a peer and advertises it as the owner.
func NewBufconnConn(t *testing.T, bufSize int, opts *BufconnOptions) (*grpc.ClientConn, func()) {
	t.Helper()
	addr := fmt.Sprintf("passthrough://bufnet-%d", testSeq.Add(1))
	lis := bufconn.Listen(bufSize)

	srv := &memServer{store: make(map[string][]byte), addr: addr}
	if opts != nil && opts.ForwardTo != nil {
		srv.forward = opts.ForwardTo
	}

	grpcSrv := grpc.NewServer()
	runev1.RegisterRuneServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		addr,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	return conn, func() {
		grpcSrv.Stop()
		_ = conn.Close()
		_ = lis.Close()
	}
}

// memServer is a minimal in-process implementation of RuneService for SDK tests.
type memServer struct {
	runev1.UnimplementedRuneServiceServer
	store   map[string][]byte
	forward *grpc.ClientConn
	addr    string
	mu      sync.RWMutex
}

func (s *memServer) ownerAddr() string {
	if s.forward != nil {
		return s.forward.Target()
	}
	return s.addr
}

func (s *memServer) Get(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	_ = stream.SetHeader(metadata.Pairs("x-rune-owner", s.ownerAddr()))
	if s.forward != nil {
		return s.proxyGet(req, stream)
	}
	s.mu.RLock()
	val, ok := s.store[req.Key]
	s.mu.RUnlock()
	if !ok {
		return status.Error(codes.NotFound, "key not found")
	}
	const chunk = 1 << 20
	for len(val) > 0 {
		n := min(chunk, len(val))
		if err := stream.Send(&runev1.GetResponse{Chunk: val[:n]}); err != nil {
			return err
		}
		val = val[n:]
	}
	return nil
}

func (s *memServer) proxyGet(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	peer := runev1.NewRuneServiceClient(s.forward)
	ps, err := peer.Get(stream.Context(), req)
	if err != nil {
		return status.Errorf(codes.Unavailable, "proxy get: %v", err)
	}
	for {
		resp, err := ps.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *memServer) Set(stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	_ = stream.SetHeader(metadata.Pairs("x-rune-owner", s.ownerAddr()))
	if s.forward != nil {
		return s.proxySet(stream)
	}
	var key string
	var val []byte
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch p := req.Payload.(type) {
		case *runev1.SetRequest_Header:
			key = p.Header.Key
		case *runev1.SetRequest_Chunk:
			val = append(val, p.Chunk...)
		}
	}
	s.mu.Lock()
	s.store[key] = val
	s.mu.Unlock()
	return stream.SendAndClose(&runev1.SetResponse{})
}

func (s *memServer) proxySet(stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	peer := runev1.NewRuneServiceClient(s.forward)
	ps, err := peer.Set(stream.Context())
	if err != nil {
		return status.Errorf(codes.Unavailable, "proxy set: %v", err)
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := ps.Send(req); err != nil {
			return err
		}
	}
	if _, err := ps.CloseAndRecv(); err != nil {
		return err
	}
	return stream.SendAndClose(&runev1.SetResponse{})
}

func (s *memServer) Delete(ctx context.Context, req *runev1.DeleteRequest) (*runev1.DeleteResponse, error) {
	if s.forward != nil {
		peer := runev1.NewRuneServiceClient(s.forward)
		return peer.Delete(ctx, req)
	}
	s.mu.Lock()
	for _, key := range req.Keys {
		delete(s.store, key)
	}
	s.mu.Unlock()
	return &runev1.DeleteResponse{}, nil
}

func (s *memServer) Ping(_ context.Context, _ *runev1.PingRequest) (*runev1.PingResponse, error) {
	return &runev1.PingResponse{}, nil
}
