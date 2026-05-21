package server

import (
	"context"
	"errors"
	"io"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"github.com/bryandguy/rune/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const metaForwarded = "x-rune-forwarded"

type handler struct {
	runev1.UnimplementedRuneServiceServer
	srv *Server
}

// owningAddr returns the peer address that owns key, or "" if this node owns it
// or cluster mode is off. "" means: serve locally.
func (h *handler) owningAddr(ctx context.Context, key string) string {
	if !h.srv.clusterMode() {
		return ""
	}
	// Already forwarded once — serve locally to prevent routing loops.
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[metaForwarded]) > 0 {
			return ""
		}
	}
	node, err := h.srv.membership.Ring().Lookup(key)
	if err != nil || node.ID == h.srv.membership.NodeID() {
		return ""
	}
	return node.Addr
}

// forwardCtx returns a context carrying the forwarded metadata header.
func forwardCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaForwarded, "1")
}

func (h *handler) Ping(_ context.Context, _ *runev1.PingRequest) (*runev1.PingResponse, error) {
	return &runev1.PingResponse{Message: "PONG"}, nil
}

func (h *handler) Get(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	if addr := h.owningAddr(stream.Context(), req.Key); addr != "" {
		return h.forwardGet(stream.Context(), addr, req, stream)
	}
	value, err := h.srv.store.Get(req.Key)
	if errors.Is(err, storage.ErrNotFound) {
		return status.Error(codes.NotFound, "key not found")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "get: %v", err)
	}

	chunkSize := h.srv.cfg.StreamChunkSize
	for len(value) > 0 {
		n := min(chunkSize, len(value))
		if err := stream.Send(&runev1.GetResponse{Chunk: value[:n]}); err != nil {
			return err
		}
		value = value[n:]
	}
	return nil
}

func (h *handler) forwardGet(ctx context.Context, peerAddr string, req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	conn, err := h.srv.dialer.Dial(peerAddr)
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial peer: %v", err)
	}
	peerStream, err := runev1.NewRuneServiceClient(conn).Get(forwardCtx(ctx), req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return status.Error(codes.NotFound, "key not found")
		}
		return status.Errorf(codes.Unavailable, "peer get: %v", err)
	}
	for {
		resp, err := peerStream.Recv()
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

func (h *handler) Set(stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first message must contain SetHeader")
	}

	if addr := h.owningAddr(stream.Context(), hdr.Key); addr != "" {
		return h.forwardSet(stream.Context(), addr, hdr, stream)
	}

	key := hdr.Key
	ttl := hdr.TtlSeconds
	var value []byte
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		chunk, ok := msg.Payload.(*runev1.SetRequest_Chunk)
		if !ok || chunk == nil {
			return status.Error(codes.InvalidArgument, "expected chunk payload after header")
		}
		value = append(value, chunk.Chunk...)
	}

	if err := h.srv.store.Set(key, value, ttl); err != nil {
		return status.Errorf(codes.Internal, "set: %v", err)
	}
	return stream.SendAndClose(&runev1.SetResponse{})
}

func (h *handler) forwardSet(ctx context.Context, peerAddr string, hdr *runev1.SetHeader, stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	conn, err := h.srv.dialer.Dial(peerAddr)
	if err != nil {
		return status.Errorf(codes.Unavailable, "dial peer: %v", err)
	}
	peerStream, err := runev1.NewRuneServiceClient(conn).Set(forwardCtx(ctx))
	if err != nil {
		return status.Errorf(codes.Unavailable, "peer set: %v", err)
	}
	if err := peerStream.Send(&runev1.SetRequest{Payload: &runev1.SetRequest_Header{Header: hdr}}); err != nil {
		return status.Errorf(codes.Unavailable, "peer set header: %v", err)
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := peerStream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "peer set chunk: %v", err)
		}
	}
	if _, err := peerStream.CloseAndRecv(); err != nil {
		return status.Errorf(codes.Unavailable, "peer set close: %v", err)
	}
	return stream.SendAndClose(&runev1.SetResponse{})
}

func (h *handler) Delete(_ context.Context, req *runev1.DeleteRequest) (*runev1.DeleteResponse, error) {
	n, err := h.srv.store.Delete(req.Keys...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &runev1.DeleteResponse{Deleted: n}, nil
}

func (h *handler) Exists(_ context.Context, req *runev1.ExistsRequest) (*runev1.ExistsResponse, error) {
	n, err := h.srv.store.Exists(req.Keys...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "exists: %v", err)
	}
	return &runev1.ExistsResponse{Count: n}, nil
}

func (h *handler) Info(_ context.Context, _ *runev1.InfoRequest) (*runev1.InfoResponse, error) {
	info, err := h.srv.store.Info()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "info: %v", err)
	}
	return &runev1.InfoResponse{
		StorageUsedBytes:  info.UsedBytes,
		StorageMaxBytes:   info.MaxBytes,
		CacheHits:         info.Hits,
		CacheMisses:       info.Misses,
		ActiveConnections: h.srv.ActiveConns(),
		EvictionsTotal:    info.EvictionsTotal,
	}, nil
}

// compile-time interface check
var _ runev1.RuneServiceServer = (*handler)(nil)
