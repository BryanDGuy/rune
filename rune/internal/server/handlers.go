package server

import (
	"context"
	"errors"
	"io"

	"github.com/bryandguy/rune/rune/internal/storage"
	runev1 "github.com/bryandguy/rune/shared/gen/rune/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	metaForwarded = "x-rune-forwarded"
	metaOwner     = "x-rune-owner"
)

type handler struct {
	runev1.UnimplementedRuneServiceServer
	srv *Server
}

// owner reports the advertised address of the node that owns key (the client's
// routing hint) and whether that owner is a remote peer, in which case the
// request must be forwarded. addr is empty when ownership can't be resolved
// here: single-node mode, an already-forwarded request (we are not the
// client-facing node), or an empty ring.
func (h *handler) owner(ctx context.Context, key string) (addr string, remote bool) {
	if !h.srv.clusterMode() {
		return "", false
	}
	if md, found := metadata.FromIncomingContext(ctx); found && len(md[metaForwarded]) > 0 {
		return "", false
	}
	node, err := h.srv.membership.Ring().Lookup(key)
	if err != nil {
		return "", false
	}
	return node.Addr, node.ID != h.srv.membership.NodeID()
}

// setOwnerHint advertises the owning node's address so a direct gRPC client can
// cache key→node and route there itself, skipping the forwarding hop next time.
func setOwnerHint(stream grpc.ServerStream, addr string) {
	if addr != "" {
		_ = stream.SetHeader(metadata.Pairs(metaOwner, addr))
	}
}

func forwardCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metaForwarded, "1")
}

func (h *handler) Ping(_ context.Context, _ *runev1.PingRequest) (*runev1.PingResponse, error) {
	return &runev1.PingResponse{Message: "PONG"}, nil
}

func (h *handler) Get(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	addr, remote := h.owner(stream.Context(), req.Key)
	setOwnerHint(stream, addr)
	if remote {
		h.srv.logger.Debug("forwarding get", "key", req.Key, "owner", addr)
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

func (h *handler) peerClient(peerAddr string) (runev1.RuneServiceClient, error) {
	conn, err := h.srv.dialer.Dial(peerAddr, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial peer: %v", err)
	}
	return runev1.NewRuneServiceClient(conn), nil
}

func (h *handler) forwardGet(ctx context.Context, peerAddr string, req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	client, err := h.peerClient(peerAddr)
	if err != nil {
		return err
	}
	peerStream, err := client.Get(forwardCtx(ctx), req)
	if err != nil {
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

	addr, remote := h.owner(stream.Context(), hdr.Key)
	setOwnerHint(stream, addr)
	if remote {
		h.srv.logger.Debug("forwarding set", "key", hdr.Key, "owner", addr)
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
	client, err := h.peerClient(peerAddr)
	if err != nil {
		return err
	}
	peerStream, err := client.Set(forwardCtx(ctx))
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
	if err := h.srv.store.Delete(req.Keys...); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &runev1.DeleteResponse{}, nil
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
