package server

import (
	"context"
	"errors"
	"io"

	runev1 "github.com/bryandguy/rune/gen/rune/v1"
	"github.com/bryandguy/rune/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handler implements RuneServiceServer by delegating to the storage layer.
type handler struct {
	runev1.UnimplementedRuneServiceServer
	srv *Server
}

func (h *handler) Ping(_ context.Context, _ *runev1.PingRequest) (*runev1.PingResponse, error) {
	return &runev1.PingResponse{Message: "PONG"}, nil
}

func (h *handler) Get(req *runev1.GetRequest, stream grpc.ServerStreamingServer[runev1.GetResponse]) error {
	r, err := h.srv.store.Get(req.Key)
	if errors.Is(err, storage.ErrNotFound) {
		return status.Error(codes.NotFound, "key not found")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "get: %v", err)
	}
	defer r.Close()

	buf := make([]byte, h.srv.cfg.StreamChunkSize)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&runev1.GetResponse{Chunk: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read: %v", readErr)
		}
	}
}

func (h *handler) Set(stream grpc.ClientStreamingServer[runev1.SetRequest, runev1.SetResponse]) error {
	// First message must be a header with key + optional TTL.
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return status.Error(codes.InvalidArgument, "first message must contain SetHeader")
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

func (h *handler) Delete(ctx context.Context, req *runev1.DeleteRequest) (*runev1.DeleteResponse, error) {
	n, err := h.srv.store.Delete(req.Keys...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &runev1.DeleteResponse{Deleted: n}, nil
}

func (h *handler) Exists(ctx context.Context, req *runev1.ExistsRequest) (*runev1.ExistsResponse, error) {
	n, err := h.srv.store.Exists(req.Keys...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "exists: %v", err)
	}
	return &runev1.ExistsResponse{Count: n}, nil
}

func (h *handler) Expire(ctx context.Context, req *runev1.ExpireRequest) (*runev1.ExpireResponse, error) {
	if req.TtlSeconds <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "ttl_seconds must be positive, got %d", req.TtlSeconds)
	}
	ok, err := h.srv.store.Expire(req.Key, req.TtlSeconds)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "expire: %v", err)
	}
	return &runev1.ExpireResponse{Ok: ok}, nil
}

func (h *handler) TTL(ctx context.Context, req *runev1.TTLRequest) (*runev1.TTLResponse, error) {
	ttl, err := h.srv.store.TTL(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ttl: %v", err)
	}
	return &runev1.TTLResponse{TtlSeconds: ttl}, nil
}

func (h *handler) Persist(ctx context.Context, req *runev1.PersistRequest) (*runev1.PersistResponse, error) {
	ok, err := h.srv.store.Persist(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "persist: %v", err)
	}
	return &runev1.PersistResponse{Ok: ok}, nil
}

func (h *handler) Info(ctx context.Context, _ *runev1.InfoRequest) (*runev1.InfoResponse, error) {
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
